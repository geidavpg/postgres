/*-------------------------------------------------------------------------
 *
 * nodeBitmapIndexscan.c
 *	  Routines to support bitmapped index scans of relations
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeBitmapIndexscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		MultiExecBitmapIndexScan	scans a relation using index.
 *		ExecInitBitmapIndexScan		creates and initializes state info.
 *		ExecReScanBitmapIndexScan	prepares to rescan the plan.
 *		ExecEndBitmapIndexScan		releases all storage.
 *		ExecBitmapIndexScanEstimate	estimates parallel shared memory.
 *		ExecBitmapIndexScanInitializeDSM initializes parallel shared state.
 *		ExecBitmapIndexScanReInitializeDSM resets parallel shared state.
 *		ExecBitmapIndexScanInitializeWorker attaches a parallel worker.
 *		ExecBitmapIndexScanRetrieveInstrumentation collects parallel worker instrumentation.
 */

/*
 * Parallel bitmap index scans
 * --------------------------------
 *
 * Each participant in a parallel-aware BitmapIndexScan scans part of the
 * index into a local TIDBitmap. The participants publish shared iterators for
 * those bitmaps, then each participant reads every published bitmap and copies
 * a disjoint subset of HEAP blocks into its result. HEAP blocks are distributed
 * among the participating processes by hashing the block number. Consequently,
 * the union of the returned bitmaps contains the complete index result without any
 * HEAP block being processed by more than one participant.
 *
 * The key design property of the partitioned approach is that BitmapAnd and BitmapOr
 * can operate efficiently on these process-local partitions. Sharing a single
 * TIDBitmap instead would make this much harder, because union and intersection
 * would need efficient synchronization.
 *
 * A BitmapHeapScan can have several BitmapIndexScans below BitmapAnd and
 * BitmapOr nodes. Every such scan must use the same set of participating
 * processes and assign the same subset of HEAP blocks to each process.
 * Otherwise, the local BitmapAnd/BitmapOr operations could combine mismatched
 * partitions and lose or duplicate tuples.
 *
 * The first BitmapIndexScan below a parallel-aware BitmapHeapScan therefore
 * acts as the coordination owner. It uses a dynamic barrier, allowing workers
 * to join while its index scan is in progress. Each process that joins during
 * phase zero records its leader or worker slot in shared state. Once the first
 * scan reaches its partitioning barrier, the set of recorded processes is
 * frozen. A process that arrives after phase zero returns an empty bitmap and
 * remains excluded from every later BitmapIndexScan in the same bitmap tree.
 * This is unlikely as processes have time to join until the index scan completes.
 *
 * Later BitmapIndexScans consult the frozen membership stored in the owner's
 * shared state. Excluded processes return empty bitmaps immediately. The
 * first admitted process to reach each later scan initializes that scan's
 * barrier as a static barrier sized to the frozen membership. Worker numbers
 * are assigned when workers are launched, but some workers may arrive too late
 * to participate. For example, workers 0, 2, and 5 might join the first scan
 * while workers 1, 3, and 4 miss it. The participating workers therefore
 * renumber themselves consecutively as 0, 1, and 2 by counting participating
 * workers with lower worker numbers. Every BitmapIndexScan uses this same
 * renumbering when assigning HEAP blocks.
 *
 * Each scan uses two barrier phases around bitmap sharing: one after all local
 * iterators have been published, and one after every participant has finished
 * reading them. The coordination owner detaches from its dynamic barrier at
 * the end; later scans use static barriers and therefore do not attach or
 * detach explicitly. BitmapAnd must still execute all children in parallel
 * mode, even when one participant's intermediate bitmap is empty, so that all
 * admitted participants reach the later static barriers.
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/relscan.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/nodeBitmapIndexscan.h"
#include "executor/nodeIndexscan.h"
#include "optimizer/optimizer.h"
#include "miscadmin.h"
#include "nodes/tidbitmap.h"
#include "storage/barrier.h"
#include "utils/wait_event.h"

typedef struct SharedBitmapIndexParticipant
{
	dsa_pointer tbmiter;
	bool		participates;
} SharedBitmapIndexParticipant;

/*
 * Per-BitmapIndexScan shared state used to share locally-built bitmaps among
 * workers and coordinate the partition/free phases. The first BitmapIndexScan
 * under a BitmapHeapScan also uses it to coordinate participant membership and
 * partition ranks across the bitmap tree.
 */
typedef struct SharedBitmapIndexState
{
	int			max_participants;	/* leader + max number of workers */
	slock_t		barrier_init_lock;
	bool		barrier_initialized;
	Barrier		barrier;
	SharedBitmapIndexParticipant participants[FLEXIBLE_ARRAY_MEMBER];
} SharedBitmapIndexState;

#define PARALLEL_KEY_BITMAP_INDEX_OFFSET UINT64CONST(0xD100000000000000)

static inline uint32_t FastMod(uint32_t hash, uint32_t nparticipants);

static Size BitmapIndexScanWorkMemForScanning(const SharedBitmapIndexState *sstate);
static Size BitmapIndexScanWorkMemForPartitioning(const SharedBitmapIndexState *sstate);
static inline bool BitmapIndexScanIsCoordinationOwner(const BitmapIndexScanState *node);
static Size BitmapIndexScanSharedStateSize(int nworkers);
static TIDBitmap *BitmapIndexScanPartition(BitmapIndexScanState *node, TIDBitmap *tbm);

/* ----------------------------------------------------------------
 *		ExecBitmapIndexScan
 *
 *		stub for pro forma compliance
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecBitmapIndexScan(PlanState *pstate)
{
	elog(ERROR, "BitmapIndexScan node does not support ExecProcNode call convention");
	return NULL;
}

/* ----------------------------------------------------------------
 *		MultiExecBitmapIndexScan(node)
 * ----------------------------------------------------------------
 */
Node *
MultiExecBitmapIndexScan(BitmapIndexScanState *node)
{
	TIDBitmap  *tbm;
	IndexScanDesc scandesc;
	double		nTuples = 0;
	bool		doscan;

	/* must provide our own instrumentation support */
	if (node->ss.ps.instrument)
		InstrStartNode(node->ss.ps.instrument);

	/*
	 * extract necessary information from index scan node
	 */
	scandesc = node->biss_ScanDesc;

	/*
	 * Serial execution of a parallel-aware plan (no workers launched) needs a
	 * scan descriptor that we did not create during DSM setup.
	 */
	if (scandesc == NULL)
	{
		scandesc = index_beginscan_bitmap(node->biss_RelationDesc,
										node->ss.ps.state->es_snapshot,
										node->biss_Instrument,
										node->biss_NumScanKeys);
		node->biss_ScanDesc = scandesc;

		if (node->biss_NumRuntimeKeys == 0 || node->biss_RuntimeKeysReady)
			index_rescan(scandesc,
						 node->biss_ScanKeys, node->biss_NumScanKeys,
						 NULL, 0);
	}

	/*
	 * If we have runtime keys and they've not already been set up, do it now.
	 * Array keys are also treated as runtime keys; note that if ExecReScan
	 * returns with biss_RuntimeKeysReady still false, then there is an empty
	 * array key so we should do nothing.
	 */
	if (!node->biss_RuntimeKeysReady &&
		(node->biss_NumRuntimeKeys != 0 || node->biss_NumArrayKeys != 0))
	{
		ExecReScan((PlanState *) node);
		doscan = node->biss_RuntimeKeysReady;
	}
	else
		doscan = true;

	/*
	 * If we're running as part of a parallel query, we need to attach to the
	 * barrier before scanning the index. This ensures that workers that arrive
	 * late don't waste time scanning the index only to discard their bitmap.
	 */
	if (node->biss_ParallelState != NULL)
	{
		SharedBitmapIndexState *sstate = node->biss_ParallelState;
		SharedBitmapIndexState *coord = node->biss_CoordState;
		const int		slot = IsParallelWorker() ? ParallelWorkerNumber + 1 : 0;

		Assert(slot < sstate->max_participants);

		if (BitmapIndexScanIsCoordinationOwner(node))
		{
			if (BarrierAttach(&sstate->barrier) == 0)
				coord->participants[slot].participates = true;
			else
			{
				/*
				 * We attached after the build phase was already done. We cannot
				 * contribute a partition, so just detach and return an empty bitmap.
				 */
				TIDBitmap  *empty_bitmap;

				BarrierDetach(&sstate->barrier);
				empty_bitmap = tbm_create(BitmapIndexScanWorkMemForScanning(sstate), NULL);
				if (node->ss.ps.instrument)
					InstrStopNode(node->ss.ps.instrument, 0);
				return (Node *) empty_bitmap;
			}
		}
		else
		{
			if (!coord->participants[slot].participates)
			{
				TIDBitmap  *empty_bitmap = tbm_create(BitmapIndexScanWorkMemForScanning(sstate), NULL);
				if (node->ss.ps.instrument)
					InstrStopNode(node->ss.ps.instrument, 0);
				return (Node *) empty_bitmap;
			}

			SpinLockAcquire(&sstate->barrier_init_lock);
			if (!sstate->barrier_initialized)
			{
				int nparticipants = 0;

				for (int i = 0; i < coord->max_participants; i++)
					if (coord->participants[i].participates)
						nparticipants++;

				Assert(nparticipants > 0);
				BarrierInit(&sstate->barrier, nparticipants);
				sstate->barrier_initialized = true;
			}
			SpinLockRelease(&sstate->barrier_init_lock);
		}

		/*
		 * Index access methods that don't support parallel scans cannot divide
		 * the index scan work among workers. Elect exactly one participant to
		 * build the full bitmap; the others will leave their bitmap empty and
		 * still receive a partition during the sharing phase below.
		 */
		if (!index_am_supports_parallel_scan(node->biss_RelationDesc) &&
			!BarrierArriveAndWait(&sstate->barrier, WAIT_EVENT_PARALLEL_BITMAP_SCAN))
			doscan = false;
	}

	/*
	 * Prepare the result bitmap. Normally we just create a new one to pass
	 * back; however, our parent node is allowed to store a pre-made one into
	 * node->biss_result, in which case we just OR our tuple IDs into the
	 * existing bitmap. (This saves needing explicit UNION steps.)
	 */
	if (node->biss_result)
	{
		tbm = node->biss_result;
		node->biss_result = NULL;	/* reset for next time */
	}
	else
	{
		/* XXX should we use less than work_mem for this? */
		tbm = tbm_create(BitmapIndexScanWorkMemForScanning(node->biss_ParallelState),
						 node->ss.ps.plan->parallel_aware ?
						 node->ss.ps.state->es_query_dsa : NULL);
	}

	/*
	 * Get TIDs from index and insert into bitmap
	 */
	while (doscan)
	{
		nTuples += (double) index_getbitmap(scandesc, tbm);

		CHECK_FOR_INTERRUPTS();

		doscan = ExecIndexAdvanceArrayKeys(node->biss_ArrayKeys,
										   node->biss_NumArrayKeys);
		if (doscan)				/* reset index scan */
			index_rescan(node->biss_ScanDesc,
						 node->biss_ScanKeys, node->biss_NumScanKeys,
						 NULL, 0);
	}

	/*
	 * If we're running as part of a parallel query, partition the full bitmap
	 * so that each worker gets a disjoint subset of the HEAP blocks.
	 */
	if (node->biss_ParallelState != NULL)
		tbm = BitmapIndexScanPartition(node, tbm);

	/* must provide our own instrumentation support */
	if (node->ss.ps.instrument)
		InstrStopNode(node->ss.ps.instrument, nTuples);

	return (Node *) tbm;
}

/* ----------------------------------------------------------------
 *		ExecReScanBitmapIndexScan(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanBitmapIndexScan(BitmapIndexScanState *node)
{
	ExprContext *econtext = node->biss_RuntimeContext;

	/*
	 * Reset the runtime-key context so we don't leak memory as each outer
	 * tuple is scanned. Note this assumes that we will recalculate *all*
	 * runtime keys on each call.
	 */
	if (econtext)
		ResetExprContext(econtext);

	/*
	 * If we are doing runtime key calculations (ie, any of the index key
	 * values weren't simple Consts), compute the new key values.
	 *
	 * Array keys are also treated as runtime keys; note that if we return
	 * with biss_RuntimeKeysReady still false, then there is an empty array
	 * key so no index scan is needed.
	 */
	if (node->biss_NumRuntimeKeys != 0)
		ExecIndexEvalRuntimeKeys(econtext,
								 node->biss_RuntimeKeys,
								 node->biss_NumRuntimeKeys);
	if (node->biss_NumArrayKeys != 0)
		node->biss_RuntimeKeysReady =
			ExecIndexEvalArrayKeys(econtext,
								 node->biss_ArrayKeys,
								 node->biss_NumArrayKeys);
	else
		node->biss_RuntimeKeysReady = true;

	/* reset index scan */
	if (node->biss_RuntimeKeysReady && node->biss_ScanDesc)
		index_rescan(node->biss_ScanDesc,
					 node->biss_ScanKeys, node->biss_NumScanKeys,
					 NULL, 0);
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapIndexScan
 * ----------------------------------------------------------------
 */
void
ExecEndBitmapIndexScan(BitmapIndexScanState *node)
{
	Relation	indexRelationDesc;
	IndexScanDesc indexScanDesc;

	/*
	 * extract information from the node
	 */
	indexRelationDesc = node->biss_RelationDesc;
	indexScanDesc = node->biss_ScanDesc;

	/*
	 * When ending a parallel worker, copy the statistics gathered by the
	 * worker back into shared memory so that it can be picked up by the main
	 * process to report in EXPLAIN ANALYZE
	 */
	if (node->biss_SharedInfo != NULL && IsParallelWorker())
	{
		IndexScanInstrumentation *winstrument;

		Assert(ParallelWorkerNumber < node->biss_SharedInfo->num_workers);
		winstrument = &node->biss_SharedInfo->winstrument[ParallelWorkerNumber];

		/*
		 * We have to accumulate the stats rather than performing a memcpy.
		 * When a Gather/GatherMerge node finishes it will perform planner
		 * shutdown on the workers. On rescan it will spin up new workers
		 * which will have a new BitmapIndexScanState and zeroed stats.
		 */
		winstrument->nsearches += node->biss_Instrument->nsearches;
		Assert(node->biss_Instrument->ntabletuplefetches == 0);
	}

	/*
	 * close the index relation (no-op if we didn't open it)
	 */
	if (indexScanDesc)
		index_endscan(indexScanDesc);
	if (indexRelationDesc)
		index_close(indexRelationDesc, NoLock);
}

/* ----------------------------------------------------------------
 *		ExecInitBitmapIndexScan
 *
 *		Initializes the index scan's state information.
 * ----------------------------------------------------------------
 */
BitmapIndexScanState *
ExecInitBitmapIndexScan(BitmapIndexScan *node, EState *estate, int eflags)
{
	BitmapIndexScanState *indexstate;
	LOCKMODE	lockmode;
	bool		parallel_aware = node->scan.plan.parallel_aware;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	indexstate = makeNode(BitmapIndexScanState);
	indexstate->ss.ps.plan = (Plan *) node;
	indexstate->ss.ps.state = estate;
	indexstate->ss.ps.ExecProcNode = ExecBitmapIndexScan;

	/* normally we don't make the result bitmap till runtime */
	indexstate->biss_result = NULL;
	indexstate->biss_CoordState = NULL;
	indexstate->biss_FirstBisPlanNodeId = -1;

	/*
	 * We do not open or lock the base relation here. We assume that an
	 * ancestor BitmapHeapScan node is holding AccessShareLock (or better) on
	 * the HEAP relation throughout the execution of the plan tree.
	 *
	 * For a parallel-aware scan, however, we need access to the HEAP relation
	 * to initialize the parallel index scan descriptor.
	 */
	if (parallel_aware && !(eflags & EXEC_FLAG_EXPLAIN_ONLY))
		indexstate->ss.ss_currentRelation =
			ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);
	else
		indexstate->ss.ss_currentRelation = NULL;
	indexstate->ss.ss_currentScanDesc = NULL;

	/*
	 * Miscellaneous initialization
	 *
	 * We do not need a standard exprcontext for this node, though we may
	 * decide below to create a runtime-key exprcontext
	 */

	/*
	 * initialize child expressions
	 *
	 * We don't need to initialize targetlist or qual since neither are used.
	 *
	 * Note: we don't initialize all of the indexqual expression, only the
	 * sub-parts corresponding to runtime keys (see below).
	 */

	/*
	 * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
	 * here. This allows an index-advisor plugin to EXPLAIN a plan containing
	 * references to nonexistent indexes.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return indexstate;

	/* Set up instrumentation of bitmap index scans if requested */
	if (estate->es_instrument)
		indexstate->biss_Instrument = palloc0_object(IndexScanInstrumentation);

	/* Open the index relation. */
	lockmode = exec_rt_fetch(node->scan.scanrelid, estate)->rellockmode;
	indexstate->biss_RelationDesc = index_open(node->indexid, lockmode);

	/*
	 * Initialize index-specific scan state
	 */
	indexstate->biss_RuntimeKeysReady = false;
	indexstate->biss_RuntimeKeys = NULL;
	indexstate->biss_NumRuntimeKeys = 0;

	/*
	 * build the index scan keys from the index qualification
	 */
	ExecIndexBuildScanKeys((PlanState *) indexstate,
						   indexstate->biss_RelationDesc,
						   node->indexqual,
						   false,
						   &indexstate->biss_ScanKeys,
						   &indexstate->biss_NumScanKeys,
						   &indexstate->biss_RuntimeKeys,
						   &indexstate->biss_NumRuntimeKeys,
						   &indexstate->biss_ArrayKeys,
						   &indexstate->biss_NumArrayKeys);

	/*
	 * If we have runtime keys or array keys, we need an ExprContext to
	 * evaluate them. We could just create a "standard" plan node exprcontext,
	 * but to keep the code looking similar to nodeIndexscan.c, it seems
	 * better to stick with the approach of using a separate ExprContext.
	 */
	if (indexstate->biss_NumRuntimeKeys != 0 ||
		indexstate->biss_NumArrayKeys != 0)
	{
		ExprContext *stdecontext = indexstate->ss.ps.ps_ExprContext;

		ExecAssignExprContext(estate, &indexstate->ss.ps);
		indexstate->biss_RuntimeContext = indexstate->ss.ps.ps_ExprContext;
		indexstate->ss.ps.ps_ExprContext = stdecontext;
	}
	else
	{
		indexstate->biss_RuntimeContext = NULL;
	}

	/*
	 * Initialize scan descriptor. For parallel-aware scans this is delayed
	 * until the DSM is set up.
	 */
	if (!parallel_aware)
	{
		indexstate->biss_ScanDesc =
			index_beginscan_bitmap(indexstate->biss_RelationDesc,
								   estate->es_snapshot,
								   indexstate->biss_Instrument,
								   indexstate->biss_NumScanKeys);

		/*
		 * If no run-time keys to calculate, go ahead and pass the scankeys to the
		 * index AM.
		 */
		if (indexstate->biss_NumRuntimeKeys == 0 &&
			indexstate->biss_NumArrayKeys == 0)
			index_rescan(indexstate->biss_ScanDesc,
						 indexstate->biss_ScanKeys, indexstate->biss_NumScanKeys,
						 NULL, 0);
	}

	/*
	 * all done.
	 */
	return indexstate;
}

/* ----------------------------------------------------------------
 *		ExecBitmapIndexScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecBitmapIndexScanEstimate(BitmapIndexScanState *node, ParallelContext *pcxt)
{
	EState		*estate = node->ss.ps.state;
	Size		size;

	if (pcxt->nworkers == 0)
		return;

	/*
	 * Parallel-aware scans need space for the parallel scan descriptor and for
	 * the per-worker bitmap sharing/partitioning state.
	 */
	if (node->ss.ps.plan->parallel_aware)
	{
		node->biss_PscanLen =
			index_parallelscan_estimate(node->biss_RelationDesc,
										node->biss_NumScanKeys,
										0,
										estate->es_snapshot);
		shm_toc_estimate_chunk(&pcxt->estimator, node->biss_PscanLen);
		shm_toc_estimate_chunk(&pcxt->estimator, BitmapIndexScanSharedStateSize(pcxt->nworkers));
		shm_toc_estimate_keys(&pcxt->estimator, 2);
	}

	if (!node->ss.ps.instrument)
		return;

	size = offsetof(SharedIndexScanInstrumentation, winstrument) +
		pcxt->nworkers * sizeof(IndexScanInstrumentation);
	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecBitmapIndexScanInitializeDSM
 *
 *		Set up shared state for a parallel-aware bitmap index scan.
 * ----------------------------------------------------------------
 */
void
ExecBitmapIndexScanInitializeDSM(BitmapIndexScanState *node,
								 ParallelContext *pcxt)
{
	EState		*estate = node->ss.ps.state;
	ParallelIndexScanDesc piscan;
	SharedBitmapIndexState *sstate;
	Size		size;

	if (pcxt->nworkers == 0)
		return;

	if (node->ss.ps.plan->parallel_aware)
	{
		piscan = shm_toc_allocate(pcxt->toc, node->biss_PscanLen);
		index_parallelscan_initialize(node->ss.ss_currentRelation,
									  node->biss_RelationDesc,
									  estate->es_snapshot,
									  piscan);
		shm_toc_insert(pcxt->toc,
					   node->ss.ps.plan->plan_node_id,
					   piscan);

		size = BitmapIndexScanSharedStateSize(pcxt->nworkers);
		sstate = shm_toc_allocate(pcxt->toc, size);
		memset(sstate, 0, size);
		sstate->max_participants = pcxt->nworkers + 1;
		SpinLockInit(&sstate->barrier_init_lock);
		BarrierInit(&sstate->barrier, 0);
		sstate->barrier_initialized = BitmapIndexScanIsCoordinationOwner(node);
		shm_toc_insert(pcxt->toc,
					   node->ss.ps.plan->plan_node_id +
					   PARALLEL_KEY_BITMAP_INDEX_OFFSET,
					   sstate);
		node->biss_ParallelState = sstate;
		node->biss_CoordState = shm_toc_lookup(pcxt->toc,
										 node->biss_FirstBisPlanNodeId +
										 PARALLEL_KEY_BITMAP_INDEX_OFFSET,
										 false);

		node->biss_ScanDesc =
			index_beginscan_bitmap_parallel(node->biss_RelationDesc,
										  estate->es_snapshot,
										  node->biss_Instrument,
										  node->biss_NumScanKeys,
										  piscan);

		/*
		 * If no run-time keys to calculate or they are ready, go ahead and pass
		 * the scankeys to the index AM.
		 */
		if (node->biss_NumRuntimeKeys == 0 || node->biss_RuntimeKeysReady)
			index_rescan(node->biss_ScanDesc,
						 node->biss_ScanKeys, node->biss_NumScanKeys,
						 NULL, 0);
	}

	if (!node->ss.ps.instrument)
		return;

	size = offsetof(SharedIndexScanInstrumentation, winstrument) +
		pcxt->nworkers * sizeof(IndexScanInstrumentation);
	node->biss_SharedInfo =
		(SharedIndexScanInstrumentation *) shm_toc_allocate(pcxt->toc,
															size);
	shm_toc_insert(pcxt->toc,
				   node->ss.ps.plan->plan_node_id +
				   PARALLEL_KEY_SCAN_INSTRUMENT_OFFSET,
				   node->biss_SharedInfo);

	/* Each per-worker area must start out as zeroes */
	memset(node->biss_SharedInfo, 0, size);
	node->biss_SharedInfo->num_workers = pcxt->nworkers;
}

/* ----------------------------------------------------------------
 *		ExecBitmapIndexScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecBitmapIndexScanReInitializeDSM(BitmapIndexScanState *node,
								   ParallelContext *pcxt)
{
	SharedBitmapIndexState *sstate = node->biss_ParallelState;
	Size		size;

	Assert(node->ss.ps.plan->parallel_aware);

	if (node->biss_ScanDesc)
		index_parallelrescan(node->biss_ScanDesc);

	if (sstate == NULL)
		return;

	/* Clear the stored per-worker iterators from the previous scan. */
	size = sstate->max_participants * sizeof(SharedBitmapIndexParticipant);
	memset(sstate->participants, 0, size);

	BarrierInit(&sstate->barrier, 0);
	sstate->barrier_initialized = BitmapIndexScanIsCoordinationOwner(node);
}

/* ----------------------------------------------------------------
 *		ExecBitmapIndexScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecBitmapIndexScanInitializeWorker(BitmapIndexScanState *node,
									ParallelWorkerContext *pwcxt)
{
	EState		*estate = node->ss.ps.state;
	ParallelIndexScanDesc piscan;

	if (node->ss.ps.plan->parallel_aware)
	{
		piscan = shm_toc_lookup(pwcxt->toc,
								node->ss.ps.plan->plan_node_id,
								false);
		node->biss_ParallelState = shm_toc_lookup(pwcxt->toc,
											 node->ss.ps.plan->plan_node_id +
											 PARALLEL_KEY_BITMAP_INDEX_OFFSET,
											 false);
		node->biss_CoordState = shm_toc_lookup(pwcxt->toc,
										 node->biss_FirstBisPlanNodeId +
										 PARALLEL_KEY_BITMAP_INDEX_OFFSET,
										 false);

		node->biss_ScanDesc =
			index_beginscan_bitmap_parallel(node->biss_RelationDesc,
										  estate->es_snapshot,
										  node->biss_Instrument,
										  node->biss_NumScanKeys,
										  piscan);

		/*
		 * If no run-time keys to calculate or they are ready, go ahead and pass
		 * the scankeys to the index AM.
		 */
		if (node->biss_NumRuntimeKeys == 0 || node->biss_RuntimeKeysReady)
			index_rescan(node->biss_ScanDesc,
						 node->biss_ScanKeys, node->biss_NumScanKeys,
						 NULL, 0);
	}

	if (!node->ss.ps.instrument)
		return;

	node->biss_SharedInfo = (SharedIndexScanInstrumentation *)
		shm_toc_lookup(pwcxt->toc,
					   node->ss.ps.plan->plan_node_id +
					   PARALLEL_KEY_SCAN_INSTRUMENT_OFFSET,
					   false);
}

/* ----------------------------------------------------------------
 * ExecBitmapIndexScanRetrieveInstrumentation
 *
 *		Transfer bitmap index scan statistics from DSM to private memory.
 * ----------------------------------------------------------------
 */
void
ExecBitmapIndexScanRetrieveInstrumentation(BitmapIndexScanState *node)
{
	SharedIndexScanInstrumentation *SharedInfo = node->biss_SharedInfo;
	size_t		size;

	if (SharedInfo == NULL)
		return;

	/* Create a copy of SharedInfo in backend-local memory */
	size = offsetof(SharedIndexScanInstrumentation, winstrument) +
		SharedInfo->num_workers * sizeof(IndexScanInstrumentation);
	node->biss_SharedInfo = palloc(size);
	memcpy(node->biss_SharedInfo, SharedInfo, size);
}

/*
 * This function improves on the costy modulo operation.
 *
 * It's based on Daniel Lemire's fast alternative to the modulo reduction.
 * Paper "Fast Random Integer Generation in an Interval", 2018, Daniel Lemire
 */
inline static uint32_t
FastMod(uint32_t hash, uint32_t nparticipants)
{
	/*
	 * This works as follows:
	 *   hash is in range [0, 2^32-1]
	 *   hash / 2^32 is in range [0, 1]
	 *   hash / 2^32 * nparticipants is in range [0, nparticipants-1]
	 *
	 * To avoid fractional math, rearrange to:
	 *   (hash * nparticipants) / 2^32
	 */
	return ((uint64_t)hash * (uint64_t)nparticipants) >> 32;
}

/*
 * Calculate the work_mem to use during the index scan phase.
 *
 * The formula is: work_mem - (work_mem / (1 + max_participants))
 *
 * The split maximizes the memory we give to the index scan phase while ensuring
 * there's enough memory left to represent the same TIDBitmap in its partitioned form.
 */
static Size
BitmapIndexScanWorkMemForScanning(const SharedBitmapIndexState *sstate)
{
	if (sstate == NULL)
		return work_mem * (Size) 1024;


	return work_mem * (Size) 1024 - BitmapIndexScanWorkMemForPartitioning(sstate);
}

/*
 * Calculate the work_mem to use during the partitioning phase.
 *
 * The formula is: (work_mem / (1 + nparticipants)
 *
 * This is what we reserved and didn't use during the index scan phase.
 * The split maximizes the memory we give to the index scan phase while ensuring
 * there's enough memory left to represent the same TIDBitmap in its partitioned form.
 */
static Size
BitmapIndexScanWorkMemForPartitioning(const SharedBitmapIndexState *sstate)
{
	Assert(sstate != NULL);
	return ((work_mem / (1 + sstate->max_participants))) * (Size) 1024;
}

/*
 * In a parallel bitmap index scan, only one participant's shared memory is
 * used for coordination.  Return true if this node instance is the one
 * who's memory is used for coordination.
 */
static inline bool
BitmapIndexScanIsCoordinationOwner(const BitmapIndexScanState *node)
{
	return node->ss.ps.plan->plan_node_id == node->biss_FirstBisPlanNodeId;
}

/*
 * Compute the size of the SharedBitmapIndexState for a given number of
 * requested workers.
 */
static Size
BitmapIndexScanSharedStateSize(int nworkers)
{
	return add_size(offsetof(SharedBitmapIndexState, participants),
					mul_size(nworkers + 1,
							 sizeof(SharedBitmapIndexParticipant)));
}

/*
 * Given a full per-worker TIDBitmap built by a parallel-aware BitmapIndexScan,
 * share it with all other workers and return a new TIDBitmap containing only
 * the HEAP blocks assigned to this worker by a hash of the block number.
 *
 * The original TIDBitmap is freed here, after all workers have finished using
 * the shared iterator state.
 */
static TIDBitmap *
BitmapIndexScanPartition(BitmapIndexScanState *node, TIDBitmap *tbm)
{
	/*
	 * Use a stable participant ID across multiple Bitmap Index Scans in the
	 * same query. This ensures consistent partitioning for Bitmap And / Or
	 * operations.
	 */
	const int	iter_idx = (IsParallelWorker() ? ParallelWorkerNumber + 1 : 0);
	int			partition_idx = 0;
	SharedBitmapIndexState *sstate = node->biss_ParallelState;
	SharedBitmapIndexState *coord = node->biss_CoordState;
	dsa_area   *dsa = node->ss.ps.state->es_query_dsa;
	int			nparticipants = 0;
	int			i;
	TIDBitmap  *partition;

	Assert(dsa != NULL);
	Assert(iter_idx < sstate->max_participants);


	/* Wait until every participant has stored its bitmap. */
	sstate->participants[iter_idx].tbmiter = tbm_prepare_shared_unordered_iterate(tbm);
	BarrierArriveAndWait(&sstate->barrier, WAIT_EVENT_PARALLEL_BITMAP_SCAN);

	for (i = 0; i < sstate->max_participants; i++)
	{
		if (!coord->participants[i].participates)
			continue;
		if (i < iter_idx)
			partition_idx++;
		nparticipants++;
	}

	Assert(nparticipants > 0);
	Assert(partition_idx < nparticipants);

	/* Build the per-worker partition. */
	partition = tbm_create(BitmapIndexScanWorkMemForPartitioning(node->biss_ParallelState), NULL);

	for (i = 0; i < sstate->max_participants; i++)
	{
		dsa_pointer slot = sstate->participants[i].tbmiter;
		TBMUnorderedIterator *iter;
		TBMIterateResult tbmres;

		if (!DsaPointerIsValid(slot))
			continue;

		/*
		 * Scan the whole shared bitmap with a private iterator; we must not
		 * use the joint shared iterator, or the participants would divide the
		 * bitmap's pages among themselves instead of each scanning them all.
		 *
		 * Use the unsorted iterator since order doesn't matter for partition
		 * assignment - this avoids the complex merge sort logic needed for
		 * sorted iteration.
		 */
		iter = tbm_begin_shared_unordered_iterate(dsa, slot);

		while (tbm_shared_unordered_iterate(iter, &tbmres))
			if (FastMod(murmurhash32(tbmres.blockno / 256), nparticipants) == (uint32)partition_idx)
				tbm_copy_page(partition, &tbmres);

		tbm_end_shared_unordered_iterate(&iter);
	}

	/* Wait until everyone is done reading the shared bitmaps. */
	BarrierArriveAndWait(&sstate->barrier, WAIT_EVENT_PARALLEL_BITMAP_SCAN);

	/* Free the original bitmap and the shared iterator state we created. */
	tbm_free(tbm);
	if (DsaPointerIsValid(sstate->participants[iter_idx].tbmiter))
		dsa_free(dsa, sstate->participants[iter_idx].tbmiter);
	sstate->participants[iter_idx].tbmiter = InvalidDsaPointer;

	/* Only the first scan uses a dynamic barrier and must undo its attachment. */
	if (BitmapIndexScanIsCoordinationOwner(node))
		BarrierDetach(&sstate->barrier);

	return partition;
}
