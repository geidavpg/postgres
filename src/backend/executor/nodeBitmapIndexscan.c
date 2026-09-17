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
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/relscan.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/nodeBitmapIndexscan.h"
#include "executor/nodeIndexscan.h"
#include "miscadmin.h"
#include "nodes/tidbitmap.h"
#include "port/atomics.h"
#include "storage/barrier.h"
#include "utils/wait_event.h"

/*
 * Per-BitmapIndexScan shared state used to share locally-built bitmaps among
 * workers and coordinate the partition/free phases.
 */
typedef struct SharedBitmapIndexState
{
	int			max_participants;	/* leader + max number of workers */
	pg_atomic_uint32 next_participant_id;	/* assigns contiguous ids */
	Barrier		barrier;
	dsa_pointer worker_tbmiter[FLEXIBLE_ARRAY_MEMBER];
} SharedBitmapIndexState;

#define PARALLEL_KEY_BITMAP_INDEX_OFFSET UINT64CONST(0xD100000000000000)

static Size BitmapIndexScanSharedStateSize(int nworkers);
static TIDBitmap *BitmapIndexScanPartition(BitmapIndexScanState *node,
									   TIDBitmap *tbm);


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
	 * barrier before scanning the index.  This ensures that workers that arrive
	 * late don't waste time scanning the index only to discard their bitmap.
	 */
	if (node->biss_ParallelState != NULL)
	{
		SharedBitmapIndexState *sstate = node->biss_ParallelState;
		int			phase;

		phase = BarrierAttach(&sstate->barrier);
		if (phase != 0)
		{
			/*
			 * We attached after the build phase was already done.  We cannot
			 * contribute a partition, so just detach and return an empty bitmap.
			 */
			TIDBitmap  *empty_bitmap;

			BarrierDetach(&sstate->barrier);
			empty_bitmap = tbm_create(work_mem * (Size) 1024, NULL);
			if (node->ss.ps.instrument)
				InstrStopNode(node->ss.ps.instrument, 0);
			return (Node *) empty_bitmap;
		}
	}

	/*
	 * Prepare the result bitmap.  Normally we just create a new one to pass
	 * back; however, our parent node is allowed to store a pre-made one into
	 * node->biss_result, in which case we just OR our tuple IDs into the
	 * existing bitmap.  (This saves needing explicit UNION steps.)
	 */
	if (node->biss_result)
	{
		tbm = node->biss_result;
		node->biss_result = NULL;	/* reset for next time */
	}
	else
	{
		/* XXX should we use less than work_mem for this? */
		tbm = tbm_create(work_mem * (Size) 1024,
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
	 * so that each worker gets a disjoint subset of the heap blocks.
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
	 * tuple is scanned.  Note this assumes that we will recalculate *all*
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
		 * shutdown on the workers.  On rescan it will spin up new workers
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

	/*
	 * We do not open or lock the base relation here.  We assume that an
	 * ancestor BitmapHeapScan node is holding AccessShareLock (or better) on
	 * the heap relation throughout the execution of the plan tree.
	 *
	 * For a parallel-aware scan, however, we need access to the heap relation
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
	 * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
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
	 * Initialize scan descriptor.  For parallel-aware scans this is delayed
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
		shm_toc_estimate_chunk(&pcxt->estimator,
							   BitmapIndexScanSharedStateSize(pcxt->nworkers));
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
		pg_atomic_init_u32(&sstate->next_participant_id, 0);
		BarrierInit(&sstate->barrier, 0);
		shm_toc_insert(pcxt->toc,
					   node->ss.ps.plan->plan_node_id +
					   PARALLEL_KEY_BITMAP_INDEX_OFFSET,
					   sstate);
		node->biss_ParallelState = sstate;

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
	size = sstate->max_participants * sizeof(dsa_pointer);
	memset(sstate->worker_tbmiter, 0, size);

	pg_atomic_init_u32(&sstate->next_participant_id, 0);
	BarrierInit(&sstate->barrier, 0);
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
	SharedBitmapIndexState *sstate;

	if (node->ss.ps.plan->parallel_aware)
	{
		piscan = shm_toc_lookup(pwcxt->toc,
								node->ss.ps.plan->plan_node_id,
								false);
		sstate = shm_toc_lookup(pwcxt->toc,
								node->ss.ps.plan->plan_node_id +
								PARALLEL_KEY_BITMAP_INDEX_OFFSET,
								false);
		node->biss_ParallelState = sstate;

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
 * Compute the size of the SharedBitmapIndexState for a given number of
 * requested workers.
 */
static Size
BitmapIndexScanSharedStateSize(int nworkers)
{
	return add_size(offsetof(SharedBitmapIndexState, worker_tbmiter),
					mul_size(nworkers + 1, sizeof(dsa_pointer)));
}



/*
 * Given a full per-worker TIDBitmap built by a parallel-aware BitmapIndexScan,
 * share it with all other workers and return a new TIDBitmap containing only
 * the heap blocks assigned to this worker by a hash of the block number.
 *
 * The original TIDBitmap is freed here, after all workers have finished using
 * the shared iterator state.
 */
static TIDBitmap *
BitmapIndexScanPartition(BitmapIndexScanState *node, TIDBitmap *tbm)
{
	SharedBitmapIndexState *sstate = node->biss_ParallelState;
	dsa_area   *dsa = node->ss.ps.state->es_query_dsa;
	int			my_id;
	int			total;
	int			i;
	dsa_pointer dp = InvalidDsaPointer;
	TIDBitmap  *partition;

	Assert(dsa != NULL);

	my_id = pg_atomic_add_fetch_u32(&sstate->next_participant_id, 1) - 1;
	Assert(my_id < sstate->max_participants);

	/* Share our local bitmap, unless it is empty. */
	if (!tbm_is_empty(tbm))
		dp = tbm_prepare_shared_unordered_iterate(tbm);
	sstate->worker_tbmiter[my_id] = dp;

	/* Wait until every participant has stored its bitmap. */
	BarrierArriveAndWait(&sstate->barrier, WAIT_EVENT_PARALLEL_BITMAP_SCAN);

	total = BarrierParticipants(&sstate->barrier);
	Assert(total > 0);

	/* Build the per-worker partition. */
	partition = tbm_create(work_mem * (Size) 1024, NULL);

	for (i = 0; i < sstate->max_participants; i++)
	{
		dsa_pointer slot = sstate->worker_tbmiter[i];
		TBMUnorderedIterator *iter;
		TBMIterateResult tbmres;

		if (!DsaPointerIsValid(slot))
			continue;

		/*
		 * Scan the whole shared bitmap with a private cursor; we must not
		 * use the joint shared cursor, or the participants would divide the
		 * bitmap's pages among themselves instead of each scanning them all.
		 *
		 * Use the unsorted iterator since order doesn't matter for partition
		 * assignment - this avoids the complex merge sort logic needed for
		 * sorted iteration.
		 */
		iter = tbm_begin_shared_unordered_iterate(dsa, slot);

		while (tbm_shared_unordered_iterate(iter, &tbmres))
			if (murmurhash32(tbmres.blockno / 256) % (uint32) total == (uint32) my_id)
				tbm_copy_page(partition, &tbmres);

		tbm_end_shared_unordered_iterate(&iter);
	}

	/* Wait until everyone is done reading the shared bitmaps. */
	BarrierArriveAndWait(&sstate->barrier, WAIT_EVENT_PARALLEL_BITMAP_SCAN);

	/* Free the original bitmap and the shared iterator state we created. */
	tbm_free(tbm);
	sstate->worker_tbmiter[my_id] = InvalidDsaPointer;
	BarrierDetach(&sstate->barrier);

	return partition;
}
