/*-------------------------------------------------------------------------
 *
 * nodeBitmapHeapscan.c
 *	  Routines to support bitmapped scans of relations
 *
 * NOTE: it is critical that this plan type only be used with MVCC-compliant
 * snapshots (ie, regular snapshots, not SnapshotAny or one of the other
 * special snapshots).  The reason is that since index and heap scans are
 * decoupled, there can be no assurance that the index tuple prompting a
 * visit to a particular heap TID still exists when the visit is made.
 * Therefore the tuple might not exist anymore either (which is OK because
 * heap_fetch will cope) --- but worse, the tuple slot could have been
 * re-used for a newer tuple.  With an MVCC snapshot the newer tuple is
 * certain to fail the time qual and so it will not be mistakenly returned,
 * but with anything else we might return a tuple that doesn't meet the
 * required index qual conditions.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeBitmapHeapscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecBitmapHeapScan			scans a relation using bitmap info
 *		ExecBitmapHeapNext			workhorse for above
 *		ExecInitBitmapHeapScan		creates and initializes state info.
 *		ExecReScanBitmapHeapScan	prepares to rescan the plan.
 *		ExecEndBitmapHeapScan		releases all storage.
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/nodeBitmapHeapscan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "utils/spccache.h"

static void BitmapTableScanSetup(BitmapHeapScanState *node);
static TupleTableSlot *BitmapHeapNext(BitmapHeapScanState *node);


/*
 * Do the underlying index scan, build the bitmap, and set up the underlying
 * table scan descriptor.  With parallel-aware BitmapHeapScans, the underlying
 * BitmapIndexScan(s) have already partitioned their output per worker.
 */
static void
BitmapTableScanSetup(BitmapHeapScanState *node)
{
	TBMOrderedIterator *tbmiterator;

	node->tbm = (TIDBitmap *) MultiExecProcNode(outerPlanState(node));

	if (!node->tbm || !IsA(node->tbm, TIDBitmap))
		elog(ERROR, "unrecognized result from subplan");

	/*
	 * The bitmap we receive is already a per-worker partition, so a private
	 * iterator is sufficient.
	 */
	tbmiterator = tbm_begin_ordered_iterate(node->tbm);

	/*
	 * If this is the first scan of the underlying table, create the table
	 * scan descriptor and begin the scan.
	 */
	if (!node->ss.ss_currentScanDesc)
	{
		uint32		flags = SO_NONE;

		if (ScanRelIsReadOnly(&node->ss))
			flags |= SO_HINT_REL_READ_ONLY;

		if (node->ss.ps.state->es_instrument & INSTRUMENT_IO)
			flags |= SO_SCAN_INSTRUMENT;

		node->ss.ss_currentScanDesc =
			table_beginscan_bm(node->ss.ss_currentRelation,
							   node->ss.ps.state->es_snapshot,
							   0,
							   NULL,
							   flags);
	}

	node->ss.ss_currentScanDesc->st.rs_tbmiterator = tbmiterator;
	node->initialized = true;
}

/* ----------------------------------------------------------------
 *		BitmapHeapNext
 *
 *		Retrieve next tuple from the BitmapHeapScan node's currentRelation
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
BitmapHeapNext(BitmapHeapScanState *node)
{
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	/*
	 * If we haven't yet performed the underlying index scan, do it, and begin
	 * the iteration over the bitmap.
	 */
	if (!node->initialized)
		BitmapTableScanSetup(node);

	while (table_scan_bitmap_next_tuple(node->ss.ss_currentScanDesc,
										slot, &node->recheck,
										&node->stats.lossy_pages,
										&node->stats.exact_pages))
	{
		/*
		 * Continuing in previously obtained page.
		 */
		CHECK_FOR_INTERRUPTS();

		/*
		 * If we are using lossy info, we have to recheck the qual conditions
		 * at every tuple.
		 */
		if (node->recheck)
		{
			econtext->ecxt_scantuple = slot;
			if (!ExecQualAndReset(node->bitmapqualorig, econtext))
			{
				/* Fails recheck, so drop it and loop back for another */
				InstrCountFiltered2(node, 1);
				ExecClearTuple(slot);
				continue;
			}
		}

		/* OK to return this tuple */
		return slot;
	}

	/*
	 * if we get here it means we are at the end of the scan..
	 */
	return ExecClearTuple(slot);
}

/*
 * BitmapHeapRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
BitmapHeapRecheck(BitmapHeapScanState *node, TupleTableSlot *slot)
{
	ExprContext *econtext;

	/*
	 * extract necessary information from index scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;

	/* Does the tuple meet the original qual conditions? */
	econtext->ecxt_scantuple = slot;
	return ExecQualAndReset(node->bitmapqualorig, econtext);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapScan(node)
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecBitmapHeapScan(PlanState *pstate)
{
	BitmapHeapScanState *node = castNode(BitmapHeapScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) BitmapHeapNext,
					(ExecScanRecheckMtd) BitmapHeapRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanBitmapHeapScan(node)
 * ----------------------------------------------------------------
 */
void
ExecReScanBitmapHeapScan(BitmapHeapScanState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	TableScanDesc scan = node->ss.ss_currentScanDesc;

	if (scan)
	{
		/*
		 * End iteration on iterators saved in scan descriptor if they have
		 * not already been cleaned up.
		 */
		if (scan->st.rs_tbmiterator != NULL)
			tbm_end_ordered_iterate(&scan->st.rs_tbmiterator);

		/* rescan to release any page pin */
		table_rescan(node->ss.ss_currentScanDesc, NULL);
	}

	/* release bitmaps and buffers if any */
	if (node->tbm)
		tbm_free(node->tbm);
	node->tbm = NULL;
	node->initialized = false;
	node->recheck = true;

	ExecScanReScan(&node->ss);

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapHeapScan
 * ----------------------------------------------------------------
 */
void
ExecEndBitmapHeapScan(BitmapHeapScanState *node)
{
	TableScanDesc scanDesc;

	/*
	 * When ending a parallel worker, copy the statistics gathered by the
	 * worker back into shared memory so that it can be picked up by the main
	 * process to report in EXPLAIN ANALYZE.
	 */
	if (node->sinstrument != NULL && IsParallelWorker())
	{
		BitmapHeapScanInstrumentation *si;

		Assert(ParallelWorkerNumber < node->sinstrument->num_workers);
		si = &node->sinstrument->sinstrument[ParallelWorkerNumber];

		/*
		 * Here we accumulate the stats rather than performing memcpy on
		 * node->stats into si.  When a Gather/GatherMerge node finishes it
		 * will perform planner shutdown on the workers.  On rescan it will
		 * spin up new workers which will have a new BitmapHeapScanState and
		 * zeroed stats.
		 */
		si->exact_pages += node->stats.exact_pages;
		si->lossy_pages += node->stats.lossy_pages;

		/* collect I/O instrumentation for this process */
		if (node->ss.ss_currentScanDesc &&
			node->ss.ss_currentScanDesc->rs_instrument)
		{
			AccumulateIOStats(&si->stats.io,
							  &node->ss.ss_currentScanDesc->rs_instrument->io);
		}
	}

	/*
	 * extract information from the node
	 */
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * close down subplans
	 */
	ExecEndNode(outerPlanState(node));

	if (scanDesc)
	{
		/*
		 * End iteration on iterators saved in scan descriptor if they have
		 * not already been cleaned up.
		 */
		if (scanDesc->st.rs_tbmiterator != NULL)
			tbm_end_ordered_iterate(&scanDesc->st.rs_tbmiterator);

		/*
		 * close table scan
		 */
		table_endscan(scanDesc);
	}

	/*
	 * release bitmaps and buffers if any
	 */
	if (node->tbm)
		tbm_free(node->tbm);
}

/* ----------------------------------------------------------------
 *		ExecInitBitmapHeapScan
 *
 *		Initializes the scan's state information.
 * ----------------------------------------------------------------
 */
BitmapHeapScanState *
ExecInitBitmapHeapScan(BitmapHeapScan *node, EState *estate, int eflags)
{
	BitmapHeapScanState *scanstate;
	Relation	currentRelation;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * Assert caller didn't ask for an unsafe snapshot --- see comments at
	 * head of file.
	 */
	Assert(IsMVCCSnapshot(estate->es_snapshot));

	/*
	 * create state structure
	 */
	scanstate = makeNode(BitmapHeapScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = ExecBitmapHeapScan;

	scanstate->tbm = NULL;

	/* Zero the statistics counters */
	memset(&scanstate->stats, 0, sizeof(BitmapHeapScanInstrumentation));

	scanstate->initialized = false;
	scanstate->recheck = true;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * open the scan relation
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);

	/*
	 * initialize child nodes
	 */
	outerPlanState(scanstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * get the scan type from the relation descriptor.
	 */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
						  RelationGetDescr(currentRelation),
						  table_slot_callbacks(currentRelation),
						  TTS_FLAG_OBEYS_NOT_NULL_CONSTRAINTS);

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);
	scanstate->bitmapqualorig =
		ExecInitQual(node->bitmapqualorig, (PlanState *) scanstate);

	scanstate->ss.ss_currentRelation = currentRelation;

	/*
	 * all done.
	 */
	return scanstate;
}

/*
 * Compute the amount of space we'll need for the shared instrumentation and
 * inform pcxt->estimator.
 */
void
ExecBitmapHeapInstrumentEstimate(BitmapHeapScanState *node,
								 ParallelContext *pcxt)
{
	Size		size;

	if (!node->ss.ps.instrument || pcxt->nworkers == 0)
		return;

	size = add_size(offsetof(SharedBitmapHeapInstrumentation, sinstrument),
				  mul_size(pcxt->nworkers, sizeof(BitmapHeapScanInstrumentation)));
	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/*
 * Set up parallel bitmap heap scan instrumentation.
 */
void
ExecBitmapHeapInstrumentInitDSM(BitmapHeapScanState *node,
								ParallelContext *pcxt)
{
	Size		size;

	if (!node->ss.ps.instrument || pcxt->nworkers == 0)
		return;

	size = add_size(offsetof(SharedBitmapHeapInstrumentation, sinstrument),
				  mul_size(pcxt->nworkers, sizeof(BitmapHeapScanInstrumentation)));
	node->sinstrument =
		(SharedBitmapHeapInstrumentation *) shm_toc_allocate(pcxt->toc, size);

	/* Each per-worker area must start out as zeroes */
	memset(node->sinstrument, 0, size);
	node->sinstrument->num_workers = pcxt->nworkers;
	shm_toc_insert(pcxt->toc,
				   node->ss.ps.plan->plan_node_id +
				   PARALLEL_KEY_SCAN_INSTRUMENT_OFFSET,
				   node->sinstrument);
}

/*
 * Look up and save the location of the shared instrumentation.
 */
void
ExecBitmapHeapInstrumentInitWorker(BitmapHeapScanState *node,
								   ParallelWorkerContext *pwcxt)
{
	if (!node->ss.ps.instrument)
		return;

	node->sinstrument = (SharedBitmapHeapInstrumentation *)
		shm_toc_lookup(pwcxt->toc,
					   node->ss.ps.plan->plan_node_id +
					   PARALLEL_KEY_SCAN_INSTRUMENT_OFFSET,
					   false);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapRetrieveInstrumentation
 *
 *		Transfer bitmap heap scan statistics from DSM to private memory.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapRetrieveInstrumentation(BitmapHeapScanState *node)
{
	SharedBitmapHeapInstrumentation *sinstrument = node->sinstrument;
	Size		size;

	if (sinstrument == NULL)
		return;

	size = offsetof(SharedBitmapHeapInstrumentation, sinstrument)
		+ sinstrument->num_workers * sizeof(BitmapHeapScanInstrumentation);

	node->sinstrument = palloc(size);
	memcpy(node->sinstrument, sinstrument, size);
}
