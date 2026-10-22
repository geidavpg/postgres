/*-------------------------------------------------------------------------
 *
 * nodeCtescan.c
 *	  routines to handle CteScan nodes.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeCtescan.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/nodeCtescan.h"
#include "miscadmin.h"
#include "storage/condition_variable.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"
#include "utils/tuplestore.h"
#include "utils/wait_event.h"

#define PARALLEL_CTE_MAX_CURSORS	256

/*
 * Shared tuple buffer used by parallel-aware CteScan nodes.  All
 * participants executing the (partial) plan of a given CTE append the rows
 * they produce to the buffer, and every reference to the CTE has a cursor
 * whose participants collectively read each row exactly once.
 *
 * This is a deliberately simple first implementation: the buffer is a
 * fixed-size chunk of dynamic shared memory, all access is serialized by a
 * single spinlock, and tuples are never removed.  Readers that catch up
 * with the writer wait on the condition variable.
 */
typedef struct SharedCteStore
{
	slock_t		mutex;
	ConditionVariable cv;
	Size		capacity;		/* size of data[] in bytes */
	int32		writeoff;		/* bump-allocation write offset */
	int			nproducers;		/* registered producer participants */
	int			ndone;			/* producers that reached EOF */
	int			ncursors;
	int			cursorid[PARALLEL_CTE_MAX_CURSORS];
	int32		readoff[PARALLEL_CTE_MAX_CURSORS];
	char		data[FLEXIBLE_ARRAY_MEMBER];
} SharedCteStore;

/* shm_toc key range used for shared CTE stores; add ctePlanId */
#define PARALLEL_CTE_STORE_KEY		UINT64CONST(0xE000000000000100)

static uint32 cte_scan_wait_event = 0;

static TupleTableSlot *CteScanNext(CteScanState *node);
static TupleTableSlot *CteScanNextShared(CteScanState *node);

/* ----------------------------------------------------------------
 *		SharedCteStore API
 *
 * All locking and condition-variable protocol is encapsulated here;
 * callers must not access struct fields directly.
 * ----------------------------------------------------------------
 */

/*
 * Initialize a SharedCteStore in already-allocated shared memory of the
 * given total size.
 */
static void
CteStoreInit(SharedCteStore *store, Size size)
{
	SpinLockInit(&store->mutex);
	ConditionVariableInit(&store->cv);
	store->capacity = size - offsetof(SharedCteStore, data);
	store->writeoff = 0;
	store->nproducers = 0;
	store->ndone = 0;
	store->ncursors = 0;
}

/*
 * Find the read cursor for the given CTE reference ID, creating it if
 * necessary.  Returns the cursor index.
 */
static int
CteStoreRegisterCursor(SharedCteStore *store, int refid)
{
	int			i;

	SpinLockAcquire(&store->mutex);
	for (i = 0; i < store->ncursors; i++)
	{
		if (store->cursorid[i] == refid)
		{
			SpinLockRelease(&store->mutex);
			return i;
		}
	}
	if (store->ncursors >= PARALLEL_CTE_MAX_CURSORS)
	{
		SpinLockRelease(&store->mutex);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("too many parallel CTE scan references")));
	}
	i = store->ncursors++;
	store->cursorid[i] = refid;
	store->readoff[i] = 0;
	SpinLockRelease(&store->mutex);
	return i;
}

/*
 * Fetch the next unclaimed row for the given cursor, or NULL if the store
 * currently holds none.  The returned MinimalTuple is palloc'd.
 */
static MinimalTuple
CteStoreNext(SharedCteStore *store, int cursor)
{
	MinimalTuple mtup;
	uint32		len;
	int32		off;

	SpinLockAcquire(&store->mutex);
	off = store->readoff[cursor];
	if (off >= store->writeoff)
	{
		SpinLockRelease(&store->mutex);
		return NULL;
	}
	memcpy(&len, store->data + off, sizeof(uint32));
	mtup = (MinimalTuple) palloc(len);
	memcpy(mtup, store->data + off + sizeof(uint32), len);
	store->readoff[cursor] = off + MAXALIGN(sizeof(uint32) + len);
	SpinLockRelease(&store->mutex);
	return mtup;
}

/*
 * Append a row to the shared buffer and wake all waiters.
 */
static void
CteStorePutTuple(SharedCteStore *store, MinimalTuple mtup)
{
	uint32		len = mtup->t_len;
	Size		needed = MAXALIGN(sizeof(uint32) + len);

	SpinLockAcquire(&store->mutex);
	if (store->writeoff + needed > store->capacity)
	{
		SpinLockRelease(&store->mutex);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("parallel CTE tuple store overflow"),
				 errhint("The shared CTE buffer is too small for the "
						 "CTE result.")));
	}
	memcpy(store->data + store->writeoff, &len, sizeof(uint32));
	memcpy(store->data + store->writeoff + sizeof(uint32), mtup, len);
	store->writeoff += needed;
	SpinLockRelease(&store->mutex);
	ConditionVariableBroadcast(&store->cv);
}

/*
 * Register this participant as a producer; the return value is just the
 * assigned slot and is only for caller bookkeeping.
 */
static int
CteStoreRegisterProducer(SharedCteStore *store)
{
	int			slot;

	SpinLockAcquire(&store->mutex);
	slot = store->nproducers++;
	SpinLockRelease(&store->mutex);
	return slot;
}

/*
 * Report that this participant's producer has reached EOF, and wake all
 * waiters in case that completes the CTE result.
 */
static void
CteStoreEndProducer(SharedCteStore *store)
{
	SpinLockAcquire(&store->mutex);
	store->ndone++;
	SpinLockRelease(&store->mutex);
	ConditionVariableBroadcast(&store->cv);
}

/*
 * Test whether every registered producer has reached EOF.  Returns false
 * while no producer has registered yet.
 */
static bool
CteStoreComplete(SharedCteStore *store)
{
	bool		done;

	SpinLockAcquire(&store->mutex);
	done = (store->nproducers > 0 && store->ndone >= store->nproducers);
	SpinLockRelease(&store->mutex);
	return done;
}

/*
 * Prepare to wait for rows or producer completion.  Must be paired with
 * CteStoreWaitEnd; CteStoreWait may be called any number of times in
 * between.
 */
static void
CteStoreWaitBegin(SharedCteStore *store)
{
	ConditionVariablePrepareToSleep(&store->cv);
}

static void
CteStoreWait(SharedCteStore *store)
{
	if (cte_scan_wait_event == 0)
		cte_scan_wait_event = WaitEventExtensionNew("ParallelCteScan");
	ConditionVariableSleep(&store->cv, cte_scan_wait_event);
}

static void
CteStoreWaitEnd(SharedCteStore *store)
{
	ConditionVariableCancelSleep();
}

/*
 * Rewind a single cursor to the start of the buffer (CTE unchanged).
 */
static void
CteStoreRewind(SharedCteStore *store, int cursor)
{
	SpinLockAcquire(&store->mutex);
	store->readoff[cursor] = 0;
	SpinLockRelease(&store->mutex);
}

/*
 * Discard the whole buffer contents and all producer/cursor positions so
 * that the CTE can be produced again from scratch.
 */
static void
CteStoreReset(SharedCteStore *store)
{
	int			i;

	SpinLockAcquire(&store->mutex);
	store->writeoff = 0;
	store->nproducers = 0;
	store->ndone = 0;
	for (i = 0; i < store->ncursors; i++)
		store->readoff[i] = 0;
	SpinLockRelease(&store->mutex);
}

/* ----------------------------------------------------------------
 *		CteScanNext
 *
 *		This is a workhorse for ExecCteScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
CteScanNext(CteScanState *node)
{
	EState	   *estate;
	ScanDirection dir;
	bool		forward;
	Tuplestorestate *tuplestorestate;
	bool		eof_tuplestore;
	TupleTableSlot *slot;

	/* Parallel-aware scans read from the shared CTE store instead. */
	if (node->shared != NULL)
		return CteScanNextShared(node);

	/*
	 * get state info from node
	 */
	estate = node->ss.ps.state;
	dir = estate->es_direction;
	forward = ScanDirectionIsForward(dir);
	tuplestorestate = node->leader->cte_table;
	tuplestore_select_read_pointer(tuplestorestate, node->readptr);
	slot = node->ss.ss_ScanTupleSlot;

	/*
	 * If we are not at the end of the tuplestore, or are going backwards, try
	 * to fetch a tuple from tuplestore.
	 */
	eof_tuplestore = tuplestore_ateof(tuplestorestate);

	if (!forward && eof_tuplestore)
	{
		if (!node->leader->eof_cte)
		{
			/*
			 * When reversing direction at tuplestore EOF, the first
			 * gettupleslot call will fetch the last-added tuple; but we want
			 * to return the one before that, if possible. So do an extra
			 * fetch.
			 */
			if (!tuplestore_advance(tuplestorestate, forward))
				return NULL;	/* the tuplestore must be empty */
		}
		eof_tuplestore = false;
	}

	/*
	 * If we can fetch another tuple from the tuplestore, return it.
	 *
	 * Note: we have to use copy=true in the tuplestore_gettupleslot call,
	 * because we are sharing the tuplestore with other nodes that might write
	 * into the tuplestore before we get called again.
	 */
	if (!eof_tuplestore)
	{
		if (tuplestore_gettupleslot(tuplestorestate, forward, true, slot))
			return slot;
		if (forward)
			eof_tuplestore = true;
	}

	/*
	 * If necessary, try to fetch another row from the CTE query.
	 *
	 * Note: the eof_cte state variable exists to short-circuit further calls
	 * of the CTE plan.  It's not optional, unfortunately, because some plan
	 * node types are not robust about being called again when they've already
	 * returned NULL.
	 */
	if (eof_tuplestore && !node->leader->eof_cte)
	{
		TupleTableSlot *cteslot;

		/*
		 * We can only get here with forward==true, so no need to worry about
		 * which direction the subplan will go.
		 */
		cteslot = ExecProcNode(node->cteplanstate);
		if (TupIsNull(cteslot))
		{
			node->leader->eof_cte = true;
			return NULL;
		}

		/*
		 * There are corner cases where the subplan could change which
		 * tuplestore read pointer is active, so be sure to reselect ours
		 * before storing the tuple we got.
		 */
		tuplestore_select_read_pointer(tuplestorestate, node->readptr);

		/*
		 * Append a copy of the returned tuple to tuplestore.  NOTE: because
		 * our read pointer is certainly in EOF state, its read position will
		 * move forward over the added tuple.  This is what we want.  Also,
		 * any other readers will *not* move past the new tuple, which is what
		 * they want.
		 */
		tuplestore_puttupleslot(tuplestorestate, cteslot);

		/*
		 * We MUST copy the CTE query's output tuple into our own slot. This
		 * is because other CteScan nodes might advance the CTE query before
		 * we are called again, and our output tuple must stay stable over
		 * that.
		 */
		return ExecCopySlot(slot, cteslot);
	}

	/*
	 * Nothing left ...
	 */
	return ExecClearTuple(slot);
}

/*
 * CteScanNextShared -- fetch the next row for a parallel-aware CteScan
 *
 * The caller's reference cursor is advanced over the shared buffer.  When
 * the buffer has no more rows for this cursor and the CTE is not complete,
 * the caller's own copy of the CTE's (partial) plan is advanced to produce
 * another row, which is appended to the shared buffer.  When all
 * registered producer participants have finished, the scan returns EOF.
 */
static TupleTableSlot *
CteScanNextShared(CteScanState *node)
{
	SharedCteStore *store = node->shared;
	CteScanState *leader = node->leader;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	int			refid = node->ss.ps.plan->plan_node_id;
	MinimalTuple mtup;

	if (!ScanDirectionIsForward(node->ss.ps.state->es_direction))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parallel CTE scans do not support backward scans")));

	if (node->shared_cursor < 0)
		node->shared_cursor = CteStoreRegisterCursor(store, refid);

	CteStoreWaitBegin(store);
	for (;;)
	{
		/* Fetch a buffered row for this reference, if any. */
		mtup = CteStoreNext(store, node->shared_cursor);
		if (mtup != NULL)
		{
			CteStoreWaitEnd(store);
			return ExecStoreMinimalTuple(mtup, slot, true);
		}

		if (CteStoreComplete(store))
		{
			CteStoreWaitEnd(store);
			return ExecClearTuple(slot);
		}

		/*
		 * If this participant's producer hasn't finished, produce the next
		 * row ourselves.  Any CteScan of this CTE in this process can drive
		 * the shared local producer; the leader CteScanState holds the
		 * per-participant producer state.
		 */
		if (!leader->eof_cte)
		{
			TupleTableSlot *cteslot;

			if (leader->participant < 0)
				leader->participant = CteStoreRegisterProducer(store);

			cteslot = ExecProcNode(leader->cteplanstate);
			if (TupIsNull(cteslot))
			{
				leader->eof_cte = true;
				CteStoreEndProducer(store);
				continue;
			}

			mtup = ExecCopySlotMinimalTuple(cteslot);
			CteStorePutTuple(store, mtup);
			pfree(mtup);
			continue;
		}

		/* Wait for another participant to publish rows or reach EOF. */
		CteStoreWait(store);
	}
}

/*
 * CteScanRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
CteScanRecheck(CteScanState *node, TupleTableSlot *slot)
{
	/* nothing to check */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecCteScan(node)
 *
 *		Scans the CTE sequentially and returns the next qualifying tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecCteScan(PlanState *pstate)
{
	CteScanState *node = castNode(CteScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) CteScanNext,
					(ExecScanRecheckMtd) CteScanRecheck);
}


/* ----------------------------------------------------------------
 *		ExecInitCteScan
 * ----------------------------------------------------------------
 */
CteScanState *
ExecInitCteScan(CteScan *node, EState *estate, int eflags)
{
	CteScanState *scanstate;
	ParamExecData *prmdata;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/*
	 * For the moment we have to force the tuplestore to allow REWIND, because
	 * we might be asked to rescan the CTE even though upper levels didn't
	 * tell us to be prepared to do it efficiently.  Annoying, since this
	 * prevents truncation of the tuplestore.  XXX FIXME
	 *
	 * Note: if we are in an EPQ recheck plan tree, it's likely that no access
	 * to the tuplestore is needed at all, making this even more annoying.
	 * It's not worth improving that as long as all the read pointers would
	 * have REWIND anyway, but if we ever improve this logic then that aspect
	 * should be considered too.
	 */
	eflags |= EXEC_FLAG_REWIND;

	/*
	 * CteScan should not have any children.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create new CteScanState for node
	 */
	scanstate = makeNode(CteScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = ExecCteScan;
	scanstate->eflags = eflags;
	scanstate->cte_table = NULL;
	scanstate->eof_cte = false;
	scanstate->shared = NULL;
	scanstate->shared_cursor = -1;
	scanstate->participant = -1;

	/*
	 * Find the already-initialized plan for the CTE query.
	 */
	scanstate->cteplanstate = (PlanState *) list_nth(estate->es_subplanstates,
													 node->ctePlanId - 1);

	/*
	 * The Param slot associated with the CTE query is used to hold a pointer
	 * to the CteState of the first CteScan node that initializes for this
	 * CTE.  This node will be the one that holds the shared state for all the
	 * CTEs, particularly the shared tuplestore.
	 */
	prmdata = &(estate->es_param_exec_vals[node->cteParam]);
	Assert(prmdata->execPlan == NULL);
	Assert(!prmdata->isnull);
	scanstate->leader = castNode(CteScanState, DatumGetPointer(prmdata->value));
	if (scanstate->leader == NULL)
	{
		/* I am the leader */
		prmdata->value = PointerGetDatum(scanstate);
		scanstate->leader = scanstate;
		scanstate->cte_table = tuplestore_begin_heap(true, false, work_mem);
		tuplestore_set_eflags(scanstate->cte_table, scanstate->eflags);
		scanstate->readptr = 0;
	}
	else
	{
		/* Not the leader */
		/* Create my own read pointer, and ensure it is at start */
		scanstate->readptr =
			tuplestore_alloc_read_pointer(scanstate->leader->cte_table,
										  scanstate->eflags);
		tuplestore_select_read_pointer(scanstate->leader->cte_table,
									   scanstate->readptr);
		tuplestore_rescan(scanstate->leader->cte_table);
	}

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * The scan tuple type (ie, the rowtype we expect to find in the work
	 * table) is the same as the result rowtype of the CTE query.
	 */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
						  ExecGetResultType(scanstate->cteplanstate),
						  &TTSOpsMinimalTuple, 0);

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

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndCteScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndCteScan(CteScanState *node)
{
	/*
	 * If I am the leader of a parallel-aware CTE, and this participant
	 * registered as a producer but never reached EOF (eg because the query
	 * finished early), report it done so other participants don't wait for
	 * rows that will never arrive.
	 */
	if (node->leader == node && node->shared != NULL &&
		node->participant >= 0 && !node->eof_cte)
	{
		node->eof_cte = true;
		CteStoreEndProducer(node->shared);
	}

	/*
	 * If I am the leader, free the tuplestore.
	 */
	if (node->leader == node)
	{
		tuplestore_end(node->cte_table);
		node->cte_table = NULL;
	}
}

/* ----------------------------------------------------------------
 *		ExecReScanCteScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanCteScan(CteScanState *node)
{
	Tuplestorestate *tuplestorestate = node->leader->cte_table;

	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	ExecScanReScan(&node->ss);

	if (node->shared != NULL)
	{
		if (node->leader->cteplanstate->chgParam != NULL)
		{
			/*
			 * The underlying CTE must be re-scanned, so discard the shared
			 * contents and let all participants start producing again.
			 */
			CteStoreReset(node->shared);
			node->leader->eof_cte = false;
			node->leader->participant = -1;
		}
		else if (node->shared_cursor >= 0)
		{
			/* Just rewind this reference's cursor. */
			CteStoreRewind(node->shared, node->shared_cursor);
		}
		return;
	}

	/*
	 * Clear the tuplestore if a new scan of the underlying CTE is required.
	 * This implicitly resets all the tuplestore's read pointers.  Note that
	 * multiple CTE nodes might redundantly clear the tuplestore; that's OK,
	 * and not unduly expensive.  We'll stop taking this path as soon as
	 * somebody has attempted to read something from the underlying CTE
	 * (thereby causing its chgParam to be cleared).
	 */
	if (node->leader->cteplanstate->chgParam != NULL)
	{
		tuplestore_clear(tuplestorestate);
		node->leader->eof_cte = false;
	}
	else
	{
		/*
		 * Else, just rewind my own pointer.  Either the underlying CTE
		 * doesn't need a rescan (and we can re-read what's in the tuplestore
		 * now), or somebody else already took care of it.
		 */
		tuplestore_select_read_pointer(tuplestorestate, node->readptr);
		tuplestore_rescan(tuplestorestate);
	}
}

/*
 * Compute the shared buffer capacity for this CTE, sized from the CTE
 * subplan's row and width estimates.  This is only a rough guess; rows are
 * appended to the buffer until the CTE is complete, and an overflow is
 * reported as an error.
 */
static Size
ExecCteScanStoreSize(CteScanState *node)
{
	Plan	   *subplan = node->cteplanstate->plan;
	double		bytes;

	bytes = subplan->plan_rows *
		(subplan->plan_width + sizeof(uint32) + sizeof(HeapTupleHeaderData));
	return (Size) Min(Max(bytes, 64 * 1024), 256 * 1024 * 1024);
}

/*
 * ExecCteScanEstimate
 *		Estimate space required for the CTE's shared tuple buffer.
 *
 * This is called once per parallel-aware CteScan node; the buffer is shared
 * between all references to the same CTE, so the estimate deliberately
 * over-counts.  shm_toc estimates only affect the segment size.
 */
void
ExecCteScanEstimate(CteScanState *node, ParallelContext *pcxt)
{
	shm_toc_estimate_chunk(&pcxt->estimator,
						   offsetof(SharedCteStore, data) +
						   ExecCteScanStoreSize(node));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/*
 * ExecCteScanInitializeDSM
 *		Find or create the CTE's shared tuple buffer in the leader.
 */
void
ExecCteScanInitializeDSM(CteScanState *node, ParallelContext *pcxt)
{
	CteScan    *cte = (CteScan *) node->ss.ps.plan;
	uint64		key = PARALLEL_CTE_STORE_KEY + cte->ctePlanId;
	SharedCteStore *store;

	store = shm_toc_lookup(pcxt->toc, key, true);
	if (store == NULL)
	{
		Size		size;

		size = offsetof(SharedCteStore, data) + ExecCteScanStoreSize(node);
		store = shm_toc_allocate(pcxt->toc, size);
		CteStoreInit(store, size);
		shm_toc_insert(pcxt->toc, key, store);
	}

	node->shared = store;
	node->shared_cursor = -1;
	node->participant = -1;
}

/*
 * ExecCteScanInitializeWorker
 *		Attach to the CTE's shared tuple buffer in a parallel worker.
 */
void
ExecCteScanInitializeWorker(CteScanState *node, ParallelWorkerContext *pwcxt)
{
	CteScan    *cte = (CteScan *) node->ss.ps.plan;
	uint64		key = PARALLEL_CTE_STORE_KEY + cte->ctePlanId;

	node->shared = shm_toc_lookup(pwcxt->toc, key, false);
	node->shared_cursor = -1;
	node->participant = -1;
}

/*
 * ExecCteScanReInitializeDSM
 *		Reset shared state for a rescan of the parallel query.
 */
void
ExecCteScanReInitializeDSM(CteScanState *node, ParallelContext *pcxt)
{
	if (node->shared == NULL)
		return;

	CteStoreReset(node->shared);

	node->leader->eof_cte = false;
	node->leader->participant = -1;
}
