/*-------------------------------------------------------------------------
 *
 * nodeCtescan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeCtescan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODECTESCAN_H
#define NODECTESCAN_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern CteScanState *ExecInitCteScan(CteScan *node, EState *estate, int eflags);
extern void ExecEndCteScan(CteScanState *node);
extern void ExecReScanCteScan(CteScanState *node);

extern void ExecCteScanEstimate(CteScanState *node, ParallelContext *pcxt);
extern void ExecCteScanInitializeDSM(CteScanState *node,
									 ParallelContext *pcxt);
extern void ExecCteScanInitializeWorker(CteScanState *node,
										ParallelWorkerContext *pwcxt);
extern void ExecCteScanReInitializeDSM(CteScanState *node,
									   ParallelContext *pcxt);

#endif							/* NODECTESCAN_H */
