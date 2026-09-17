/*-------------------------------------------------------------------------
 *
 * tidbitmap.h
 *	  PostgreSQL tuple-id (TID) bitmap package
 *
 * This module provides bitmap data structures that are spiritually
 * similar to Bitmapsets, but are specially adapted to store sets of
 * tuple identifiers (TIDs), or ItemPointers.  In particular, the division
 * of an ItemPointer into BlockNumber and OffsetNumber is catered for.
 * Also, since we wish to be able to store very large tuple sets in
 * memory with this data structure, we support "lossy" storage, in which
 * we no longer remember individual tuple offsets on a page but only the
 * fact that a particular page needs to be visited.
 *
 *
 * Copyright (c) 2003-2026, PostgreSQL Global Development Group
 *
 * src/include/nodes/tidbitmap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TIDBITMAP_H
#define TIDBITMAP_H

#include "storage/itemptr.h"
#include "utils/dsa.h"

/*
 * The maximum number of tuples per page is not large (typically 256 with
 * 8K pages, or 1024 with 32K pages).  So there's not much point in making
 * the per-page bitmaps variable size.  We just legislate that the size
 * is this:
 */
#define TBM_MAX_TUPLES_PER_PAGE  MaxHeapTuplesPerPage

/*
 * When we have to switch over to lossy storage, we use a data structure
 * with one bit per page, where all pages having the same number DIV
 * TBM_MAX_PAGES_PER_CHUNK are aggregated into one chunk.  When a chunk is present
 * and has the bit set for a given page, there must not be a per-page entry
 * for that page in the page table.
 *
 * We actually store both exact pages and lossy chunks in the same hash
 * table, using identical data structures.  (This is because the memory
 * management for hashtables doesn't easily/efficiently allow space to be
 * transferred easily from one hashtable to another.)  Therefore it's best
 * if TBM_MAX_PAGES_PER_CHUNK is the same as TBM_MAX_TUPLES_PER_PAGE, or at least not
 * too different.  But we also want TBM_MAX_PAGES_PER_CHUNK to be a power of 2 to
 * avoid expensive integer remainder operations.  So, define it like this:
 */
#define TBM_MAX_PAGES_PER_CHUNK  (BLCKSZ / 32)

/*
 * Actual bitmap representation is private to tidbitmap.c.  Callers can
 * do IsA(x, TIDBitmap) on it, but nothing else.
 */
typedef struct TIDBitmap TIDBitmap;

/* Likewise, iterators are private */
typedef struct TBMOrderedIterator TBMOrderedIterator;
typedef struct TBMUnorderedIterator TBMUnorderedIterator;

/* Result structure for iteration */
typedef struct TBMIterateResult
{
	BlockNumber blockno;		/* block number containing tuples */

	bool		lossy;

	/*
	 * Whether or not the tuples should be rechecked. This is always true if
	 * the page is lossy but may also be true if the query requires recheck.
	 */
	bool		recheck;

	/*
	 * Pointer to the page containing the bitmap for this block. It is a void *
	 * to avoid exposing the details of the tidbitmap PagetableEntry to API
	 * users.
	 */
	void	   *internal_page;
} TBMIterateResult;

/* function prototypes in nodes/tidbitmap.c */

extern TIDBitmap *tbm_create(Size maxbytes, dsa_area *dsa);
extern void tbm_free(TIDBitmap *tbm);

extern void tbm_add_tuples(TIDBitmap *tbm,
						   const ItemPointerData *tids, int ntids,
						   bool recheck);
extern void tbm_add_page(TIDBitmap *tbm, BlockNumber pageno);
extern void tbm_copy_page(TIDBitmap *dest, TBMIterateResult *src);

extern void tbm_union(TIDBitmap *a, const TIDBitmap *b);
extern void tbm_intersect(TIDBitmap *a, const TIDBitmap *b);

extern int	tbm_extract_page_tuple(TBMIterateResult *iteritem,
								   OffsetNumber *offsets,
								   uint32 max_offsets);

extern bool tbm_is_empty(const TIDBitmap *tbm);

extern TBMOrderedIterator *tbm_begin_ordered_iterate(TIDBitmap *tbm);
extern bool tbm_ordered_iterate(TBMOrderedIterator *iterator, TBMIterateResult *tbmres);
extern void tbm_end_ordered_iterate(TBMOrderedIterator **iterator);

extern dsa_pointer tbm_prepare_shared_unordered_iterate(TIDBitmap *tbm);
extern TBMUnorderedIterator *tbm_begin_shared_unordered_iterate(dsa_area *dsa, dsa_pointer dp);
extern bool tbm_shared_unordered_iterate(TBMUnorderedIterator *iterator, TBMIterateResult *tbmres);
extern void tbm_end_shared_unordered_iterate(TBMUnorderedIterator **iterator);

extern int	tbm_calculate_entries(Size maxbytes);

#endif							/* TIDBITMAP_H */
