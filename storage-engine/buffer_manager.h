#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include "buffer_frame.h"
#include "page_table.h"
#include "eviction_policy.h"
#include "trace.h"

// ============================================================================
// BufferManager — public API that unites the three buffer pool components:
//   - BufferPool     (Part 1): frames in RAM
//   - PageTable      (Part 2): O(1) hash map (table, page_id) -> frame_id
//   - EvictionPolicy (Part 3): pluggable replacement policy
//
// Callers only need two functions:
//   bm_fetch_page  — get a pointer to a page in RAM (loads from disk if needed)
//   bm_unpin_page  — release a page after use
// ============================================================================

typedef struct {
    BufferPool      pool;
    PageTable       pt;
    EvictionPolicy *policy;
    char            data_dir[256];
    Trace          *trace;   // borrowed pointer — NULL when not recording
} BufferManager;

// ============================================================================
// Lifecycle
// ============================================================================

// Initializes the buffer manager.
// num_frames: number of frames in the pool (1 to BUFFER_POOL_MAX_FRAMES)
// data_dir:   directory where .db files are stored
// policy:     pluggable eviction policy (takes ownership — bm_destroy frees it)
// Returns 0 on success, -1 on error.
int bm_init(BufferManager *bm, int num_frames,
            const char *data_dir, EvictionPolicy *policy);

// Destroys the buffer manager: flushes all dirty frames to disk,
// frees the eviction policy, and clears the page table.
// Returns 0 on success, -1 on error.
int bm_destroy(BufferManager *bm);

// ============================================================================
// Core API
// ============================================================================

// Fetches a page into the buffer pool and returns a pointer to its data.
// If the page is already in the pool (HIT): pins it and returns the pointer.
// If the page is not in the pool (MISS): loads it from disk, evicting a
// victim frame if necessary, and returns the pointer.
// The caller MUST call bm_unpin_page when done with the page.
// Returns a pointer to the 4096-byte page data, or NULL on error.
char *bm_fetch_page(BufferManager *bm, const char *table_name, int page_id);

// Releases a previously fetched page.
// dirty: 1 if the page was modified and must be written to disk on eviction.
// Returns 0 on success, -1 on error.
int bm_unpin_page(BufferManager *bm, const char *table_name,
                  int page_id, int dirty);

// ============================================================================
// Metrics
// ============================================================================

// Returns the number of cache hits since last reset.
long long bm_get_hits(BufferManager *bm);

// Returns the number of cache misses since last reset.
long long bm_get_misses(BufferManager *bm);

// Returns the number of evictions since last reset.
long long bm_get_evictions(BufferManager *bm);

// Returns the hit rate as a value between 0.0 and 1.0.
// Returns 0.0 if no accesses have been made yet.
double bm_get_hit_rate(BufferManager *bm);

// Resets all metric counters to 0.
void bm_reset_metrics(BufferManager *bm);

// ============================================================================
// Debug
// ============================================================================

void bm_print_state(BufferManager *bm);
void bm_print_metrics(BufferManager *bm);

#endif
