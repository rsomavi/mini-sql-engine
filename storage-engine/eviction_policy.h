#ifndef EVICTION_POLICY_H
#define EVICTION_POLICY_H

#include "buffer_frame.h"

// ============================================================================
// Abstract eviction policy interface
// ============================================================================
//
// Each policy implements three callbacks:
//   on_pin   — called when a frame is pinned (page accessed)
//   on_unpin — called when a frame is unpinned (page released)
//   evict    — called when the pool is full and a victim must be chosen
//              returns the frame_id to evict, or -1 if no victim available
//
// The void *data pointer holds policy-specific private state.
// The destroy callback frees that private state.

typedef struct {
    void (*on_pin)  (void *data, int frame_id);
    void (*on_unpin)(void *data, int frame_id);
    int  (*evict)   (void *data, BufferPool *pool);
    void (*destroy) (void *data);
    void (*advance) (void *data);   // advance trace position after each access (OPT only)
    void *data;
} EvictionPolicy;

// ============================================================================
// Convenience macros to call through the interface
// ============================================================================

#define POLICY_ON_PIN(p, fid)   do { if ((p) && ((EvictionPolicy*)(p))->on_pin)    ((EvictionPolicy*)(p))->on_pin(((EvictionPolicy*)(p))->data, (fid));   } while(0)
#define POLICY_ON_UNPIN(p, fid) do { if ((p) && ((EvictionPolicy*)(p))->on_unpin)  ((EvictionPolicy*)(p))->on_unpin(((EvictionPolicy*)(p))->data, (fid)); } while(0)
#define POLICY_EVICT(p, pool)   (((p) && ((EvictionPolicy*)(p))->evict) ? ((EvictionPolicy*)(p))->evict(((EvictionPolicy*)(p))->data, (pool)) : -1)
#define POLICY_DESTROY(p)       do { if ((p) && ((EvictionPolicy*)(p))->destroy)   ((EvictionPolicy*)(p))->destroy(((EvictionPolicy*)(p))->data); } while(0)
#define POLICY_ADVANCE(p)       do { if ((p) && ((EvictionPolicy*)(p))->advance)   ((EvictionPolicy*)(p))->advance(((EvictionPolicy*)(p))->data);          } while(0)
// ============================================================================
// NoCache — always evicts the first OCCUPIED frame found
// Baseline: hit rate = 0%, no data structures needed
// ============================================================================

EvictionPolicy *policy_nocache_create(void);

// ============================================================================
// Clock — approximation of LRU using a rotating hand and ref_bit
// Used by PostgreSQL (Clock Sweep). O(n) worst case, O(1) amortized.
// ============================================================================

EvictionPolicy *policy_clock_create(int num_frames);

// ============================================================================
// LRU — evicts the frame with the oldest last_access timestamp
// O(n) per eviction. Optimal for workloads with strong temporal locality.
// ============================================================================

EvictionPolicy *policy_lru_create(void);

// ============================================================================
// OPT — offline simulation of Belady's optimal algorithm
// Requires a pre-recorded access trace. Theoretical upper bound.
// Cannot be used online — only for experimental comparison.
// ============================================================================

// Access record for OPT trace
typedef struct {
    char table_name[MAX_TABLE_NAME_LEN];
    int  page_id;
} OPTAccess;

EvictionPolicy *policy_opt_create(const OPTAccess *trace, int trace_len);

// Advance OPT to the next step in the trace (call after each page access)
void policy_opt_advance(EvictionPolicy *policy);

// Returns current position in the OPT trace (for testing)
int policy_opt_get_pos(EvictionPolicy *policy);

#endif
