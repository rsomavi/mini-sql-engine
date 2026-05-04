#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "eviction_policy.h"

// ============================================================================
// NoCache
// ============================================================================

static void nocache_on_pin(void *data, int frame_id) {
    (void)data; (void)frame_id;  // nothing to do
}

static void nocache_on_unpin(void *data, int frame_id) {
    (void)data; (void)frame_id;  // nothing to do
}

static int nocache_evict(void *data, BufferPool *pool) {
    (void)data;
    if (!pool) return -1;
    // Evict the first OCCUPIED frame found
    for (int i = 0; i < pool->num_frames; i++) {
        if (pool->frames[i].state == FRAME_OCCUPIED)
            return i;
    }
    return -1;  // all frames pinned
}

static void nocache_destroy(void *data) {
    (void)data;  // no private state to free
}

EvictionPolicy *policy_nocache_create(void) {
    EvictionPolicy *p = malloc(sizeof(EvictionPolicy));
    if (!p) return NULL;
    p->on_pin   = nocache_on_pin;
    p->on_unpin = nocache_on_unpin;
    p->evict    = nocache_evict;
    p->destroy  = nocache_destroy;
    p->advance  = NULL;
    p->data     = NULL;
    return p;
}

// ============================================================================
// Clock
// ============================================================================

typedef struct {
    int hand;        // current position of the clock hand
    int num_frames;  // total frames in the pool
} ClockData;

static void clock_on_pin(void *data, int frame_id) {
    (void)data; (void)frame_id;  // ref_bit set by bp_pin_frame / bp_load_frame
}

static void clock_on_unpin(void *data, int frame_id) {
    (void)data; (void)frame_id;  // nothing to do on unpin
}

static int clock_evict(void *data, BufferPool *pool) {
    if (!data || !pool) return -1;
    ClockData *cd = (ClockData *)data;

    // Two full sweeps maximum — first clears ref_bits, second finds victim
    int checked = 0;
    int limit   = cd->num_frames * 2;

    while (checked < limit) {
        int i = cd->hand % cd->num_frames;
        cd->hand = (cd->hand + 1) % cd->num_frames;
        checked++;

        BufferFrame *f = &pool->frames[i];

        if (f->state != FRAME_OCCUPIED) continue;

        if (f->ref_bit) {
            f->ref_bit = 0;   // give it a second chance
        } else {
            return i;         // victim found
        }
    }
    return -1;  // all frames pinned or all had ref_bit set twice
}

static void clock_destroy(void *data) {
    free(data);
}

EvictionPolicy *policy_clock_create(int num_frames) {
    if (num_frames <= 0) return NULL;
    EvictionPolicy *p = malloc(sizeof(EvictionPolicy));
    if (!p) return NULL;

    ClockData *cd = malloc(sizeof(ClockData));
    if (!cd) { free(p); return NULL; }

    cd->hand       = 0;
    cd->num_frames = num_frames;

    p->on_pin   = clock_on_pin;
    p->on_unpin = clock_on_unpin;
    p->evict    = clock_evict;
    p->destroy  = clock_destroy;
    p->advance  = NULL;
    p->data     = cd;
    return p;
}

// ============================================================================
// LRU
// ============================================================================

static void lru_on_pin(void *data, int frame_id) {
    (void)data; (void)frame_id;  // last_access updated by bp_pin_frame
}

static void lru_on_unpin(void *data, int frame_id) {
    (void)data; (void)frame_id;  // nothing to do on unpin
}

static int lru_evict(void *data, BufferPool *pool) {
    (void)data;
    if (!pool) return -1;

    int       victim     = -1;
    long long oldest     = LLONG_MAX;

    for (int i = 0; i < pool->num_frames; i++) {
        if (pool->frames[i].state != FRAME_OCCUPIED) continue;
        if (pool->frames[i].last_access < oldest) {
            oldest = pool->frames[i].last_access;
            victim = i;
        }
    }
    return victim;
}

static void lru_destroy(void *data) {
    (void)data;  // no private state to free
}

EvictionPolicy *policy_lru_create(void) {
    EvictionPolicy *p = malloc(sizeof(EvictionPolicy));
    if (!p) return NULL;
    p->on_pin   = lru_on_pin;
    p->on_unpin = lru_on_unpin;
    p->evict    = lru_evict;
    p->destroy  = lru_destroy;
    p->advance  = NULL;
    p->data     = NULL;
    return p;
}

// ============================================================================
// OPT — offline Belady simulation
// ============================================================================
//
// How it works:
// 1. A trace of all page accesses is recorded beforehand.
// 2. At each eviction decision, OPT looks ahead in the trace to find which
//    currently-loaded page will be used furthest in the future (or never).
//    That page is the victim — evicting it minimizes future misses.
// 3. policy_opt_advance() must be called after each page access to move
//    the current position forward in the trace.

typedef struct {
    const OPTAccess *trace;     // full access trace (not owned)
    int              trace_len;
    int              pos;       // current position in trace
} OPTData;

static void opt_on_pin(void *data, int frame_id) {
    (void)data; (void)frame_id;
}

static void opt_on_unpin(void *data, int frame_id) {
    (void)data; (void)frame_id;
}

static int opt_evict(void *data, BufferPool *pool) {
    if (!data || !pool) return -1;
    OPTData *od = (OPTData *)data;

    int victim      = -1;
    int farthest    = -1;  // farthest next-use position found so far

    for (int i = 0; i < pool->num_frames; i++) {
        if (pool->frames[i].state != FRAME_OCCUPIED) continue;

        const char *tname = pool->frames[i].table_name;
        int         pid   = pool->frames[i].page_id;

        // Find the next access to this page after current position
        int next_use = INT_MAX;  // never used again = evict first
        for (int t = od->pos; t < od->trace_len; t++) {
            if (od->trace[t].page_id == pid &&
                strcmp(od->trace[t].table_name, tname) == 0) {
                next_use = t;
                break;
            }
        }

        if (next_use > farthest) {
            farthest = next_use;
            victim   = i;
        }
    }
    return victim;
}

static void opt_advance_cb(void *data) {
    if (!data) return;
    OPTData *od = (OPTData *)data;
    if (od->pos < od->trace_len)
        od->pos++;
}

static void opt_destroy(void *data) {
    free(data);  // free OPTData but NOT the trace (caller owns it)
}

EvictionPolicy *policy_opt_create(const OPTAccess *trace, int trace_len) {
    if (!trace || trace_len <= 0) return NULL;

    EvictionPolicy *p = malloc(sizeof(EvictionPolicy));
    if (!p) return NULL;

    OPTData *od = malloc(sizeof(OPTData));
    if (!od) { free(p); return NULL; }

    od->trace     = trace;
    od->trace_len = trace_len;
    od->pos       = 0;

    p->on_pin   = opt_on_pin;
    p->on_unpin = opt_on_unpin;
    p->evict    = opt_evict;
    p->destroy  = opt_destroy;
    p->advance  = opt_advance_cb;
    p->data     = od;
    return p;
}

void policy_opt_advance(EvictionPolicy *policy) {
    if (!policy || !policy->data) return;
    OPTData *od = (OPTData *)policy->data;
    if (od->pos < od->trace_len)
        od->pos++;
}

int policy_opt_get_pos(EvictionPolicy *policy) {
    if (!policy || !policy->data) return -1;
    return ((OPTData *)policy->data)->pos;
}
