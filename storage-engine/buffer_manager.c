#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer_manager.h"

// ============================================================================
// Lifecycle
// ============================================================================

int bm_init(BufferManager *bm, int num_frames,
            const char *data_dir, EvictionPolicy *policy) {
    if (!bm || !data_dir || !policy) return -1;

    if (bp_init(&bm->pool, num_frames, data_dir) != 0) return -1;
    if (pt_init(&bm->pt) != 0) return -1;

    bm->policy = policy;
    strncpy(bm->data_dir, data_dir, 255);
    bm->data_dir[255] = '\0';
    bm->trace  = NULL;

    return 0;
}

int bm_destroy(BufferManager *bm) {
    if (!bm) return -1;

    // Flush all dirty frames to disk before destroying
    for (int i = 0; i < bm->pool.num_frames; i++) {
        BufferFrame *f = &bm->pool.frames[i];
        if (f->state != FRAME_FREE && f->dirty)
            write_page(bm->data_dir, f->table_name, f->page_id, f->data);
    }

    // Free eviction policy
    if (bm->policy) {
        POLICY_DESTROY(bm->policy);
        free(bm->policy);
        bm->policy = NULL;
    }

    // Clear page table (frees all allocated entries)
    pt_clear(&bm->pt);

    return 0;
}

// ============================================================================
// Internal: find or load a frame for (table_name, page_id)
// ============================================================================

static int bm_get_frame(BufferManager *bm,
                        const char *table_name, int page_id) {
    int evicted_frame = -1;

    // Step 1: check page table — O(1) lookup
    int frame_id = pt_lookup(&bm->pt, table_name, page_id);

    if (frame_id >= 0) {
        // HIT: page already in pool
        bp_pin_frame(&bm->pool, frame_id);
        POLICY_ON_PIN(bm->policy, frame_id);
        POLICY_ADVANCE(bm->policy);
        trace_record_full(bm->trace, bm->pool.access_clock,
                          table_name, page_id, 1, frame_id, -1,
                          &bm->pool);
        return frame_id;
    }

    // MISS: need to load from disk

    // Step 2: find a free frame
    frame_id = bp_find_free_frame(&bm->pool);
    int evicted = (frame_id < 0);

    if (evicted) {
        // No free frame — need to evict
        frame_id = POLICY_EVICT(bm->policy, &bm->pool);
        if (frame_id < 0) return -1;  // all frames pinned
        evicted_frame = frame_id;

        // Remove evicted page from page table
        pt_remove(&bm->pt,
                  bm->pool.frames[frame_id].table_name,
                  bm->pool.frames[frame_id].page_id);

        // bp_evict_frame handles write-back if dirty
        bp_evict_frame(&bm->pool, frame_id);
    }

    // Step 3: load page from disk into the frame
    char page_buf[PAGE_SIZE];
    if (load_page(bm->data_dir, table_name, page_id, page_buf) < 0)
        return -1;

    bp_load_frame(&bm->pool, frame_id, table_name, page_id, page_buf);

    // Step 4: register in page table
    pt_insert(&bm->pt, table_name, page_id, frame_id);

    POLICY_ON_PIN(bm->policy, frame_id);
    POLICY_ADVANCE(bm->policy);
    trace_record_full(bm->trace, bm->pool.access_clock,
                      table_name, page_id, 0, frame_id, evicted_frame,
                      &bm->pool);
    return frame_id;
}

// ============================================================================
// Core API
// ============================================================================

char *bm_fetch_page(BufferManager *bm,
                    const char *table_name, int page_id) {
    if (!bm || !table_name) return NULL;

    int frame_id = bm_get_frame(bm, table_name, page_id);
    if (frame_id < 0) return NULL;

    return bm->pool.frames[frame_id].data;
}

int bm_unpin_page(BufferManager *bm, const char *table_name,
                  int page_id, int dirty) {
    if (!bm || !table_name) return -1;

    int frame_id = pt_lookup(&bm->pt, table_name, page_id);
    if (frame_id < 0) return -1;

    POLICY_ON_UNPIN(bm->policy, frame_id);
    return bp_unpin_frame(&bm->pool, frame_id, dirty);
}

// ============================================================================
// Metrics
// ============================================================================

long long bm_get_hits(BufferManager *bm) {
    if (!bm) return 0;
    return bm->pool.hits;
}

long long bm_get_misses(BufferManager *bm) {
    if (!bm) return 0;
    return bm->pool.misses;
}

long long bm_get_evictions(BufferManager *bm) {
    if (!bm) return 0;
    return bm->pool.evictions;
}

double bm_get_hit_rate(BufferManager *bm) {
    if (!bm) return 0.0;
    long long total = bm->pool.hits + bm->pool.misses;
    if (total == 0) return 0.0;
    return (double)bm->pool.hits / (double)total;
}

void bm_reset_metrics(BufferManager *bm) {
    if (!bm) return;
    bp_reset_metrics(&bm->pool);
}

// ============================================================================
// Debug
// ============================================================================

void bm_print_state(BufferManager *bm) {
    if (!bm) return;
    printf("=== Buffer Manager State ===\n");
    printf("  data_dir: %s\n", bm->data_dir);
    printf("  page table entries: %d\n", pt_size(&bm->pt));
    bp_print_state(&bm->pool);
}

void bm_print_metrics(BufferManager *bm) {
    if (!bm) return;
    printf("=== Buffer Manager Metrics ===\n");
    printf("  hits:      %lld\n", bm_get_hits(bm));
    printf("  misses:    %lld\n", bm_get_misses(bm));
    printf("  evictions: %lld\n", bm_get_evictions(bm));
    printf("  hit rate:  %.1f%%\n", bm_get_hit_rate(bm) * 100.0);
}
