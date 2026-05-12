#include <string.h>

#include "trace.h"
#include "buffer_frame.h"

void trace_init(Trace *t) {
    if (!t) return;
    t->count  = 0;
    t->active = 0;
}

void trace_start(Trace *t) {
    if (!t) return;
    t->count  = 0;
    t->active = 1;
}

void trace_stop(Trace *t) {
    if (!t) return;
    t->active = 0;
}

void trace_clear(Trace *t) {
    if (!t) return;
    t->active = 0;
    t->count  = 0;
}

void trace_record(Trace *t, long long timestamp, const char *table,
                  int page_id, int hit, int frame_id) {
    if (!t || !t->active) return;
    if (t->count >= MAX_TRACE_EVENTS) {
        t->active = 0;   // buffer full — stop silently
        return;
    }
    TraceEvent *ev = &t->events[t->count++];
    ev->timestamp = timestamp;
    strncpy(ev->table, table, 63);
    ev->table[63] = '\0';
    ev->page_id   = page_id;
    ev->hit       = hit;
    ev->frame_id  = frame_id;
    ev->evicted_frame = -1;
    ev->n_frames = 0;
}

void trace_record_full(Trace *t, long long timestamp, const char *table,
                       int page_id, int hit, int frame_id, int evicted_frame,
                       BufferPool *pool) {
    if (!t || !t->active || !pool) return;
    if (t->count >= MAX_TRACE_EVENTS) {
        t->active = 0;
        return;
    }

    TraceEvent *ev = &t->events[t->count++];
    ev->timestamp = timestamp;
    strncpy(ev->table, table, 63);
    ev->table[63] = '\0';
    ev->page_id = page_id;
    ev->hit = hit;
    ev->frame_id = frame_id;
    ev->evicted_frame = evicted_frame;
    ev->n_frames = pool->num_frames;

    for (int i = 0; i < pool->num_frames; i++) {
        BufferFrame *frame = &pool->frames[i];
        FrameSnapshot *snapshot = &ev->frames[i];

        snapshot->frame_id = i;
        snapshot->state = frame->state;
        snapshot->dirty = frame->dirty;
        snapshot->pin_count = frame->pin_count;
        snapshot->ref_bit = frame->ref_bit;
        snapshot->last_access = frame->last_access;

        if (frame->state == FRAME_FREE) {
            snapshot->table[0] = '\0';
            snapshot->page_id = -1;
            continue;
        }

        strncpy(snapshot->table, frame->table_name, sizeof(snapshot->table) - 1);
        snapshot->table[sizeof(snapshot->table) - 1] = '\0';
        snapshot->page_id = frame->page_id;
    }
}
