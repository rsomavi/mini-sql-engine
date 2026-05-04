#include <string.h>

#include "trace.h"

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
}
