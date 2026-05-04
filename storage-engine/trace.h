#ifndef TRACE_H
#define TRACE_H

#define MAX_TRACE_EVENTS 65536

typedef struct {
    long long timestamp;   // monotonic access counter (bm pool.access_clock)
    char      table[64];   // table name
    int       page_id;     // page accessed
    int       hit;         // 1=hit, 0=miss
    int       frame_id;    // frame assigned (-1 if eviction was required)
} TraceEvent;

typedef struct {
    TraceEvent events[MAX_TRACE_EVENTS];
    int        count;
    int        active;     // 1 if currently recording
} Trace;

// Initialise trace (inactive, count=0). Call once after allocation.
void trace_init(Trace *t);

// Clear any existing events and start recording.
void trace_start(Trace *t);

// Stop recording (preserves collected events).
void trace_stop(Trace *t);

// Stop recording and discard all events.
void trace_clear(Trace *t);

// Append one event when active==1. Silently stops recording if buffer is full.
void trace_record(Trace *t, long long timestamp, const char *table,
                  int page_id, int hit, int frame_id);

#endif
