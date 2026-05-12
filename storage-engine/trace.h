#ifndef TRACE_H
#define TRACE_H

// Reduced from 65536: each event carries a full pool snapshot (~96 KB per event).
#define MAX_TRACE_EVENTS 512
#define MAX_FRAMES       1024   // must equal BUFFER_POOL_MAX_FRAMES

// Per-frame state captured after each buffer pool access.
typedef struct {
    int        frame_id;
    int        state;        // 0=FREE, 1=OCCUPIED, 2=PINNED
    char       table[64];    // empty string if FREE
    int        page_id;      // -1 if FREE
    int        dirty;
    int        pin_count;
    int        ref_bit;      // Clock only — 0 or 1
    long long  last_access;  // LRU only — monotonic counter
} FrameSnapshot;

typedef struct {
    long long     timestamp;
    char          table[64];
    int           page_id;
    int           hit;           // 1=hit, 0=miss
    int           frame_id;      // frame that was loaded/hit, -1 if eviction failed
    int           evicted_frame; // -1 if no eviction, else frame that was evicted
    int           n_frames;      // number of frames in the pool
    FrameSnapshot frames[MAX_FRAMES]; // full pool snapshot after this access
} TraceEvent;

#include "buffer_frame.h"  // for BufferPool

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

// Append one event (no pool snapshot) when active==1.
void trace_record(Trace *t, long long timestamp, const char *table,
                  int page_id, int hit, int frame_id);

// Append one event with a full pool snapshot when active==1.
void trace_record_full(Trace *t, long long timestamp, const char *table,
                       int page_id, int hit, int frame_id, int evicted_frame,
                       BufferPool *pool);

#endif
