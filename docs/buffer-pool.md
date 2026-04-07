# Buffer Pool

The buffer pool is the caching layer between the server handlers and disk. It keeps frequently accessed pages in RAM so that repeated reads do not require disk I/O. It is implemented across four files: `buffer_manager.c`, `buffer_frame.c`, `page_table.c`, and `eviction_policy.c`.

## Structure

```
BufferManager
  ├── BufferPool     — array of frames in RAM
  ├── PageTable      — hash map (table_name, page_id) -> frame_id
  └── EvictionPolicy — pluggable replacement algorithm
```

The `BufferManager` is the public interface. Server handlers only call two functions:

```c
char *bm_fetch_page(bm, table_name, page_id);      // pin page into RAM
int   bm_unpin_page(bm, table_name, page_id, dirty); // release page
```

Everything else — loading from disk, evicting victims, updating the page table — is handled internally.

---

## BufferPool — `buffer_frame.c`

The pool is a fixed-size array of `BufferFrame` structs allocated at initialization. The maximum size is 1024 frames (`BUFFER_POOL_MAX_FRAMES`). Each frame is 4096 bytes of page data plus metadata.

### BufferFrame structure

```c
typedef struct {
    char       table_name[64];  // which table this page belongs to
    int        page_id;         // page number within that table
    FrameState state;           // FREE, OCCUPIED, or PINNED
    int        pin_count;       // number of active users
    int        dirty;           // 1 if modified since last write to disk
    int        ref_bit;         // Clock: was this frame accessed recently?
    long long  last_access;     // LRU: timestamp of last access
    char       data[4096];      // the actual page content
} BufferFrame;
```

### Frame states

```
FREE      — empty, available for use
OCCUPIED  — holds a page, no active users, eligible for eviction
PINNED    — holds a page, has active users, cannot be evicted
```

A frame transitions from FREE to OCCUPIED when a page is loaded. It becomes PINNED when `pin_count > 0` (i.e. a handler is actively using the page). When `bm_unpin_page` is called and `pin_count` reaches 0, it returns to OCCUPIED.

### Core operations

`bp_load_frame` copies page data into a frame and sets `state = OCCUPIED`, `pin_count = 1`, `dirty = 0`, `ref_bit = 1`, and updates `last_access`. Increments `pool->misses`.

`bp_pin_frame` increments `pin_count`, sets `state = PINNED`, `ref_bit = 1`, updates `last_access`. Increments `pool->hits`.

`bp_unpin_frame` decrements `pin_count`. If `dirty == 1`, sets `frame->dirty = 1`. If `pin_count` reaches 0, sets `state = OCCUPIED`.

`bp_evict_frame` clears the frame. If `frame->dirty`, calls `write_page` first to flush to disk. Then zeroes the frame and sets `state = FREE`. Increments `pool->evictions`.

### Metrics

The pool tracks three counters: `hits`, `misses`, `evictions`. These are exposed through the buffer manager API and sent to the client in every response.

---

## PageTable — `page_table.c`

The page table is a hash map from `(table_name, page_id)` to `frame_id`. It enables O(1) lookup to check whether a page is already in the pool.

### Implementation

2048 buckets (`PT_NUM_BUCKETS`), each a linked list of `PageTableEntry` nodes. The bucket is selected with a combined hash of `table_name` (DJB2) and `page_id` (Knuth multiplicative hash):

```c
hash = DJB2(table_name) ^ (page_id * 2654435761u)
bucket = hash & (PT_NUM_BUCKETS - 1)   // fast modulo, power of 2
```

Collisions are resolved by chaining. Each entry stores `table_name`, `page_id`, `frame_id`, and a `next` pointer.

### API

```c
int pt_insert(pt, table_name, page_id, frame_id);  // insert or update
int pt_lookup(pt, table_name, page_id);            // returns frame_id or -1
int pt_remove(pt, table_name, page_id);            // removes entry
int pt_clear(pt);                                  // frees all entries
int pt_size(pt);                                   // number of entries
```

`pt_insert` checks for an existing entry first — if found, it updates `frame_id` in place instead of allocating a new node.

---

## Eviction Policies — `eviction_policy.c`

The eviction policy is defined as an abstract interface with four callbacks:

```c
typedef struct {
    void (*on_pin)  (void *data, int frame_id);
    void (*on_unpin)(void *data, int frame_id);
    int  (*evict)   (void *data, BufferPool *pool);
    void (*destroy) (void *data);
    void *data;   // policy-specific private state
} EvictionPolicy;
```

The buffer manager calls through macros:

```c
POLICY_ON_PIN(policy, frame_id)
POLICY_ON_UNPIN(policy, frame_id)
POLICY_EVICT(policy, pool)      // returns frame_id of victim
POLICY_DESTROY(policy)
```

This design means swapping policies requires no changes to the buffer manager — only a different `policy_*_create()` call at server startup.

---

### NoCache

**Strategy:** evicts the first OCCUPIED frame found (index 0, 1, 2...).

**Private state:** none.

**Hit rate:** always 0% by definition — every eviction discards a page regardless of future use.

**Use:** baseline for experiments. Represents the worst case.

```c
static int nocache_evict(void *data, BufferPool *pool) {
    for (int i = 0; i < pool->num_frames; i++)
        if (pool->frames[i].state == FRAME_OCCUPIED)
            return i;
    return -1;
}
```

---

### Clock

**Strategy:** rotating hand sweeps frames in circular order. Frames with `ref_bit = 1` get a second chance (bit cleared, hand advances). Frames with `ref_bit = 0` are evicted.

**Private state:** `ClockData { int hand; int num_frames; }`.

**Worst case:** two full sweeps (first clears all ref_bits, second finds victim).

**Use:** approximation of LRU with O(1) amortized cost. Used by PostgreSQL (Clock Sweep).

```
hand →  [ref=1]  →  clear, advance
        [ref=0]  →  evict this frame
        [ref=1]  →  clear, advance
        ...
```

The `ref_bit` is set to 1 by `bp_load_frame` and `bp_pin_frame` — the policy's `on_pin` callback does nothing because the bit is already handled at the pool level.

---

### LRU

**Strategy:** evicts the frame with the smallest `last_access` timestamp (least recently used).

**Private state:** none — uses `last_access` stored in each `BufferFrame`.

**Cost:** O(n) per eviction — scans all frames to find the minimum timestamp.

**Stack property:** LRU guarantees that with more frames, the hit rate never decreases. This is a theoretical property that the experimental framework will verify empirically.

```c
static int lru_evict(void *data, BufferPool *pool) {
    int victim = -1;
    long long oldest = LLONG_MAX;
    for (int i = 0; i < pool->num_frames; i++) {
        if (pool->frames[i].state != FRAME_OCCUPIED) continue;
        if (pool->frames[i].last_access < oldest) {
            oldest = pool->frames[i].last_access;
            victim = i;
        }
    }
    return victim;
}
```

The `last_access` counter is a monotonically increasing integer (`pool->access_clock`) incremented on every `bp_load_frame` and `bp_pin_frame` call.

---

### OPT (Belady)

**Strategy:** evicts the frame whose next access is furthest in the future. If a page is never accessed again, it is evicted immediately.

**Private state:** `OPTData { const OPTAccess *trace; int trace_len; int pos; }`.

**Requirement:** requires a pre-recorded access trace. Cannot be used online — only for offline simulation and experimental comparison.

**Theoretical significance:** OPT is the optimal offline algorithm. No online algorithm can achieve a higher hit rate on the same workload. It serves as the theoretical upper bound for all other policies.

```c
// For each frame in the pool, find its next access after position pos.
// Evict the frame whose next access is farthest (or never).
int next_use = INT_MAX;  // never used again
for (int t = od->pos; t < od->trace_len; t++) {
    if (trace[t].page_id == pid && strcmp(trace[t].table_name, tname) == 0) {
        next_use = t;
        break;
    }
}
```

`policy_opt_advance()` must be called after each page access to advance `pos` in the trace. `policy_opt_get_pos()` returns the current position for testing.

The trace is owned by the caller — `opt_destroy` frees `OPTData` but not the trace array.

---

## bm_fetch_page — full flow

When a handler calls `bm_fetch_page(bm, "empleados", 1)`:

```
1. pt_lookup(&bm->pt, "empleados", 1)
   HIT  → bp_pin_frame, POLICY_ON_PIN, return frame data pointer
   MISS → continue

2. bp_find_free_frame(&bm->pool)
   found  → go to step 4
   -1     → need to evict

3. POLICY_EVICT(bm->policy, &bm->pool)
   returns victim frame_id
   pt_remove(victim's table_name, victim's page_id)
   bp_evict_frame (write-back if dirty)

4. load_page(data_dir, "empleados", 1, page_buf)
   bp_load_frame(pool, frame_id, "empleados", 1, page_buf)
   pt_insert(&bm->pt, "empleados", 1, frame_id)
   POLICY_ON_PIN(bm->policy, frame_id)

5. return pool->frames[frame_id].data
```

---

## Dirty page flush

Pages are never written to disk immediately on modification. The write-back happens in two cases:

**On eviction** — `bp_evict_frame` calls `write_page` if `frame->dirty == 1` before clearing the frame.

**On shutdown** — `bm_destroy` iterates all frames and calls `write_page` for every dirty frame before freeing resources. This is triggered by the SIGINT/SIGTERM signal handler in `server.c`.

---

## Metrics exposed by the server

Every response includes a METRICS line:

```
METRICS hits=7 misses=2 evictions=0 hit_rate=0.778
```

These are reset with `bm_reset_metrics` and retrieved with `bm_get_hits`, `bm_get_misses`, `bm_get_evictions`, `bm_get_hit_rate`.

---

## Experimental framework (planned)

The four policies are the foundation of the research component of this project. The planned experimental framework will:

- Execute the same SQL workload against all four policies with identical pool sizes
- Record hits, misses, evictions per policy
- Compute distance to OPT: `(OPT_hits - policy_hits) / OPT_hits * 100`
- Vary pool size (4, 8, 16, 32, 64 frames) to study the effect on each policy
- Use three workload distributions: sequential, uniform random, and Zipf

The research questions the experiments aim to answer:

- Does the Belady anomaly manifest under real SQL workloads?
- Does the stack property of LRU hold empirically, and does Clock ever violate it?
- At what pool size does LRU become indistinguishable from OPT under Zipf workloads?
- Under what access distributions does Clock approach LRU performance?
