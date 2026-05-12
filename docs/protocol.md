# MINIDBMS-RESP Protocol Specification

## Overview

MINIDBMS-RESP is a text-framed binary-payload protocol for communication
between the Python SQL executor and the C storage engine server.

It is inspired by Redis RESP (REdis Serialization Protocol) but adapted
for database storage operations. The Python layer is responsible for SQL
parsing, planning, and optimization. The C server is responsible for
storage — buffer pool management, heap I/O, and schema.

**What travels over the wire is not SQL.**
The Python executor parses SQL into an AST, plans the query, and translates
the plan into storage operations. Those operations are what the C server receives.

## Design Decisions

- **Row data is binary** — more efficient than text, avoids encoding ambiguity
- **Single connection per session** — concurrency added in a later version
- **Metrics included in every response** — the monitor always has fresh data
- **One operation per request** — no pipelining in v1

## Transport

- TCP, default port **5433** (5432 is PostgreSQL, we avoid conflicts)
- One request → one response per round trip
- Connection is persistent for the duration of a session
- Server sends no data until it receives a request

## Message Format

### Request

Every request starts with a header line in ASCII, followed by optional
binary payload for operations that send row data.

```
<OPERATION> [ARG1] [ARG2] ... [ARGn]\n
[PAYLOAD_SIZE]\n                        ← only if operation has payload
[binary payload bytes]                  ← only if operation has payload
```

Lines are terminated with `\n` (LF, 0x0A). The binary payload does NOT
have a line terminator — its length is determined by PAYLOAD_SIZE.

### Response

Every response has the following structure:

```
OK|ERR\n                                ← status line
[DATA_LINES]                            ← zero or more data lines
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n                                   ← end of response marker
```

For responses that return rows, each row is preceded by its byte size.
The client reads rows until it encounters the `METRICS` line:

```
OK\n
<row_size_bytes>\n
<binary row data>
<row_size_bytes>\n
<binary row data>
...
METRICS hits=3 misses=1 evictions=0 hit_rate=0.750\n
END\n
```

The `ROWS <count>` header was removed in v1.4. The client determines
the row count by reading until `METRICS`. This eliminates the double
buffer pool scan that previously generated artificial cache hits.

Error responses:

```
ERR <error_code> <human readable message>\n
METRICS hits=0 misses=0 evictions=0 hit_rate=0.000\n
END\n
```

### Error Codes

| Code | Meaning |
|------|---------|
| `TABLE_NOT_FOUND` | Table does not exist |
| `TABLE_EXISTS` | Table already exists |
| `SCHEMA_MISMATCH` | Row data does not match table schema |
| `POOL_FULL` | All frames pinned, cannot evict |
| `INVALID_OP` | Unknown operation |
| `INVALID_ARGS` | Wrong number or type of arguments |
| `IO_ERROR` | Disk read/write failed |

---

## Operations

---

### PING

Verify that the server is alive and responding.

**Request:**
```
PING\n
```

**Response:**
```
OK\n
PONG\n
METRICS hits=0 misses=0 evictions=0 hit_rate=0.000\n
END\n
```

**Notes:**
- PING never fails unless the connection is broken.
- Metrics reflect the current state of the buffer pool, not this operation.

**Example:**
```
→ PING\n
← OK\n
  PONG\n
  METRICS hits=0 misses=0 evictions=0 hit_rate=0.000\n
  END\n
```

---

### SCAN

Read all rows from a table, page by page through the buffer pool.

**Request:**
```
SCAN <table_name>\n
```

**Response:**
```
OK\n
<row_id> <row_size>\n
<binary row data>
<row_id> <row_size>\n
<binary row data>
...
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n
```

**Arguments:**
- `table_name` — name of the table to scan (max 63 chars, ASCII)

**Notes:**
- Rows are returned in heap order (insertion order, not sorted).
- Deleted rows (logical deletes) are skipped automatically.
- Each row is a binary blob serialized by the schema layer.
  The Python layer is responsible for deserializing using the table schema.
- If the table is empty, the response goes directly from `OK\n` to `METRICS`.
- The buffer pool is used — each page access updates hit/miss counters.
- There is no `ROWS <count>` header. The client reads rows until it
  encounters the `METRICS` line. This ensures a single buffer pool pass
  and eliminates artificial cache hits from a preliminary count scan.
- Each row line contains `<row_id> <size>` — the RowID allows the client
  to perform UPDATE and DELETE operations without a second scan.

**Design decision (v1.4):**
The original protocol (v1.0–v1.3) sent `ROWS <count>` before the rows,
requiring two full passes over the buffer pool — one to count, one to send.
This generated artificial hits on the second pass, corrupting cache metrics.
In v1.4 the count header is removed. The server makes a single pass,
copies rows to a temporary memory buffer, then sends them. The client
counts rows as it receives them.

**Errors:**
- `TABLE_NOT_FOUND` if the table does not exist.

**Example:**
```
→ SCAN users\n
← OK\n
  65536 12\n
  <12 bytes: row 1>
  65537 12\n
  <12 bytes: row 2>
  METRICS hits=0 misses=2 evictions=0 hit_rate=0.000\n
  END\n
```

---

### SCHEMA

Get the column definitions of a table.

**Request:**
```
SCHEMA <table_name>\n
```

**Response:**
```
OK\n
COLUMNS <count>\n
<col_name>:<type>:<max_size>:<nullable>\n
...
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n
```

**Arguments:**
- `table_name` — name of the table (max 63 chars, ASCII)

**Column line format:** `name:type:max_size:nullable:pk`
| Field | Description |
|-------|-------------|
| `name` | column name |
| `type` | INT, FLOAT, BOOL, or VARCHAR |
| `max_size` | 0 for fixed types, max bytes for VARCHAR |
| `nullable` | 1 if nullable, 0 if NOT NULL |
| `pk` | 1 if primary key, 0 otherwise |

**Notes:**
- SCHEMA reads page 0 of the table file where the schema is stored.
- The ServerStorage Python client caches schemas — SCHEMA is called
  only once per table per session, then served from the local cache.
- max_size is 0 for INT, FLOAT, and BOOL.

**Errors:**
- `TABLE_NOT_FOUND` if the table does not exist.

**Example:**
```
→ SCHEMA users\n
← OK\n
  COLUMNS 4\n
  id:INT:0:1:1\n
  name:VARCHAR:64:0:0\n
  age:INT:0:0:0\n
  city:VARCHAR:64:0:0\n
  METRICS hits=0 misses=1 evictions=0 hit_rate=0.000\n
  END\n
```

---

### GET

Fetch a single row by its RowID.

**Request:**
```
GET <table_name> <row_id>\n
```

**Response (found):**
```
OK\n
ROWS 1\n
<row_size>\n
<binary row data>
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n
```

**Response (not found):**
```
OK\n
ROWS 0\n
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n
```

**Arguments:**
- `table_name` — name of the table
- `row_id` — integer RowID encoded as (page_id << 16) | slot_id

**Notes:**
- GET decodes the RowID to find the exact page and slot — O(1) lookup.
- A ROWS 0 response is not an error — it means the row was not found.

**Errors:**
- `TABLE_NOT_FOUND` if the table does not exist.
- `INVALID_ARGS` if row_id is not a valid integer.

**Example:**
```
→ GET users 65537\n
← OK\n
  ROWS 1\n
  12\n
  <12 bytes: row data>
  METRICS hits=1 misses=0 evictions=0 hit_rate=1.000\n
  END\n
```

---

### CREATE

Create a new table with a given schema.

**Request:**
```
CREATE <table_name> <col1:type1> <col2:type2> ...\n
```

**Response:**
```
OK\n
CREATED <table_name>\n
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n
```

**Arguments:**
- `table_name` — name of the new table (max 63 chars)
- `colN:typeN` — column definitions, space-separated

**Supported types:**
| Type | Description |
|------|-------------|
| `INT` | 32-bit signed integer |
| `FLOAT` | 64-bit float |
| `BOOL` | boolean (1 byte) |
| `VARCHAR(n)` | variable-length string, max n bytes |

**Errors:**
- `TABLE_EXISTS` if a table with that name already exists.
- `INVALID_ARGS` if any column definition is malformed.

**Example:**
```
→ CREATE users id:INT name:VARCHAR(64) age:INT\n
← OK\n
  CREATED users\n
  METRICS hits=0 misses=0 evictions=0 hit_rate=0.000\n
  END\n
```

---

### INSERT

Insert a serialized row into a table.

**Request:**
```
INSERT <table_name> <payload_size>\n
<payload_size>\n
<binary serialized row>
```

**Response:**
```
OK\n
ROW_ID <row_id>\n
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n
```

**Arguments:**
- `table_name` — name of the table
- `payload_size` — size of the binary row in bytes

**Notes:**
- The binary payload is a row serialized by the Python schema layer
  using the same format as `row_serialize` in schema.c.
- The server validates payload size against the table schema.
- On success, the assigned RowID is returned so Python can track it.

**Errors:**
- `TABLE_NOT_FOUND` if the table does not exist.
- `SCHEMA_MISMATCH` if payload size does not match expected row size.
- `IO_ERROR` if the page cannot be written.

**Example:**
```
→ INSERT users 12\n
  12\n
  <12 bytes: serialized row>
← OK\n
  ROW_ID 65537\n
  METRICS hits=0 misses=1 evictions=0 hit_rate=0.000\n
  END\n
```

---

### DELETE

Mark a row as deleted by its RowID (logical delete).

**Request:**
```
DELETE <table_name> <row_id>\n
```

**Response:**
```
OK\n
DELETED 1\n
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n
```

**Arguments:**
- `table_name` — name of the table
- `row_id` — RowID of the row to delete

**Notes:**
- Delete is logical — the slot is marked as deleted in the slotted page.
  Physical space is not reclaimed immediately (no compaction in v1).
- DELETED 0 means the row was not found (not an error).

**Errors:**
- `TABLE_NOT_FOUND` if the table does not exist.
- `INVALID_ARGS` if row_id is not a valid integer.

**Example:**
```
→ DELETE users 65537\n
← OK\n
  DELETED 1\n
  METRICS hits=1 misses=0 evictions=0 hit_rate=1.000\n
  END\n
```

---

### UPDATE

Replace a row's data in place by RowID.

**Request:**
```
UPDATE <table_name> <row_id> <payload_size>\n
<payload_size>\n
<binary serialized row>
```

**Response:**
```
OK\n
UPDATED 1\n
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n
```

**Arguments:**
- `table_name` — name of the table
- `row_id` — RowID of the row to update
- `payload_size` — size of the new binary row in bytes

**Notes:**
- If the new payload has the same size as the existing row, the update
  is performed in-place — bytes overwritten directly in the buffer pool page.
- If the new payload has a different size, a HOT update is performed:
  the new row is inserted in a free slot (same page if space allows,
  otherwise a new page), and the original slot is marked `SLOT_REDIRECT`
  pointing to the new location. The original RowID remains valid.
- SLOT_REDIRECT chains are followed transparently by SCAN and UPDATE.
- UPDATED 0 means the row was not found or the slot was already deleted.
- The page is marked dirty in the buffer pool after any update.

**Design decision:**
HOT updates follow the same principle as PostgreSQL Heap Only Tuples —
the original RowID is preserved by leaving a forwarding pointer in the
original slot. This avoids invalidating any reference to the row held
by the client, at the cost of an extra indirection on subsequent reads.

**Errors:**
- `TABLE_NOT_FOUND` if the table does not exist.
- `SCHEMA_MISMATCH` if payload size does not match.
- `INVALID_ARGS` if row_id is not valid.

---

### POLICY

Change the buffer pool eviction policy without restarting the server.

**Request:**
```
POLICY <policy_name>\n
```

**Response:**
```
OK\n
POLICY_SET <policy_name>\n
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n
```

**Supported policies:**
| Name | Description |
|------|-------------|
| `nocache` | Always evicts first OCCUPIED frame (baseline) |
| `clock` | Clock sweep with ref_bit (PostgreSQL-style) |
| `lru` | Least Recently Used |

**Notes:**
- Changing policy resets the eviction strategy immediately.
- In-flight pins are not affected — pinned frames stay pinned.
- Metrics are NOT reset when the policy changes.
  Use RESET_METRICS explicitly if needed.
- OPT policy is not available via POLICY command (requires offline trace).

**Errors:**
- `INVALID_ARGS` if policy_name is not one of the supported values.

**Example:**
```
→ POLICY lru\n
← OK\n
  POLICY_SET lru\n
  METRICS hits=18 misses=6 evictions=2 hit_rate=0.750\n
  END\n
```

---

### METRICS

Request current buffer pool metrics explicitly.

**Request:**
```
METRICS\n
```

**Response:**
```
OK\n
hits=<n>\n
misses=<n>\n
evictions=<n>\n
hit_rate=<f>\n
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n
```

**Notes:**
- Returns the same metrics that are appended to every response,
  but as the primary payload rather than a footer.
- Useful for the monitor to poll metrics between experiments.

---

### RESET_METRICS

Reset all buffer pool metric counters to zero.

**Request:**
```
RESET_METRICS\n
```

**Response:**
```
OK\n
METRICS hits=0 misses=0 evictions=0 hit_rate=0.000\n
END\n
```

**Notes:**
- Used between experiments to get clean measurements.
- Does not affect buffer pool contents — pages already in memory stay.

---

### TRACE_START

Clear any existing trace buffer and start recording every buffer pool page access.

**Request:**
```
TRACE_START\n
```

**Response:**
```
OK\n
TRACE_STARTED\n
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n
```

**Notes:**
- Calling TRACE_START while already recording discards the previous trace and restarts from zero.
- Each subsequent `bm_fetch_page` call (hit or miss) appends one event to the internal buffer.
- Recording stops silently when the buffer reaches 512 events (`MAX_TRACE_EVENTS`).
- Metrics are not reset; use RESET_METRICS beforehand if clean metrics are needed.

---

### TRACE_STOP

Stop recording and return all collected events.

**Request:**
```
TRACE_STOP\n
```

**Response:**
```
OK\n
TRACE_EVENTS <count> <n_frames>\n
EVENT <timestamp> <table> <page_id> <hit> <frame_id> <evicted_frame>\n
FRAME <frame_id> <state> <table> <page_id> <dirty> <pin_count> <ref_bit> <last_access>\n
FRAME <frame_id> <state> <table> <page_id> <dirty> <pin_count> <ref_bit> <last_access>\n
...
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n
```

**Header fields:**
| Field | Type | Description |
|-------|------|-------------|
| `count` | int | Number of recorded events returned |
| `n_frames` | int | Number of frames in the buffer pool for this trace |

**EVENT line fields:**
| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | int64 | Monotonic access counter (pool.access_clock at time of access) |
| `table` | string | Table name |
| `page_id` | int | Page number within the table |
| `hit` | 0 or 1 | 1 = page was already in the buffer pool; 0 = miss (disk load) |
| `frame_id` | int | Frame that served the access, or -1 if the access failed because all frames were pinned |
| `evicted_frame` | int | Victim frame id, or -1 if no eviction happened |

**FRAME line fields:**
| Field | Type | Description |
|-------|------|-------------|
| `frame_id` | int | Frame number within the pool |
| `state` | 0, 1 or 2 | 0 = FREE, 1 = OCCUPIED, 2 = PINNED |
| `table` | string | Table name, or `.` when the frame is FREE |
| `page_id` | int | Page id stored in the frame, or -1 when FREE |
| `dirty` | 0 or 1 | 1 if the frame is dirty |
| `pin_count` | int | Current pin count |
| `ref_bit` | 0 or 1 | Clock replacement metadata |
| `last_access` | int64 | Monotonic last-access counter used by LRU/OPT visualisation |

**Notes:**
- Recording stops before the events are sent; further page accesses are not recorded.
- The event list is preserved in the server; call TRACE_CLEAR to release it.
- Each `EVENT` is followed by exactly `n_frames` `FRAME` lines describing the full buffer pool state after that access.
- Primary use: feed the OPT offline policy. Extract `table:page_id` pairs in order,
  save to `data/opt_trace.txt`, then restart the server with `opt` policy.

**Example:**
```
→ TRACE_STOP\n
← OK\n
  TRACE_EVENTS 2 2\n
  EVENT 1 users 1 0 0 -1\n
  FRAME 0 1 users 1 0 1 1 1\n
  FRAME 1 0 . -1 0 0 0 0\n
  EVENT 2 users 1 1 0 -1\n
  FRAME 0 2 users 1 0 2 1 2\n
  FRAME 1 0 . -1 0 0 0 0\n
  METRICS hits=1 misses=2 evictions=0 hit_rate=0.333\n
  END\n
```

---

### TRACE_CLEAR

Stop recording and discard all collected events without returning them.

**Request:**
```
TRACE_CLEAR\n
```

**Response:**
```
OK\n
TRACE_CLEARED\n
METRICS hits=<n> misses=<n> evictions=<n> hit_rate=<f>\n
END\n
```

**Notes:**
- Equivalent to TRACE_STOP followed by discarding the events.
- Useful for aborting a trace run without the overhead of transmitting events.

---

## Versioning

This document describes **MINIDBMS-RESP v1**.

Future versions may add:
- Pipelining (multiple requests before reading responses)
- Concurrency (multiple simultaneous connections)
- PostgreSQL wire protocol compatibility layer
- OPT policy via POLICY command with trace upload
- FETCH operation for direct page access (for index experiments)

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| v1.0 | 2026-03 | Initial specification: PING, SCAN, GET, CREATE, INSERT, DELETE, UPDATE, POLICY, METRICS, RESET_METRICS |
| v1.1 | 2026-03 | Add SCHEMA operation — column definitions with client-side caching |
| v1.2 | 2026-03 | SCHEMA column format includes pk field |
| v1.3 | 2026-04 | DELETE by ROW_ID — logical delete, slot marked in slotted page. WHERE evaluation delegated to storage engine |
| v1.4 | 2026-04 | Remove ROWS count header from SCAN — single buffer pool pass eliminates artificial cache hits |
| v1.5 | 2026-04 | SCAN row lines include RowID — format changed from `<size>` to `<row_id> <size>`. Add UPDATE with HOT update support via SLOT_REDIRECT chains |
| v1.6 | 2026-05 | Add TRACE_START, TRACE_STOP, TRACE_CLEAR — generic trace mode for OPT offline feeding, workload reproduction and access pattern analysis |
| v1.7 | 2026-05 | Enrich TRACE_STOP with per-event frame snapshots, eviction metadata and pool-size header for cache inspection playback |
