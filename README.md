# minidbms

A database management system implemented from scratch in C and Python, without relying on any existing database libraries or storage frameworks.

The project serves as an experimental platform to empirically verify theoretical properties of buffer pool replacement policies — properties that have been proven mathematically but never measured on a functioning DBMS executing real SQL workloads.

Built as a TFG (Bachelor's thesis) at Universidad Autónoma de Madrid.

---

## Experimental Results

Running the same SQL workload across four buffer pool policies and varying pool sizes from 3 to 64 frames reveals three theoretical properties empirically:

**1. Bélády's anomaly** — with 3 and 8 frames, NoCache outperforms LRU. A simpler policy beats a smarter one with small pool sizes.

**2. LRU stack property** — the LRU line never decreases. Adding more frames never hurts LRU performance.

**3. Working set convergence** — at 32 frames all policies converge to the same hit rate with zero evictions. The pool is large enough to hold the entire working set; further frames add nothing.

OPT (Bélády's optimal offline policy) serves as the theoretical upper bound throughout.

---

## Architecture

The system is split into two subsystems connected via TCP on port 5433:

```
SQL Engine (Python)          C Server
  Lexer                      protocol_read_request
  Parser        TCP :5433    handler_dispatch
  Planner      ─────────►    heap / HOT updates
  Executor                   BufferManager
  ServerStorage              EvictionPolicy
  populate.py                  NoCache / Clock / LRU / OPT
                             Trace (generic access recorder)
                             disk → .db files
```

The GUI (Tkinter + matplotlib) connects to the same server and provides:
- **Query Runner** — ad-hoc SQL queries with live telemetry
- **Benchmark** — compare policies on the same workload with bar charts
- **Sweep Analysis** — hit rate vs pool size line chart, updated in real time as each frame size completes
- **Cache Inspector** — step-by-step buffer pool visualisation with full frame state (state, dirty, pin_count, ref_bit, last_access), playback controls, and event log

See [docs/architecture.md](docs/architecture.md) for the full layer diagram and data flow.

---

## Status

| Feature | Status |
|---------|--------|
| SELECT (WHERE, JOIN, GROUP BY, HAVING, ORDER BY, DISTINCT, LIMIT) |  done |
| CREATE TABLE |  done |
| INSERT (with and without column list) |  done |
| DELETE (WHERE evaluated in C server) |  done |
| UPDATE with HOT updates (SLOT_REDIRECT) |  done |
| Buffer pool — NoCache, Clock, LRU, OPT |  done |
| TCP protocol — MINIDBMS-RESP v1.7 |  done |
| Persistence (dirty page flush on shutdown) |  done |
| Generic trace mode (TRACE_START/STOP/CLEAR) |  done |
| Enriched trace with full frame snapshots per event |  done |
| OPT integration via offline trace |  done |
| GUI — Query Runner, Benchmark, Sweep Analysis, Cache Inspector |  done |
| Slotted pages with explicit slot state (SLOT_NORMAL/DELETED/REDIRECT) |  done |
| B+ tree indexes + PK enforcement |  planned |
| WAL (Write-Ahead Log) |  planned |
| VACUUM (slot compaction) |  planned |
| Concurrency (multi-client) |  planned |

---

## Requirements

**Python dependencies:**

```bash
pip install ply rich matplotlib
```

- `ply` — SQL lexer and parser
- `rich` — terminal dashboard for the SQL REPL
- `matplotlib` — charts in the GUI
- `tkinter` — GUI framework (included with most Python installations)

**C compiler:**

```bash
gcc --version   # gcc 11+ recommended
```

---

## Running

There are two ways to use minidbms — via the GUI or via the terminal REPL. Choose one.

### Option A — GUI (recommended)

The GUI manages the server lifecycle internally. No need to start the server manually.

```bash
cd gui-app/
python3 main.py
```

Use the Server Runtime panel to start the server with your chosen policy and frame count. The Query Runner, Benchmark, Sweep Analysis, and Cache Inspector tabs are all available from there.

### Option B — Terminal REPL

Start the C server manually:

```bash
cd server/
make run                        # default: lru, 64 frames
make run POLICY=clock
./minidbms-server ../data 64 nocache   # explicit
```

Then start the SQL REPL in a separate terminal:

```bash
cd sql-engine/
python3 main.py
```

```sql
sql> CREATE TABLE empleados (id INT, nombre VARCHAR(50), salario INT);
sql> INSERT INTO empleados VALUES (1, 'Ana', 35000);
sql> SELECT * FROM empleados WHERE salario > 30000;
sql> UPDATE empleados SET salario = 40000 WHERE id = 1;
sql> DELETE FROM empleados WHERE id = 1;
```

### Seed the database (optional)

To populate the database with realistic test data (2010 rows across 5 tables) for buffer pool experiments:

```bash
# Server must be running first
cd sql-engine/
python3 populate.py
```

---

## Tests

**Storage engine (C) — 488+ tests:**

```bash
cd storage-engine/tests/
make run
```

**SQL engine (Python) — no server required:**

```bash
cd sql-engine/
python3 tests/run_tests.py
```

**Integration tests — server must be running on port 5433:**

```bash
cd server/tests/
python3 test_update.py          # 47 tests — UPDATE, HOT updates, SLOT_REDIRECT
python3 test_reset_metrics.py   # 11 tests — RESET_METRICS protocol
python3 test_scan.py
python3 test_create.py
python3 test_schema.py
python3 test_ping.py
```

---

## Protocol

The Python executor and C server communicate via **MINIDBMS-RESP v1.7** — a custom text-framed binary-payload protocol inspired by Redis RESP.

Operations: `PING`, `SCAN`, `SCHEMA`, `GET`, `CREATE`, `INSERT`, `UPDATE`, `DELETE`, `POLICY`, `METRICS`, `RESET_METRICS`, `TRACE_START`, `TRACE_STOP`, `TRACE_CLEAR`

Key design decisions:
- `SCAN` returns `<row_id> <size>` per row so the client has the RowID without a second pass
- `UPDATE` uses HOT updates via `SLOT_REDIRECT` chains — original RowID always valid
- `TRACE_STOP` returns full frame snapshots per event for step-by-step replay in the Cache Inspector

See [docs/protocol.md](docs/protocol.md) for the full specification.

---

## Documentation

| Doc | Contents |
|-----|----------|
| [docs/architecture.md](docs/architecture.md) | Layer diagram, data flows, binary row format, physical file layout |
| [docs/storage-engine.md](docs/storage-engine.md) | Disk I/O, slotted pages with explicit slot state, HOT updates, heap files, schema, RowID encoding |
| [docs/buffer-pool.md](docs/buffer-pool.md) | Buffer pool, page table, eviction policies, trace mode, OPT integration |
| [docs/sql-engine.md](docs/sql-engine.md) | Lexer, parser, AST, planner, executor, ServerStorage |
| [docs/server.md](docs/server.md) | TCP server, protocol MINIDBMS-RESP v1.7, handlers |
| [docs/protocol.md](docs/protocol.md) | Full protocol specification with versioned changelog |
| [docs/tests.md](docs/tests.md) | Test suites, how to run, coverage |
