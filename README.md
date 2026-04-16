# minidbms

A database management system implemented from the ground up in C and Python, without relying on any existing database libraries or storage frameworks.

The project serves as an experimental platform to study the empirical behavior of buffer pool replacement policies (NoCache, Clock, LRU, OPT) under real SQL workloads — verifying theoretical properties that have been proven mathematically but never measured on a functioning system executing actual queries.

Built as a TFG (Bachelor's thesis) at Universidad Autónoma de Madrid.

---

## Architecture

The system is split into two subsystems connected via TCP on port 5433:

```
SQL Engine (Python)          C Server

  Lexer                      protocol_read_request
  Parser        TCP :5433    handler_dispatch
  Planner      ─────────►    heap_insert_bm
  Executor                   BufferManager
  ServerStorage              EvictionPolicy (NoCache/Clock/LRU/OPT)
                             disk → .db files
```

See [docs/architecture.md](docs/architecture.md) for the full layer diagram and data flow.

---

## Status

| Feature | Status |
|---------|--------|
| SELECT (WHERE, JOIN, GROUP BY, HAVING, ORDER BY, DISTINCT, LIMIT) | done |
| CREATE TABLE | done |
| INSERT (with and without column list) | done |
| Buffer pool — NoCache, Clock, LRU, OPT | done |
| TCP server — MINIDBMS-RESP v1.2 | done |
| Persistence (dirty page flush on shutdown) | done |
| DELETE | pending |
| UPDATE | pending |
| Experimental framework (buffer pool benchmarks) | pending |
| WAL | pending |

---

## Running

Start the server:

```bash
cd server/
make run
```

Choose a policy explicitly:

```bash
cd server/
make run POLICY=clock
# or:
./minidbms-server ../data 64 nocache
```

Start the SQL client in a separate terminal:

```bash
cd sql-engine/
python3 main.py
```

```sql
sql> CREATE TABLE empleados (id INT, nombre VARCHAR(50), salario INT);
sql> INSERT INTO empleados VALUES (1, 'Ana', 35000);
sql> SELECT * FROM empleados WHERE salario > 30000;
```

---

## Tests

**Storage engine (C):**

```bash
cd storage-engine/tests/
make run
```

**SQL engine (Python) — no server required:**

```bash
cd sql-engine/
python3 tests/run_tests.py
```

**Integration tests — server must be running:**

```bash
python3 tests/test_server_storage.py
python3 tests/test_insert.py
```

---

## Documentation

| Doc | Contents |
|-----|----------|
| [docs/architecture.md](docs/architecture.md) | Layer diagram, data flows, binary row format, physical file layout |
| [docs/storage-engine.md](docs/storage-engine.md) | Disk I/O, slotted pages, heap files, schema, RowID encoding |
| [docs/buffer-pool.md](docs/buffer-pool.md) | Buffer pool, page table, eviction policies, experimental framework |
| [docs/sql-engine.md](docs/sql-engine.md) | Lexer, parser, AST, planner, executor, ServerStorage |
| [docs/server.md](docs/server.md) | TCP server, protocol MINIDBMS-RESP v1.2, handlers |
| [docs/protocol.md](docs/protocol.md) | Full protocol specification |
| [docs/tests.md](docs/tests.md) | Test suites, how to run, coverage |

---

## Requirements

```bash
pip install ply
gcc --version   # gcc 11+ recommended
```
