# minidbms

A minimal but complete Database Management System written in Python and C. It implements a full query pipeline — lexer, parser, AST, planner, executor — backed by a persistent storage engine in C with disk-based page management, slotted pages, heap files, and typed schemas.

Built as an experimental platform to compare buffer pool replacement policies (NoCache, Clock, LRU, OPT) under controlled conditions — the core research question of the accompanying TFG at Universidad Autónoma de Madrid.

## Architecture

```
SQL Query (text)
      |
   Lexer              tokens          (PLY tokenizer)
      |
   Parser             AST             (PLY/Yacc grammar)
      |
   Planner            query plan      (AST -> SelectPlan / AggregatePlan)
      |
   Executor           results         (WHERE, JOIN, GROUP BY, ORDER BY...)
      |
   Storage Interface
      |-- MemoryStorage               (in-memory, for tests)
      +-- DiskStorage                 (calls C engine via subprocess)
                |
                +-- Storage Engine (C)
                      |-- Schema   (page 0)    TYPE_INT / FLOAT / BOOLEAN / VARCHAR
                      |-- Heap     (pages 1+)  insert_into_table, RowID encoding
                      |-- Page                 slotted page, 4096 bytes
                      +-- Disk                 write_page, load_page, .db files
```

## Components

### SQL Engine (Python)

| File | Responsibility |
|------|---------------|
| `lexer.py` | Tokenizes SQL strings. Keywords, operators, literals, identifiers |
| `parser.py` | PLY/Yacc grammar -> AST. Handles precedence: NOT > AND > OR |
| `ast_nodes.py` | SelectQuery, CountQuery, SumQuery, AvgQuery, MinQuery, MaxQuery, Condition, LogicalCondition, NotCondition |
| `planner.py` | Transforms AST into executable plans |
| `executor.py` | Runs plans: WHERE, INNER JOIN, GROUP BY, HAVING, DISTINCT, ORDER BY, LIMIT |
| `storage.py` | MemoryStorage (dict-based) and DiskStorage (calls C engine) |
| `ui.py` | Terminal dashboard for interactive queries |
| `main.py` | Entry point |

### Storage Engine (C)

| File | Responsibility |
|------|---------------|
| `disk.c / disk.h` | Raw page I/O. write_page, load_page, get_num_pages. Files: table.db, PAGE_SIZE = 4096 |
| `page.c / page.h` | Slotted page format. init_page, insert_row, delete_row, read_row, get_row_size |
| `heap.c / heap.h` | Multi-page heap file. insert_into_table, scan_table. Page 0 reserved for schema. RowID = (page_id << 16) | slot_id |
| `schema.c / schema.h` | Table schema: TYPE_INT, TYPE_FLOAT, TYPE_BOOLEAN, TYPE_VARCHAR. schema_save, schema_load, row_serialize, row_deserialize with null bitmap |

## Supported SQL

### Queries

```sql
SELECT name, age FROM users WHERE age > 25

SELECT city, COUNT(*) FROM users GROUP BY city HAVING COUNT(*) > 2 ORDER BY COUNT(*) LIMIT 2

SELECT city, AVG(age) FROM users GROUP BY city

SELECT DISTINCT city FROM users LIMIT 3

SELECT city, COUNT(*) FROM users
  WHERE age > 25 AND city = 'Madrid' OR age > 30
  GROUP BY city HAVING COUNT(*) > 1
  ORDER BY COUNT(*) LIMIT 3
```

### INNER JOIN

```sql
SELECT users.name, orders.amount
  FROM users
  JOIN orders ON users.id = orders.user_id
  WHERE orders.amount > 100
```

### Supported clauses

| Clause | Status |
|--------|--------|
| SELECT | done |
| WHERE (AND / OR / NOT) | done |
| INNER JOIN | done |
| GROUP BY | done |
| HAVING | done |
| ORDER BY ASC / DESC | done |
| LIMIT | done |
| DISTINCT | done |
| COUNT / SUM / AVG / MIN / MAX | done |
| INSERT / UPDATE / DELETE | pending |
| CREATE TABLE | pending |

## Tests

### Storage engine (C)

```bash
cd storage-engine/tests

gcc -Wall -Wextra -g -o test_disk        test_disk.c        ../disk.c
gcc -Wall -Wextra -g -o test_page        test_page.c        ../page.c ../disk.c
gcc -Wall -Wextra -g -o test_heap        test_heap.c        ../heap.c ../page.c ../disk.c
gcc -Wall -Wextra -g -o test_schema      test_schema.c      ../schema.c ../heap.c ../page.c ../disk.c
gcc -Wall -Wextra -g -o test_integration test_integration.c ../schema.c ../heap.c ../page.c ../disk.c

./test_disk && ./test_page && ./test_heap && ./test_schema && ./test_integration
```

| Suite | Tests | Coverage |
|-------|-------|----------|
| test_disk.c | 38 | write/read roundtrip, page isolation, binary data, bounds with fork, stress 100 pages |
| test_page.c | 36 | slotted page, insert/delete/reuse slots, capacity, binary with null bytes |
| test_heap.c | 35 | RowID encoding, page 0 reserved, overflow to page 2, two tables, stress |
| test_schema.c | 32 | all 4 types, MAX_COLUMNS, save/load, row_serialize null bitmap, INT_MIN/MAX, VARCHAR 255 |
| test_integration.c | 14 | schema + heap end-to-end, reload simulation, two tables interleaved |
| **Total** | **155** | **all passing** |

### SQL engine (Python)

```bash
python tests/run_tests.py
```

## Running

### Prerequisites

```bash
pip install ply
```

### Interactive mode

```bash
python main.py
```

### Programmatic usage

```python
from parser import get_parser
from planner import QueryPlanner
from executor import QueryExecutor
from storage import MemoryStorage

database = {
    "users": [
        {"id": 1, "name": "Juan",  "age": 25, "city": "Madrid"},
        {"id": 2, "name": "Ana",   "age": 30, "city": "Barcelona"},
    ]
}

parser   = get_parser()
planner  = QueryPlanner()
storage  = MemoryStorage(database)
executor = QueryExecutor(storage)

ast    = parser.parse("SELECT name FROM users WHERE age > 20")
plan   = planner.plan(ast)
result = executor.execute(plan)

for row in result:
    print(row)
```

## Project structure

```
minidbms/
|-- lexer.py
|-- parser.py
|-- planner.py
|-- executor.py
|-- storage.py
|-- table.py
|-- ast_nodes.py
|-- ast_printer.py
|-- ui.py
|-- main.py
|
|-- tests/                         SQL engine tests (Python)
|   |-- run_tests.py
|   |-- test_queries.py
|   |-- test_group_by.py
|   |-- test_having.py
|   +-- test_sql_engine.py
|
+-- storage-engine/                Storage engine (C)
    |-- disk.c / disk.h
    |-- page.c / page.h
    |-- heap.c / heap.h
    |-- schema.c / schema.h
    +-- tests/
        |-- test_disk.c
        |-- test_page.c
        |-- test_heap.c
        |-- test_schema.c
        +-- test_integration.c
```

## Roadmap

- done: SQL parser with full SELECT support
- done: INNER JOIN (nested loop)
- done: GROUP BY / HAVING / ORDER BY / DISTINCT / LIMIT
- done: Disk-based storage engine in C (disk, page, heap, schema)
- done: Typed rows with null bitmap (INT, FLOAT, BOOLEAN, VARCHAR)
- done: 155-test suite for storage engine
- pending: Buffer pool with pluggable replacement policies (NoCache / Clock / LRU / OPT)
- pending: IPC server — C engine as shared library called from Python via ctypes
- pending: DML: INSERT, UPDATE, DELETE, CREATE TABLE
- pending: WAL (Write-Ahead Log)
- pending: B+ tree indexes
- pending: Hash join and merge join
- pending: Metrics monitor (hit rate, I/Os, latency, distance to Belady OPT)

---

Built with PLY (Python Lex-Yacc) and C (gcc).
