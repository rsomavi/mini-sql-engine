# SQL Engine

The SQL engine is the Python layer that accepts SQL text from the user, parses it, plans the execution, and delegates to the storage layer. It is implemented across six files in `sql-engine/`.

## File overview

| File | Responsibility |
|------|---------------|
| `lexer.py` | Tokenizes SQL strings using PLY |
| `parser.py` | PLY/Yacc grammar — produces AST from token stream |
| `ast_nodes.py` | AST node classes |
| `planner.py` | Converts AST nodes into plan objects |
| `executor.py` | Executes plans and returns results |
| `storage_server.py` | `ServerStorage` — TCP client to the C server |

---

## Pipeline

```
SQL text
    |
  Lexer          → token stream
    |
  Parser         → AST node
    |
  Planner        → plan object
    |
  Executor       → results (list of dicts)
    |
  ServerStorage  → TCP → C server
```

Each stage is independent. The lexer knows nothing about grammar, the parser knows nothing about execution, and the executor knows nothing about how storage works.

---

## Lexer — `lexer.py`

Built with PLY (`ply.lex`). The `SQLLexer` class defines token rules as attributes and methods, then calls `lex.lex(module=self)` to build the lexer.

### Tokens

49 tokens total. Keywords are case-insensitive — identifiers are lowercased before checking the reserved words dictionary:

```python
def t_ID(self, t):
    r'[a-zA-Z_][a-zA-Z0-9_]*'
    t.value = t.value.lower()
    t.type = self.reserved.get(t.value.upper(), 'ID')
    return t
```

Selected tokens:

| Token | Rule |
|-------|------|
| `NUMBER` | `\d+` — integer only, value converted to `int` |
| `STRING` | Single or double quoted, quotes stripped |
| `MINUS` | `-` — used for negative literals in parser |
| `GE`, `LE` | `>=`, `<=` — defined before `GT`, `LT` to avoid partial match |
| `STAR` | `\*` |
| `DOT` | `\.` — for qualified column references (`table.column`) |

Comments (`-- ...`) and whitespace are ignored. Line numbers are tracked for error reporting.

### Important note on PLY caching

PLY generates `parsetab.py` and `parser.out` as cached parse tables. These must be deleted after any grammar change:

```bash
rm sql-engine/parsetab.py sql-engine/parser.out
```

---

## Parser — `parser.py`

Built with PLY (`ply.yacc`). The `SQLParser` class defines grammar rules as methods with docstring productions. Operator precedence is defined explicitly:

```
NOT  >  AND  >  OR
```

This means `a OR b AND c` is parsed as `a OR (b AND c)`, matching standard SQL.

### Supported grammar

**SELECT**
```sql
SELECT [DISTINCT] columns FROM table
  [JOIN table2 ON table.col = table2.col]
  [WHERE condition]
  [GROUP BY col [, col ...]]
  [HAVING aggregate_condition]
  [ORDER BY col [ASC | DESC]]
  [LIMIT n]
```

**Aggregate functions in SELECT**
```sql
SELECT COUNT(*) FROM table
SELECT SUM(col), AVG(col), MIN(col), MAX(col) FROM table
SELECT col, COUNT(*) FROM table GROUP BY col
```

**CREATE TABLE**
```sql
CREATE TABLE name (
  col INT [PRIMARY KEY] [NOT NULL],
  col VARCHAR(N) [NOT NULL],
  col FLOAT,
  col BOOL
)
```

**INSERT**
```sql
INSERT INTO table (col1, col2) VALUES (v1, v2)
INSERT INTO table VALUES (v1, v2)   -- columns=None, uses schema order
```

**Negative literals**
```sql
INSERT INTO table VALUES (-5000, 'Ana')
```
Handled by a separate rule `p_value_negative: value : MINUS NUMBER` that negates the number at parse time. The lexer emits `-` as a `MINUS` token — negative numbers are not tokenized as a unit.

### Condition grammar

```
condition  → or_condition
or_condition  → and_condition (OR and_condition)*
and_condition → not_condition (AND not_condition)*
not_condition → NOT not_condition | simple_condition
simple_condition → column comparator value | (condition)
comparator → = | > | < | >= | <=
```

This recursive structure produces a tree of `Condition`, `LogicalCondition`, and `NotCondition` nodes.

---

## AST nodes — `ast_nodes.py`

All nodes inherit from `ASTNode`. There is no visitor pattern — the planner uses `isinstance` checks.

| Node | Fields |
|------|--------|
| `SelectQuery` | `columns, table, where, order_by, limit, distinct, group_by, having, join_table, join_condition` |
| `CountQuery` | `table, where, group_by` |
| `SumQuery` | `column, table, where, group_by` |
| `AvgQuery` | `column, table, where, group_by` |
| `MinQuery` | `column, table, where, group_by` |
| `MaxQuery` | `column, table, where, group_by` |
| `CreateTableQuery` | `table_name, columns` (list of `ColumnDef`) |
| `InsertQuery` | `table_name, columns, values` (`columns=None` for implicit form) |
| `DeleteQuery` | `table_name, where` |
| `UpdateQuery` | `table_name, assignments, where` |
| `ColumnDef` | `name, col_type, max_size, nullable, primary_key` |
| `Condition` | `column, operator, value` |
| `LogicalCondition` | `left, operator, right` |
| `NotCondition` | `condition` |

---

## Planner — `planner.py`

`QueryPlanner.plan(ast)` dispatches by type and returns a plan object. The planner is thin — it does no optimization and makes no decisions. Its only job is to copy fields from the AST into a plan struct.

The separation exists to keep a clean boundary: the parser produces declarative descriptions of what the user asked for, the planner produces imperative descriptions of what to execute.

| AST node | Plan produced |
|----------|--------------|
| `SelectQuery` | `SelectPlan` |
| `CountQuery` | `CountPlan` |
| `SumQuery` | `SumPlan` |
| `AvgQuery` | `AvgPlan` |
| `MinQuery` | `MinPlan` |
| `MaxQuery` | `MaxPlan` |
| `CreateTableQuery` | `CreateTablePlan` |
| `InsertQuery` | `InsertPlan` |
| `DeleteQuery` | `DeletePlan` |
| `UpdateQuery` | `UpdatePlan` |

`SelectPlan` additionally extracts aggregate column names from the column list (for `SUM(col)`, `AVG(col)`, etc.) and stores them as `plan.sum_column`, `plan.avg_column`, etc.

---

## Executor — `executor.py`

`QueryExecutor.execute(plan)` dispatches by plan type to a `_execute_*` method. All execution happens in Python — the executor loads rows from storage, filters and transforms them in memory, and returns a list of dicts.

### Common pattern

Most operations follow this pattern:

```python
rows = self._get_filtered_rows(plan.table, plan.where)
# transform rows
return results
```

`_get_filtered_rows` loads the table via `storage.load_table`, then applies `_evaluate_condition` to filter rows.

### Condition evaluation

`_evaluate_condition` is recursive and handles all three condition types:

```python
Condition        → compare row[col] with value using operator
LogicalCondition → evaluate left AND/OR right recursively
NotCondition     → NOT evaluate(condition)
```

Supported operators: `=`, `>`, `<`, `>=`, `<=`.

### SELECT execution

1. Load and filter rows with WHERE
2. Apply INNER JOIN if `plan.join_table` is set (nested loop)
3. Apply GROUP BY + HAVING if present
4. Apply ORDER BY (Python `sorted` with key function)
5. Apply DISTINCT (`set` of tuples, then back to dicts)
6. Apply LIMIT (`rows[:plan.limit]`)
7. Apply column projection

### Aggregate execution

COUNT, SUM, AVG, MIN, MAX each have a dedicated `_execute_*` method and a `_execute_*_with_group_by` variant. When `plan.group_by` is set, rows are grouped into a dict keyed by the group-by column values, and the aggregate is computed per group.

### DML execution

```python
def _execute_insert(self, plan):
    row_id = self.storage.insert_row(plan.table_name, plan.columns, plan.values)
    return [{"result": "1 row inserted", "row_id": row_id}]

def _execute_create(self, plan):
    self.storage.create_table(plan.table_name, plan.columns)
    return [{"result": f"Table '{plan.table_name}' created successfully"}]
```

DELETE and UPDATE delegate to `storage.delete_rows` and `storage.update_rows` respectively.

---

## ServerStorage — `storage_server.py`

`ServerStorage` is the TCP client that connects to the C server. It implements the storage interface expected by the executor.

### Connection model

Each operation opens a new TCP connection to `localhost:5433`, sends one request, reads the response, and closes the connection. There is no connection pooling in v1.

```python
sock = self._connect()
try:
    self._send(sock, command)
    resp = self._read_response(sock)
finally:
    sock.close()
```

### Key methods

`load_table(table_name)` sends `SCAN table_name`, receives binary rows, deserializes each one, and returns a `Table` object with a list of dicts.

`_fetch_schema(table_name)` sends `SCHEMA table_name` and returns a list of column dicts with `name`, `type`, `max_size`, `nullable`, `pk`.

`insert_row(table_name, columns, values)` fetches the schema, builds a value dict from columns and values (if `columns=None`, uses schema column order), serializes the row to binary with `_serialize_row`, and sends `INSERT table_name size\n<bytes>`.

`create_table(table_name, columns)` serializes the column list as `name:type:max_size:nullable:pk` tokens and sends `CREATE table_name col_defs`.

`ping()` sends `PING` and returns `True` if the server responds with `PONG`.

### Row serialization

`_serialize_row` produces the same binary format as `schema.c row_serialize`:

```
[null_bitmap: ceil(n_cols/8) bytes]
per column (skipped if NULL):
  INT:     struct.pack('<i', val)    — 4 bytes little-endian int32
  FLOAT:   struct.pack('<f', val)    — 4 bytes little-endian float32
  BOOL:    struct.pack('B', val)     — 1 byte
  VARCHAR: struct.pack('<H', len) + bytes  — 2-byte length + UTF-8
```

`_deserialize_row` is the inverse — reads the null bitmap first, then reads each non-NULL column in schema order and returns a dict.

### Metrics

`get_metrics()` returns the last metrics received from the server (hits, misses, evictions, hit_rate). `reset_metrics()` sends no request — it resets the local copy and requests a reset from the server on the next operation.

---

## SQL supported

### Queries

```sql
SELECT * FROM empleados
SELECT id, nombre FROM empleados WHERE salario > 30000
SELECT * FROM users WHERE city = 'Madrid' AND age > 25
SELECT * FROM empleados WHERE NOT salario > 50000
SELECT DISTINCT city FROM users
SELECT city, COUNT(*) FROM users GROUP BY city
SELECT city, AVG(age) FROM users GROUP BY city HAVING AVG(age) > 26
SELECT * FROM users ORDER BY age DESC LIMIT 5
SELECT name, amount FROM users JOIN orders ON users.id = orders.user_id
```

### DML

```sql
CREATE TABLE empleados (id INT PRIMARY KEY, nombre VARCHAR(50) NOT NULL, salario INT)
INSERT INTO empleados (id, nombre, salario) VALUES (1, 'Ana', 35000)
INSERT INTO empleados VALUES (1, 'Juan', 27000)
```

### Not yet supported

```sql
DELETE FROM empleados WHERE id = 1    -- pending
UPDATE empleados SET salario = 40000 WHERE id = 1  -- pending
```
