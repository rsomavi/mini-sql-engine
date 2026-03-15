# SQL-Parser

A minimal SQL parser and execution engine written in Python. It implements a simplified SQL query pipeline similar to real database engines, featuring a lexer, parser, query planner, and in-memory storage.

## Features

- **SQL Lexer** — Tokenizer that breaks SQL queries into meaningful tokens
- **SQL Parser** — Built with PLY (Python Lex-Yacc) to generate an Abstract Syntax Tree (AST)
- **Query Planner** — Optimizes and plans query execution
- **Query Executor** — Executes queries against in-memory storage
- **In-Memory Storage Engine** — Fast, ephemeral data storage
- **Terminal Dashboard UI** — Interactive CLI interface
- **Comprehensive Test Suite** — Unit tests for core functionality

### Supported SQL Features

| Category | Supported |
|----------|-----------|
| Queries | `SELECT` |
| Filtering | `WHERE` |
| Grouping | `GROUP BY` |
| Group Filtering | `HAVING` |
| Sorting | `ORDER BY` |
| Limiting | `LIMIT` |
| Deduplication | `DISTINCT` |

### Aggregate Functions

- `COUNT(*)`
- `SUM(column)`
- `AVG(column)`
- `MIN(column)`
- `MAX(column)`

### Operators

**Logical:**
- `AND`, `OR`, `NOT`

**Comparison:**
- `=`, `>`, `<`, `>=`, `<=`, `!=`

## Architecture

```
SQL Query
    ↓
Lexer → Tokens
    ↓
Parser → AST
    ↓
Planner → Query Plan
    ↓
Executor → Results
```

### Components

1. **Lexer** (`lexer.py`) — Tokenizes raw SQL strings into a stream of tokens
2. **Parser** (`parser.py`) — Uses PLY to parse tokens and build an AST
3. **AST Nodes** (`ast_nodes.py`) — Defines the node types representing SQL constructs
4. **AST Printer** (`ast_printer.py`) — Utility for visualizing the AST
5. **Planner** (`planner.py`) — Transforms AST into an executable query plan
6. **Executor** (`executor.py`) — Runs the query plan against storage and returns results
7. **Storage** (`storage.py`) — In-memory table storage engine
8. **Table** (`table.py`) — Table data structure and operations
9. **UI** (`ui.py`) — Terminal-based dashboard for interactive queries
10. **Main** (`main.py`) — Entry point for the application

## Example Queries

### Basic SELECT

```sql
SELECT name, age FROM users WHERE age > 25
```

### GROUP BY with HAVING

```sql
SELECT city, COUNT(*) FROM users GROUP BY city HAVING COUNT(*) > 2 ORDER BY COUNT(*) LIMIT 2
```

### Aggregate Functions

```sql
SELECT department, AVG(salary) AS avg_salary, MIN(salary) AS min_salary, MAX(salary) AS max_salary, COUNT(*) AS employee_count FROM employees GROUP BY department HAVING AVG(salary) > 50000 ORDER BY avg_salary DESC
```

### DISTINCT and LIMIT

```sql
SELECT DISTINCT category FROM products LIMIT 10
```

### Complex WHERE with AND/OR

```sql
SELECT * FROM orders WHERE status = 'pending' AND (total > 100 OR priority = 'high') ORDER BY created_at DESC LIMIT 5
```

## Running the Project

### Prerequisites

- Python 3.8+
- PLY (Python Lex-Yacc)

```bash
pip install ply
```

### Interactive Mode

Start the terminal dashboard:

```bash
python main.py
```

### Programmatic Usage

```python
from lexer import Lexer
from parser import Parser
from planner import Planner
from executor import Executor
from storage import Storage

# Initialize components
storage = Storage()
lexer = Lexer()
parser = Parser()
planner = Planner()
executor = Executor(storage)

# Execute a query
query = "SELECT name FROM users WHERE age > 21"
tokens = lexer.tokenize(query)
ast = parser.parse(tokens)
plan = planner.plan(ast)
results = executor.execute(plan)

for row in results:
    print(row)
```

## Running the Tests

Execute the test suite:

```bash
python tests/run_tests.py
```

Run specific test files:

```bash
python tests/test_group_by.py
python tests/test_having.py
```

## Project Structure

```
SQL-Parser/
├── lexer.py           # SQL tokenizer
├── parser.py          # PLY-based SQL parser
├── planner.py         # Query planning
├── executor.py        # Query execution
├── storage.py         # In-memory storage engine
├── table.py           # Table data structure
├── ast_nodes.py       # AST node definitions
├── ast_printer.py     # AST visualization
├── ui.py              # Terminal dashboard
├── main.py            # Application entry point
│
└── tests/
    ├── run_tests.py   # Test runner
    ├── test_group_by.py
    ├── test_having.py
    └── ...
```

## Future Improvements

- [ ] JOIN operations (INNER, LEFT, RIGHT)
- [ ] INSERT, UPDATE, DELETE statements
- [ ] CREATE TABLE support
- [ ] Indexing for query optimization
- [ ] Subqueries
- [ ] Window functions
- [ ] Transaction support (BEGIN, COMMIT, ROLLBACK)
- [ ] Persistence layer (save/load to disk)
- [ ] Additional data types (DATE, TIMESTAMP, BOOLEAN)
- [ ] Query caching

## Development Status

**Under Development**

This project is actively being developed. Features, APIs, and internal architecture may change. Contributions and feedback are welcome.

---

Built with [PLY (Python Lex-Yacc)](https://www.dabeaz.com/ply/)
