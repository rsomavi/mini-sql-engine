"""
populate.py — seed the database with realistic test data for buffer pool experiments.

Run from sql-engine/:
    python3 populate.py

Requires the C server to be running on localhost:5433.
Uses random.seed(42) so every run produces the same dataset.
"""

import io
import random
import sys

from parser import get_parser
from planner import QueryPlanner
from executor import QueryExecutor
from storage_server import ServerStorage


random.seed(42)

# =============================================================================
# Data pools
# =============================================================================

_FIRST_NAMES = [
    "Carlos", "Maria", "Ana", "Jose", "Luis", "Laura", "Miguel", "Sofia",
    "David", "Elena", "Pedro", "Carmen", "Juan", "Isabel", "Pablo", "Lucia",
    "Fernando", "Teresa", "Alberto", "Rosa", "Manuel", "Pilar", "Francisco",
    "Marta", "Antonio", "Cristina", "Alejandro", "Sandra", "Javier", "Patricia",
    "Roberto", "Monica", "Sergio", "Natalia", "Daniel", "Beatriz", "Rafael",
    "Silvia", "Jorge", "Raquel", "Oscar", "Gloria", "Ruben", "Claudia",
    "Victor", "Nuria", "Marcos", "Alicia", "Andres", "Miriam",
]

_LAST_NAMES = [
    "Garcia", "Rodriguez", "Martinez", "Lopez", "Sanchez", "Perez",
    "Gonzalez", "Fernandez", "Torres", "Diaz", "Ruiz", "Moreno",
    "Jimenez", "Alvarez", "Romero", "Navarro", "Gutierrez", "Molina",
    "Morales", "Ortega", "Delgado", "Castro", "Suarez", "Vargas",
    "Ramos", "Gil", "Serrano", "Medina", "Reyes", "Herrera",
]

_CITIES = [
    "Madrid", "Barcelona", "Valencia", "Sevilla", "Bilbao",
    "Zaragoza", "Malaga", "Murcia", "Palma", "Valladolid",
]

_PRODUCT_CATEGORIES = [
    "Electronica", "Ropa", "Hogar", "Deportes", "Alimentacion",
]

_PRODUCTS_BY_CATEGORY = {
    "Electronica":  ["Laptop", "Smartphone", "Tablet", "Monitor", "Teclado",
                     "Raton", "Auriculares", "Camara", "Impresora", "Altavoz"],
    "Ropa":         ["Camiseta", "Pantalon", "Chaqueta", "Zapatos", "Bolso",
                     "Vestido", "Abrigo", "Jersey", "Bufanda", "Gorra"],
    "Hogar":        ["Sofa", "Mesa", "Silla", "Lampara", "Espejo",
                     "Alfombra", "Cortina", "Cojin", "Estanteria", "Armario"],
    "Deportes":     ["Bicicleta", "Raqueta", "Pelota", "Zapatillas", "Mochila",
                     "Pesas", "Casco", "Guantes", "Red", "Cronometro"],
    "Alimentacion": ["Aceite", "Vino", "Cafe", "Galletas", "Conservas",
                     "Pan", "Queso", "Jamon", "Miel", "Pasta"],
}

_PRODUCT_ADJECTIVES = [
    "Pro", "Plus", "Max", "Lite", "Sport", "Classic", "Premium", "Basic",
]

_ORDER_STATUSES = [
    "pending", "confirmed", "shipped", "delivered", "cancelled",
]

_EMPLOYEE_DEPARTMENTS = [
    "Tecnologia", "Marketing", "Ventas", "Recursos Humanos", "Finanzas",
]

_DEPARTMENT_ROWS = [
    (1,  "Tecnologia",       random.randint(100000, 1000000), 0),
    (2,  "Marketing",        random.randint(100000, 1000000), 0),
    (3,  "Ventas",           random.randint(100000, 1000000), 0),
    (4,  "Recursos Humanos", random.randint(100000, 1000000), 0),
    (5,  "Finanzas",         random.randint(100000, 1000000), 0),
    (6,  "Operaciones",      random.randint(100000, 1000000), 0),
    (7,  "Legal",            random.randint(100000, 1000000), 0),
    (8,  "Investigacion",    random.randint(100000, 1000000), 0),
    (9,  "Logistica",        random.randint(100000, 1000000), 0),
    (10, "Compras",          random.randint(100000, 1000000), 0),
]


def _full_name():
    return f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}"


def _product_name():
    cat  = random.choice(_PRODUCT_CATEGORIES)
    base = random.choice(_PRODUCTS_BY_CATEGORY[cat])
    adj  = random.choice(_PRODUCT_ADJECTIVES)
    num  = random.randint(1, 9)
    return f"{base} {adj} {num}"


# =============================================================================
# SQL pipeline helpers
# =============================================================================

def _run(parser, planner, executor, sql):
    """Execute one SQL statement through parser → planner → executor."""
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        ast = parser.parse(sql)
    finally:
        sys.stdout = old_stdout

    if ast is None:
        raise ValueError(f"Parse error in: {sql[:100]}")

    plan = planner.plan(ast)
    return executor.execute(plan)


def _lit(value):
    """Format a Python value as a SQL literal."""
    if isinstance(value, str):
        return f"'{value}'"
    return str(value)


# =============================================================================
# Row generators (each returns a list of values for the INSERT)
# =============================================================================

def _users_row(i):
    return [i, _full_name(), random.randint(18, 65), random.choice(_CITIES)]


def _products_row(i):
    return [
        i,
        _product_name(),
        random.randint(10, 2000),
        random.randint(0, 500),
        random.choice(_PRODUCT_CATEGORIES),
    ]


def _orders_row(i):
    return [
        i,
        random.randint(1, 500),
        random.randint(1, 300),
        random.randint(1, 10),
        random.choice(_ORDER_STATUSES),
    ]


def _employees_row(i):
    return [
        i,
        _full_name(),
        random.choice(_EMPLOYEE_DEPARTMENTS),
        random.randint(25000, 90000),
    ]


def _departments_row(i):
    dept_id, name, budget, _ = _DEPARTMENT_ROWS[i - 1]
    return [dept_id, name, budget, random.randint(1, 200)]


# =============================================================================
# Table specs
# =============================================================================

_TABLES = [
    {
        "name":    "users",
        "create":  (
            "CREATE TABLE users "
            "(id INT, name VARCHAR(50), age INT, city VARCHAR(50))"
        ),
        "columns": ["id", "name", "age", "city"],
        "n_rows":  500,
        "gen":     _users_row,
    },
    {
        "name":    "products",
        "create":  (
            "CREATE TABLE products "
            "(id INT, name VARCHAR(100), price INT, stock INT, category VARCHAR(50))"
        ),
        "columns": ["id", "name", "price", "stock", "category"],
        "n_rows":  300,
        "gen":     _products_row,
    },
    {
        "name":    "orders",
        "create":  (
            "CREATE TABLE orders "
            "(id INT, user_id INT, product_id INT, amount INT, status VARCHAR(20))"
        ),
        "columns": ["id", "user_id", "product_id", "amount", "status"],
        "n_rows":  1000,
        "gen":     _orders_row,
    },
    {
        "name":    "employees",
        "create":  (
            "CREATE TABLE employees "
            "(id INT, name VARCHAR(50), department VARCHAR(50), salary INT)"
        ),
        "columns": ["id", "name", "department", "salary"],
        "n_rows":  200,
        "gen":     _employees_row,
    },
    {
        "name":    "departments",
        "create":  (
            "CREATE TABLE departments "
            "(id INT, name VARCHAR(50), budget INT, manager_id INT)"
        ),
        "columns": ["id", "name", "budget", "manager_id"],
        "n_rows":  10,
        "gen":     _departments_row,
    },
]


# =============================================================================
# Main
# =============================================================================

def main():
    parser   = get_parser()
    planner  = QueryPlanner()
    storage  = ServerStorage()
    executor = QueryExecutor(storage)

    summary       = {}
    total_rows    = 0

    for spec in _TABLES:
        name    = spec["name"]
        n_rows  = spec["n_rows"]
        cols    = spec["columns"]
        gen     = spec["gen"]
        col_sql = ", ".join(cols)

        # ---- CREATE TABLE ----
        print(f"\nCreating table {name}...")
        try:
            _run(parser, planner, executor, spec["create"])
        except RuntimeError as exc:
            if "TABLE_EXISTS" in str(exc):
                print(f"  WARNING: table '{name}' already exists, skipping.")
                summary[name] = 0
                continue
            raise

        # ---- INSERT rows ----
        print(f"Inserting {n_rows} rows into {name}...")
        inserted = 0

        for i in range(1, n_rows + 1):
            row      = gen(i)
            val_sql  = ", ".join(_lit(v) for v in row)
            sql      = f"INSERT INTO {name} ({col_sql}) VALUES ({val_sql})"
            _run(parser, planner, executor, sql)
            inserted += 1

            if inserted % 50 == 0 or inserted == n_rows:
                print(f"  {inserted}/{n_rows}", flush=True)

        summary[name] = inserted
        total_rows   += inserted

    # ---- Summary ----
    print("\n=== Summary ===")
    for table_name, count in summary.items():
        print(f"  {table_name:<15} {count} rows")
    print(f"  {'Total':<15} {total_rows} rows")


if __name__ == "__main__":
    main()
