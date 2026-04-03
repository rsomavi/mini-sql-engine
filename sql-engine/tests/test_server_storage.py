#!/usr/bin/env python3
# test_server_storage.py - Integration test for ServerStorage
# Requires: server running on localhost:5433
# Run from sql-parse/sql-engine/: python3 tests/test_server_storage.py

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from storage_server import ServerStorage

# ============================================================================
# Infrastructure
# ============================================================================

tests_passed = 0
tests_failed  = 0


def check(condition, msg):
    global tests_passed, tests_failed
    if condition:
        print(f"  PASS: {msg}")
        tests_passed += 1
    else:
        print(f"  FAIL: {msg}")
        tests_failed += 1


# ============================================================================
# Block 1: connection
# ============================================================================

def test_ping():
    s = ServerStorage()
    check(s.ping(), "server is alive")


def test_ping_wrong_port():
    s = ServerStorage(port=9999)
    check(not s.ping(), "ping on wrong port returns False")


# ============================================================================
# Block 2: load_table returns Table
# ============================================================================

def test_load_table_returns_table():
    from table import Table
    s = ServerStorage()
    t = s.load_table("users")
    check(isinstance(t, Table), "load_table returns Table object")


def test_load_table_has_rows():
    s = ServerStorage()
    t = s.load_table("users")
    rows = t.get_rows()
    check(len(rows) > 0, f"users has rows (got {len(rows)})")


def test_load_table_rows_are_dicts():
    s = ServerStorage()
    rows = s.load_table("users").get_rows()
    for i, row in enumerate(rows):
        check(isinstance(row, dict), f"row {i} is a dict")


# ============================================================================
# Block 3: deserialization — column names and types
# ============================================================================

def test_rows_have_all_columns():
    s = ServerStorage()
    rows = s.load_table("users").get_rows()
    expected = {"id", "name", "age", "city"}
    for i, row in enumerate(rows):
        check(set(row.keys()) == expected,
              f"row {i} has all columns {expected}")


def test_id_is_int():
    s = ServerStorage()
    rows = s.load_table("users").get_rows()
    for row in rows:
        check(isinstance(row["id"], int),
              f"id={row['id']} is int")


def test_name_is_str():
    s = ServerStorage()
    rows = s.load_table("users").get_rows()
    for row in rows:
        check(isinstance(row["name"], str),
              f"name={row['name']!r} is str")


def test_age_is_int():
    s = ServerStorage()
    rows = s.load_table("users").get_rows()
    for row in rows:
        check(isinstance(row["age"], int),
              f"age={row['age']} is int")


def test_city_is_str():
    s = ServerStorage()
    rows = s.load_table("users").get_rows()
    for row in rows:
        check(isinstance(row["city"], str),
              f"city={row['city']!r} is str")


# ============================================================================
# Block 4: deserialization — values are sensible
# ============================================================================

def test_ids_are_positive():
    s = ServerStorage()
    rows = s.load_table("users").get_rows()
    for row in rows:
        check(row["id"] > 0, f"id={row['id']} is positive")


def test_ids_are_unique():
    s = ServerStorage()
    rows = s.load_table("users").get_rows()
    ids = [row["id"] for row in rows]
    check(len(ids) == len(set(ids)), "all ids are unique")


def test_names_are_non_empty():
    s = ServerStorage()
    rows = s.load_table("users").get_rows()
    for row in rows:
        check(len(row["name"].strip()) > 0,
              f"name={row['name']!r} is non-empty")


def test_ages_are_reasonable():
    s = ServerStorage()
    rows = s.load_table("users").get_rows()
    for row in rows:
        check(0 < row["age"] < 150,
              f"age={row['age']} is reasonable")


def test_cities_are_non_empty():
    s = ServerStorage()
    rows = s.load_table("users").get_rows()
    for row in rows:
        check(len(row["city"].strip()) > 0,
              f"city={row['city']!r} is non-empty")


# ============================================================================
# Block 5: multiple tables
# ============================================================================

def test_load_products():
    s = ServerStorage()
    rows = s.load_table("products").get_rows()
    check(len(rows) > 0, f"products has rows (got {len(rows)})")


def test_products_rows_are_dicts():
    s = ServerStorage()
    rows = s.load_table("products").get_rows()
    for i, row in enumerate(rows):
        check(isinstance(row, dict), f"products row {i} is a dict")


def test_users_and_products_different_schemas():
    s = ServerStorage()
    users_keys    = set(s.load_table("users").get_rows()[0].keys())
    products_keys = set(s.load_table("products").get_rows()[0].keys())
    check(users_keys != products_keys,
          "users and products have different schemas")


# ============================================================================
# Block 6: repeated loads — cache warming
# ============================================================================

def test_repeated_load_same_table():
    s = ServerStorage()
    s.reset_metrics()

    rows1 = s.load_table("users").get_rows()
    rows2 = s.load_table("users").get_rows()

    check(len(rows1) == len(rows2),
          "repeated load returns same number of rows")

    m = s.get_metrics()
    check(m.get("hits", 0) > 0,
          f"cache warming: hits > 0 after second load (got {m.get('hits', 0)})")


def test_metrics_after_load():
    s = ServerStorage()
    s.reset_metrics()
    s.load_table("users")
    m = s.get_metrics()
    check("hits"      in m, "metrics has hits after load")
    check("misses"    in m, "metrics has misses after load")
    check("hit_rate"  in m, "metrics has hit_rate after load")
    check(m.get("misses", 0) > 0, "at least one miss on first load")


# ============================================================================
# Block 7: error handling
# ============================================================================

def test_load_nonexistent_table_raises():
    s = ServerStorage()
    try:
        s.load_table("nonexistent_xyz")
        check(False, "load_table nonexistent should raise")
    except RuntimeError:
        check(True, "load_table nonexistent raises RuntimeError")


# ============================================================================
# Block 8: executor integration
# ============================================================================

def test_executor_select_star():
    from planner  import QueryPlanner
    from executor import QueryExecutor
    from parser   import get_parser

    storage  = ServerStorage()
    executor = QueryExecutor(storage)
    planner  = QueryPlanner()
    parser   = get_parser()

    ast  = parser.parse("SELECT * FROM users")
    plan = planner.plan(ast)
    rows = executor.execute(plan)

    check(len(rows) > 0,
          f"SELECT * FROM users returns {len(rows)} rows via TCP")
    check(all("id"   in row for row in rows), "all rows have id column")
    check(all("name" in row for row in rows), "all rows have name column")


def test_executor_select_where():
    from planner  import QueryPlanner
    from executor import QueryExecutor
    from parser   import get_parser

    storage  = ServerStorage()
    executor = QueryExecutor(storage)
    planner  = QueryPlanner()
    parser   = get_parser()

    ast  = parser.parse("SELECT * FROM users WHERE city = 'Madrid'")
    plan = planner.plan(ast)
    rows = executor.execute(plan)

    check(len(rows) > 0,
          f"SELECT WHERE city=Madrid returns {len(rows)} rows")
    check(all(row["city"].strip() == "Madrid" for row in rows),
          "all returned rows have city=Madrid")

# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("=== TEST SERVER STORAGE ===\n")

    print("-- Block 1: connection --")
    test_ping()
    test_ping_wrong_port()

    print("\n-- Block 2: load_table --")
    test_load_table_returns_table()
    test_load_table_has_rows()
    test_load_table_rows_are_dicts()

    print("\n-- Block 3: column names and types --")
    test_rows_have_all_columns()
    test_id_is_int()
    test_name_is_str()
    test_age_is_int()
    test_city_is_str()

    print("\n-- Block 4: values --")
    test_ids_are_positive()
    test_ids_are_unique()
    test_names_are_non_empty()
    test_ages_are_reasonable()
    test_cities_are_non_empty()

    print("\n-- Block 5: multiple tables --")
    test_load_products()
    test_products_rows_are_dicts()
    test_users_and_products_different_schemas()

    print("\n-- Block 6: cache warming --")
    test_repeated_load_same_table()
    test_metrics_after_load()

    print("\n-- Block 7: error handling --")
    test_load_nonexistent_table_raises()

    print("\n-- Block 8: executor integration --")
    test_executor_select_star()
    test_executor_select_where()

    print(f"\n=== RESULT: {tests_passed} passed, {tests_failed} failed, "
          f"{tests_passed + tests_failed} total ===")
