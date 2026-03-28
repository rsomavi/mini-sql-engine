#!/usr/bin/env python3
# test_schema.py - Integration test for SCHEMA operation
# Requires: server running on localhost:5433
# Run: python3 test_schema.py

import socket

HOST = "localhost"
PORT = 5433

# ============================================================================
# Client helpers (same as test_scan.py)
# ============================================================================

def connect():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    return sock


def send_command(sock, cmd):
    sock.sendall((cmd + "\n").encode())


def recv_line(sock):
    buf = b""
    while True:
        c = sock.recv(1)
        if not c:
            raise ConnectionError("server disconnected")
        if c == b"\n":
            return buf.decode()
        buf += c


def read_response(sock):
    """
    Read a full server response.
    Returns dict with:
      - status: "OK" or "ERR"
      - lines: list of text lines before METRICS
      - columns: list of dicts (for SCHEMA responses)
      - metrics: dict with hits, misses, evictions, hit_rate
    """
    status_line = recv_line(sock)
    status = "OK" if status_line.startswith("OK") else "ERR"
    err_msg = status_line if status == "ERR" else ""

    lines   = []
    columns = []
    metrics = {}

    while True:
        line = recv_line(sock)

        if line.startswith("END"):
            break

        if line.startswith("METRICS"):
            parts = line.split()
            for part in parts[1:]:
                key, val = part.split("=")
                try:
                    metrics[key] = float(val) if "." in val else int(val)
                except ValueError:
                    metrics[key] = val
            continue

        if line.startswith("COLUMNS"):
            col_count = int(line.split()[1])
            for _ in range(col_count):
                col_line = recv_line(sock)
                parts = col_line.split(":")
                # format: name:type:max_size:nullable:pk
                columns.append({
                    "name":     parts[0],
                    "type":     parts[1],
                    "max_size": int(parts[2]),
                    "nullable": int(parts[3]),
                    "pk":       int(parts[4]),
                })
            continue

        lines.append(line)

    return {
        "status":  status,
        "err_msg": err_msg,
        "lines":   lines,
        "columns": columns,
        "metrics": metrics,
    }


# ============================================================================
# Tests
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
# Block 1: basic response
# ============================================================================

def test_schema_returns_ok():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    check(resp["status"] == "OK", "SCHEMA users returns OK")
    sock.close()


def test_schema_returns_columns():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    check(len(resp["columns"]) > 0,
          f"SCHEMA users returns columns (got {len(resp['columns'])})")
    sock.close()


def test_schema_has_metrics():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    m = resp["metrics"]
    check("hits"      in m, "metrics contains hits")
    check("misses"    in m, "metrics contains misses")
    check("evictions" in m, "metrics contains evictions")
    check("hit_rate"  in m, "metrics contains hit_rate")
    sock.close()


# ============================================================================
# Block 2: column format
# ============================================================================

def test_schema_column_has_all_fields():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    for col in resp["columns"]:
        check("name"     in col, f"column has name field")
        check("type"     in col, f"column has type field")
        check("max_size" in col, f"column has max_size field")
        check("nullable" in col, f"column has nullable field")
        check("pk"       in col, f"column has pk field")
    sock.close()


def test_schema_column_types_valid():
    valid_types = {"INT", "FLOAT", "BOOL", "VARCHAR"}
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    for col in resp["columns"]:
        check(col["type"] in valid_types,
              f"column {col['name']} has valid type (got {col['type']})")
    sock.close()


def test_schema_nullable_is_0_or_1():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    for col in resp["columns"]:
        check(col["nullable"] in (0, 1),
              f"column {col['name']} nullable is 0 or 1")
    sock.close()


def test_schema_pk_is_0_or_1():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    for col in resp["columns"]:
        check(col["pk"] in (0, 1),
              f"column {col['name']} pk is 0 or 1")
    sock.close()


def test_schema_exactly_one_pk():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    pk_cols = [c for c in resp["columns"] if c["pk"] == 1]
    check(len(pk_cols) >= 1, f"at least one primary key column exists")
    sock.close()


def test_schema_int_max_size():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    for col in resp["columns"]:
        if col["type"] == "INT":
            check(col["max_size"] == 4,
                  f"INT column {col['name']} has max_size=4")
    sock.close()


def test_schema_varchar_max_size_positive():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    for col in resp["columns"]:
        if col["type"] == "VARCHAR":
            check(col["max_size"] > 0,
                  f"VARCHAR column {col['name']} has max_size > 0 (got {col['max_size']})")
    sock.close()


# ============================================================================
# Block 3: users schema specifically
# ============================================================================

def test_schema_users_has_4_columns():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    check(len(resp["columns"]) == 4,
          f"users has 4 columns (got {len(resp['columns'])})")
    sock.close()


def test_schema_users_column_names():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    names = [c["name"] for c in resp["columns"]]
    check("id"   in names, "users has column 'id'")
    check("name" in names, "users has column 'name'")
    check("age"  in names, "users has column 'age'")
    check("city" in names, "users has column 'city'")
    sock.close()


def test_schema_users_id_is_pk():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    id_col = next((c for c in resp["columns"] if c["name"] == "id"), None)
    check(id_col is not None,     "column 'id' exists")
    check(id_col["pk"] == 1,      "column 'id' is primary key")
    check(id_col["type"] == "INT","column 'id' is INT")
    sock.close()


def test_schema_users_name_is_varchar():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp = read_response(sock)
    name_col = next((c for c in resp["columns"] if c["name"] == "name"), None)
    check(name_col is not None,           "column 'name' exists")
    check(name_col["type"] == "VARCHAR",  "column 'name' is VARCHAR")
    check(name_col["max_size"] > 0,       "column 'name' has max_size > 0")
    sock.close()


# ============================================================================
# Block 4: multiple tables
# ============================================================================

def test_schema_products_returns_ok():
    sock = connect()
    send_command(sock, "SCHEMA products")
    resp = read_response(sock)
    check(resp["status"] == "OK", "SCHEMA products returns OK")
    check(len(resp["columns"]) > 0, "products has columns")
    sock.close()


def test_schema_different_tables_different_columns():
    sock = connect()
    send_command(sock, "SCHEMA users")
    resp_users = read_response(sock)
    sock.close()

    sock = connect()
    send_command(sock, "SCHEMA products")
    resp_products = read_response(sock)
    sock.close()

    names_users    = {c["name"] for c in resp_users["columns"]}
    names_products = {c["name"] for c in resp_products["columns"]}
    check(names_users != names_products,
          "users and products have different column sets")


# ============================================================================
# Block 5: error handling
# ============================================================================

def test_schema_unknown_table_returns_err():
    sock = connect()
    send_command(sock, "SCHEMA nonexistent_xyz")
    resp = read_response(sock)
    check(resp["status"] == "ERR",
          "SCHEMA nonexistent table returns ERR")
    sock.close()


def test_schema_missing_table_name_returns_err():
    sock = connect()
    send_command(sock, "SCHEMA")
    resp = read_response(sock)
    check(resp["status"] == "ERR",
          "SCHEMA with no table name returns ERR")
    sock.close()


# ============================================================================
# Block 6: regression
# ============================================================================

def test_ping_works_after_schema():
    sock = connect()
    send_command(sock, "SCHEMA users")
    read_response(sock)
    sock.close()

    sock = connect()
    send_command(sock, "PING")
    resp = read_response(sock)
    check(resp["status"] == "OK", "PING works after SCHEMA")
    sock.close()


def test_scan_works_after_schema():
    sock = connect()
    send_command(sock, "SCHEMA users")
    read_response(sock)
    sock.close()

    sock = connect()
    send_command(sock, "SCAN users")
    resp = read_response(sock)
    check(resp["status"] == "OK", "SCAN works after SCHEMA")
    sock.close()


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("=== TEST SCHEMA ===\n")

    print("-- Block 1: basic response --")
    test_schema_returns_ok()
    test_schema_returns_columns()
    test_schema_has_metrics()

    print("\n-- Block 2: column format --")
    test_schema_column_has_all_fields()
    test_schema_column_types_valid()
    test_schema_nullable_is_0_or_1()
    test_schema_pk_is_0_or_1()
    test_schema_exactly_one_pk()
    test_schema_int_max_size()
    test_schema_varchar_max_size_positive()

    print("\n-- Block 3: users schema --")
    test_schema_users_has_4_columns()
    test_schema_users_column_names()
    test_schema_users_id_is_pk()
    test_schema_users_name_is_varchar()

    print("\n-- Block 4: multiple tables --")
    test_schema_products_returns_ok()
    test_schema_different_tables_different_columns()

    print("\n-- Block 5: error handling --")
    test_schema_unknown_table_returns_err()
    test_schema_missing_table_name_returns_err()

    print("\n-- Block 6: regression --")
    test_ping_works_after_schema()
    test_scan_works_after_schema()

    print(f"\n=== RESULT: {tests_passed} passed, {tests_failed} failed, "
          f"{tests_passed + tests_failed} total ===")
