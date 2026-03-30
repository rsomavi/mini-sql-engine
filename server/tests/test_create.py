#!/usr/bin/env python3
# test_create.py - Integration tests for CREATE TABLE operation
# Requires: server running on localhost:5433
# Run from sql-parse/server/tests/: python3 test_create.py

import socket
import os

HOST = "localhost"
PORT = 5433

# ============================================================================
# Client helpers
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
    status_line = recv_line(sock)
    status  = "OK" if status_line.startswith("OK") else "ERR"
    err_msg = status_line if status == "ERR" else ""

    lines   = []
    columns = []
    metrics = {}

    while True:
        line = recv_line(sock)

        if line == "END":
            break

        if line.startswith("METRICS "):
            for part in line.split()[1:]:
                key, val = part.split("=")
                try:
                    metrics[key] = float(val) if "." in val else int(val)
                except ValueError:
                    metrics[key] = val
            continue

        if line.startswith("COLUMNS "):
            col_count = int(line.split()[1])
            for _ in range(col_count):
                col_line = recv_line(sock)
                parts = col_line.split(":")
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


def create_table(name, col_defs):
    """Send CREATE <name> <col_defs> and return response.
    col_defs: only column definitions, NO table name prefix.
    Example: create_table("users", "id:INT:4:0:1 name:VARCHAR:50:0:0")
    """
    sock = connect()
    send_command(sock, f"CREATE {name} {col_defs}")
    resp = read_response(sock)
    sock.close()
    return resp


def schema_table(name):
    sock = connect()
    send_command(sock, f"SCHEMA {name}")
    resp = read_response(sock)
    sock.close()
    return resp


def drop_if_exists(name):
    path = f"../data/{name}.db"
    if os.path.exists(path):
        os.remove(path)


# ============================================================================
# Test infrastructure
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
# Block 1: basic CREATE
# ============================================================================

def test_create_returns_ok():
    drop_if_exists("t_basic")
    resp = create_table("t_basic", "id:INT:4:0:1")
    check(resp["status"] == "OK", "CREATE returns OK")
    drop_if_exists("t_basic")


def test_create_returns_created_line():
    drop_if_exists("t_created")
    resp = create_table("t_created", "id:INT:4:0:1")
    check(any("CREATED" in l and "t_created" in l for l in resp["lines"]),
          "CREATE returns CREATED <table_name> line")
    drop_if_exists("t_created")


def test_create_has_metrics():
    drop_if_exists("t_metrics")
    resp = create_table("t_metrics", "id:INT:4:0:1")
    m = resp["metrics"]
    check("hits"     in m, "CREATE response has hits")
    check("misses"   in m, "CREATE response has misses")
    check("hit_rate" in m, "CREATE response has hit_rate")
    drop_if_exists("t_metrics")


# ============================================================================
# Block 2: schema round-trip
# ============================================================================

def test_schema_after_create_int():
    drop_if_exists("t_int")
    create_table("t_int", "id:INT:4:0:1")
    resp = schema_table("t_int")
    check(resp["status"] == "OK", "SCHEMA after CREATE returns OK")
    cols = {c["name"]: c for c in resp["columns"]}
    check("id" in cols,                "id column exists")
    check(cols["id"]["type"] == "INT", "id type is INT")
    check(cols["id"]["max_size"] == 4, "id max_size is 4")
    check(cols["id"]["pk"] == 1,       "id is primary key")
    check(cols["id"]["nullable"] == 0, "id is not nullable")
    drop_if_exists("t_int")


def test_schema_after_create_varchar():
    drop_if_exists("t_varchar")
    create_table("t_varchar", "name:VARCHAR:50:0:0")
    resp = schema_table("t_varchar")
    cols = {c["name"]: c for c in resp["columns"]}
    check("name" in cols,                    "name column exists")
    check(cols["name"]["type"] == "VARCHAR", "name type is VARCHAR")
    check(cols["name"]["max_size"] == 50,    "name max_size is 50")
    check(cols["name"]["pk"] == 0,           "name is not pk")
    drop_if_exists("t_varchar")


def test_schema_after_create_float():
    drop_if_exists("t_float")
    create_table("t_float", "price:FLOAT:4:1:0")
    resp = schema_table("t_float")
    cols = {c["name"]: c for c in resp["columns"]}
    check("price" in cols,                  "price column exists")
    check(cols["price"]["type"] == "FLOAT", "price type is FLOAT")
    check(cols["price"]["nullable"] == 1,   "price is nullable")
    drop_if_exists("t_float")


def test_schema_after_create_bool():
    drop_if_exists("t_bool")
    create_table("t_bool", "active:BOOL:1:1:0")
    resp = schema_table("t_bool")
    cols = {c["name"]: c for c in resp["columns"]}
    check("active" in cols,                 "active column exists")
    check(cols["active"]["type"] == "BOOL", "active type is BOOL")
    drop_if_exists("t_bool")


def test_schema_multi_column():
    drop_if_exists("t_multi")
    create_table("t_multi",
        "id:INT:4:0:1 name:VARCHAR:100:0:0 age:INT:4:1:0 active:BOOL:1:1:0 score:FLOAT:4:1:0")
    resp = schema_table("t_multi")
    check(len(resp["columns"]) == 5, f"5 columns (got {len(resp['columns'])})")
    names = [c["name"] for c in resp["columns"]]
    check("id"     in names, "id column present")
    check("name"   in names, "name column present")
    check("age"    in names, "age column present")
    check("active" in names, "active column present")
    check("score"  in names, "score column present")
    drop_if_exists("t_multi")


def test_schema_column_order_preserved():
    drop_if_exists("t_order")
    create_table("t_order", "a:INT:4:0:1 b:VARCHAR:50:0:0 c:FLOAT:4:1:0")
    resp = schema_table("t_order")
    names = [c["name"] for c in resp["columns"]]
    check(names == ["a", "b", "c"], f"column order preserved: {names}")
    drop_if_exists("t_order")


def test_schema_exactly_one_pk():
    drop_if_exists("t_pk")
    create_table("t_pk", "id:INT:4:0:1 name:VARCHAR:50:0:0 age:INT:4:1:0")
    resp = schema_table("t_pk")
    pks = [c for c in resp["columns"] if c["pk"] == 1]
    check(len(pks) == 1,          "exactly one primary key")
    check(pks[0]["name"] == "id", "primary key is 'id'")
    drop_if_exists("t_pk")


def test_varchar_large_max_size():
    drop_if_exists("t_bigvarchar")
    create_table("t_bigvarchar", "bio:VARCHAR:500:1:0")
    resp = schema_table("t_bigvarchar")
    cols = {c["name"]: c for c in resp["columns"]}
    check(cols["bio"]["max_size"] == 500, "VARCHAR max_size=500 preserved")
    drop_if_exists("t_bigvarchar")


# ============================================================================
# Block 3: error handling
# ============================================================================

def test_create_table_exists():
    drop_if_exists("t_exists")
    create_table("t_exists", "id:INT:4:0:1")
    resp = create_table("t_exists", "id:INT:4:0:1")
    check(resp["status"] == "ERR",          "duplicate CREATE returns ERR")
    check("TABLE_EXISTS" in resp["err_msg"], "error code is TABLE_EXISTS")
    drop_if_exists("t_exists")


def test_create_missing_table_name():
    sock = connect()
    send_command(sock, "CREATE")
    resp = read_response(sock)
    sock.close()
    check(resp["status"] == "ERR", "CREATE with no args returns ERR")


def test_create_no_columns():
    drop_if_exists("t_nocols")
    resp = create_table("t_nocols", "")
    check(resp["status"] == "ERR", "CREATE with no columns returns ERR")
    drop_if_exists("t_nocols")


def test_create_unknown_type():
    drop_if_exists("t_badtype")
    resp = create_table("t_badtype", "id:BIGINT:8:0:1")
    check(resp["status"] == "ERR", "CREATE with unknown type returns ERR")
    drop_if_exists("t_badtype")


# ============================================================================
# Block 4: persistence
# ============================================================================

def test_schema_survives_reconnect():
    drop_if_exists("t_persist")
    create_table("t_persist", "id:INT:4:0:1 city:VARCHAR:30:0:0")
    resp = schema_table("t_persist")
    check(resp["status"] == "OK",    "schema readable on new connection")
    check(len(resp["columns"]) == 2, "2 columns after reconnect")
    drop_if_exists("t_persist")


def test_schema_correct_after_multiple_creates():
    drop_if_exists("t_aa")
    drop_if_exists("t_bb")
    create_table("t_aa", "x:INT:4:0:1")
    create_table("t_bb", "y:VARCHAR:64:0:1 z:FLOAT:4:1:0")

    resp_aa = schema_table("t_aa")
    resp_bb = schema_table("t_bb")

    names_aa = [c["name"] for c in resp_aa["columns"]]
    names_bb = [c["name"] for c in resp_bb["columns"]]

    check(names_aa == ["x"],      f"t_aa has only x: {names_aa}")
    check(names_bb == ["y", "z"], f"t_bb has y,z: {names_bb}")
    drop_if_exists("t_aa")
    drop_if_exists("t_bb")


# ============================================================================
# Block 5: regression
# ============================================================================

def test_ping_works_after_create():
    drop_if_exists("t_ping")
    create_table("t_ping", "id:INT:4:0:1")
    sock = connect()
    send_command(sock, "PING")
    resp = read_response(sock)
    sock.close()
    check(resp["status"] == "OK",  "PING works after CREATE")
    check("PONG" in resp["lines"], "PING returns PONG after CREATE")
    drop_if_exists("t_ping")


def test_scan_users_unaffected_by_create():
    drop_if_exists("t_scan")
    create_table("t_scan", "id:INT:4:0:1 val:VARCHAR:20:1:0")
    sock = connect()
    send_command(sock, "SCAN users")
    resp = read_response(sock)
    sock.close()
    check(resp["status"] == "OK", "SCAN users unaffected by CREATE")
    drop_if_exists("t_scan")


def test_schema_users_unaffected_by_create():
    drop_if_exists("t_schtest")
    create_table("t_schtest", "id:INT:4:0:1")
    resp = schema_table("users")
    names = [c["name"] for c in resp["columns"]]
    check("id"   in names, "users.id unaffected")
    check("name" in names, "users.name unaffected")
    check("age"  in names, "users.age unaffected")
    check("city" in names, "users.city unaffected")
    drop_if_exists("t_schtest")


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("=== TEST CREATE TABLE ===\n")

    print("-- Block 1: basic CREATE --")
    test_create_returns_ok()
    test_create_returns_created_line()
    test_create_has_metrics()

    print("\n-- Block 2: schema round-trip --")
    test_schema_after_create_int()
    test_schema_after_create_varchar()
    test_schema_after_create_float()
    test_schema_after_create_bool()
    test_schema_multi_column()
    test_schema_column_order_preserved()
    test_schema_exactly_one_pk()
    test_varchar_large_max_size()

    print("\n-- Block 3: error handling --")
    test_create_table_exists()
    test_create_missing_table_name()
    test_create_no_columns()
    test_create_unknown_type()

    print("\n-- Block 4: persistence --")
    test_schema_survives_reconnect()
    test_schema_correct_after_multiple_creates()

    print("\n-- Block 5: regression --")
    test_ping_works_after_create()
    test_scan_users_unaffected_by_create()
    test_schema_users_unaffected_by_create()

    print(f"\n=== RESULT: {tests_passed} passed, {tests_failed} failed, "
          f"{tests_passed + tests_failed} total ===")
