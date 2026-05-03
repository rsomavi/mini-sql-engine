# server/tests/test_update.py
# Requires: server running on localhost:5433
# Run from sql-parse/: python3 server/tests/test_update.py
#
# Tests UPDATE end-to-end, with specific focus on HOT updates (SLOT_REDIRECT):
# after a size-changing UPDATE the SCAN must return exactly the live rows —
# no duplicates from REDIRECT slots being included as normal rows.

import socket
import struct
import time

HOST = "localhost"
PORT  = 5433

passed = 0
failed = 0

# Unique suffix so repeated runs don't collide on CREATE TABLE
TS = str(int(time.time()))


# ============================================================================
# Low-level socket helpers
# ============================================================================

def connect():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    return sock


def recv_line(sock):
    """Read one text line (up to \\n) from socket."""
    buf = b""
    while True:
        c = sock.recv(1)
        if not c:
            raise ConnectionError("server disconnected")
        if c == b"\n":
            return buf.decode("utf-8", errors="replace")
        buf += c


def recv_bytes(sock, n):
    """Read exactly n bytes from socket."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("server disconnected")
        buf += chunk
    return buf


# ============================================================================
# Protocol response readers
# ============================================================================

def _parse_metrics_line(line):
    parts = line.split()
    m = {}
    for part in parts[1:]:
        key, _, val = part.partition("=")
        try:
            m[key] = float(val) if "." in val else int(val)
        except ValueError:
            m[key] = val
    return m


def read_response(sock):
    """
    Read a text-only response (PING, CREATE, INSERT, UPDATE, DELETE, SCHEMA).
    Returns dict: status ("OK"/"ERR"), lines (all non-METRICS/END lines), metrics.
    """
    status_line = recv_line(sock)
    status  = "OK" if status_line.startswith("OK") else "ERR"
    lines   = [status_line]
    metrics = {}

    while True:
        line = recv_line(sock)
        if line == "END":
            break
        if line.startswith("METRICS"):
            metrics = _parse_metrics_line(line)
        else:
            lines.append(line)

    return {"status": status, "lines": lines, "metrics": metrics}


def read_scan_response(sock):
    """
    Read a SCAN response in v1.5 format (no ROWS header):
      OK
      <row_id> <size>
      <binary row data — exactly size bytes, may contain \\n>
      ...
      METRICS ...
      END

    Returns dict: status, rows (list of (row_id, bytes)), metrics.
    """
    status_line = recv_line(sock)
    status  = "OK" if status_line.startswith("OK") else "ERR"
    rows    = []
    metrics = {}

    while True:
        line = recv_line(sock)
        if line == "END":
            break
        if line.startswith("METRICS"):
            metrics = _parse_metrics_line(line)
            continue
        # Row header: two integers separated by a space
        parts = line.split()
        if len(parts) == 2:
            try:
                row_id = int(parts[0])
                size   = int(parts[1])
                data   = recv_bytes(sock, size)
                rows.append((row_id, data))
                continue
            except ValueError:
                pass
        # Unrecognised line — skip silently

    return {"status": status, "rows": rows, "metrics": metrics}


# ============================================================================
# Row serialization / deserialization
# ============================================================================
# Binary format (mirrors storage-engine/schema.c):
#   null_bitmap : ceil(n_cols / 8) bytes
#   per non-NULL column:
#     INT     : 4 bytes little-endian signed
#     FLOAT   : 4 bytes little-endian IEEE-754
#     BOOL    : 1 byte
#     VARCHAR : 2 bytes LE unsigned length + N bytes UTF-8

def serialize_row(col_defs, values):
    """col_defs: list of (name, type_str, max_size, nullable, pk)."""
    n = len(col_defs)
    null_bitmap = bytearray((n + 7) // 8)
    data        = bytearray()

    for i, ((_, col_type, _, _, _), val) in enumerate(zip(col_defs, values)):
        if val is None:
            null_bitmap[i // 8] |= 1 << (i % 8)
            continue
        if col_type == "INT":
            data += struct.pack("<i", val)
        elif col_type == "FLOAT":
            data += struct.pack("<f", val)
        elif col_type == "BOOL":
            data += struct.pack("B", 1 if val else 0)
        elif col_type == "VARCHAR":
            enc = val.encode("utf-8")
            data += struct.pack("<H", len(enc)) + enc

    return bytes(null_bitmap) + bytes(data)


def deserialize_row(col_defs, raw):
    """Returns list of Python values, or None on parse error."""
    n               = len(col_defs)
    bitmap_size     = (n + 7) // 8
    if len(raw) < bitmap_size:
        return None

    offset = bitmap_size
    values = []

    for i, (_, col_type, _, _, _) in enumerate(col_defs):
        is_null = bool(raw[i // 8] & (1 << (i % 8)))
        if is_null:
            values.append(None)
            continue

        if col_type == "INT":
            if offset + 4 > len(raw):
                return None
            values.append(struct.unpack_from("<i", raw, offset)[0])
            offset += 4
        elif col_type == "FLOAT":
            if offset + 4 > len(raw):
                return None
            values.append(struct.unpack_from("<f", raw, offset)[0])
            offset += 4
        elif col_type == "BOOL":
            if offset + 1 > len(raw):
                return None
            values.append(bool(raw[offset]))
            offset += 1
        elif col_type == "VARCHAR":
            if offset + 2 > len(raw):
                return None
            length = struct.unpack_from("<H", raw, offset)[0]
            offset += 2
            if offset + length > len(raw):
                return None
            values.append(raw[offset:offset + length].decode("utf-8", errors="replace"))
            offset += length
        else:
            values.append(None)

    return values


# ============================================================================
# High-level command helpers
# ============================================================================

def cmd_create(table, col_defs):
    col_str = " ".join(f"{n}:{t}:{s}:{nl}:{pk}" for n, t, s, nl, pk in col_defs)
    sock = connect()
    sock.sendall(f"CREATE {table} {col_str}\n".encode())
    resp = read_response(sock)
    sock.close()
    return resp


def cmd_schema(table):
    """Returns list of (name, type_str, max_size, nullable, pk) from SCHEMA response."""
    sock = connect()
    sock.sendall(f"SCHEMA {table}\n".encode())
    resp = read_response(sock)
    sock.close()

    cols = []
    for line in resp["lines"]:
        if line.startswith("OK") or line.startswith("COLUMNS"):
            continue
        parts = line.split(":")
        if len(parts) == 5:
            name, col_type, max_size, nullable, pk = parts
            cols.append((name, col_type, int(max_size), int(nullable), int(pk)))
    return cols


def cmd_insert(table, col_defs, values):
    """Returns row_id (int) or None on error."""
    payload = serialize_row(col_defs, values)
    sock    = connect()
    sock.sendall(f"INSERT {table} {len(payload)}\n".encode())
    sock.sendall(payload)
    resp = read_response(sock)
    sock.close()

    if resp["status"] != "OK":
        return None
    for line in resp["lines"]:
        if line.startswith("ROW_ID"):
            return int(line.split()[1])
    return None


def cmd_update(table, row_id, col_defs, values):
    """Returns number of rows updated (0 or 1)."""
    payload = serialize_row(col_defs, values)
    sock    = connect()
    sock.sendall(f"UPDATE {table} {row_id} {len(payload)}\n".encode())
    sock.sendall(payload)
    resp = read_response(sock)
    sock.close()

    for line in resp["lines"]:
        if line.startswith("UPDATED"):
            return int(line.split()[1])
    return 0


def cmd_delete(table, where_clause):
    """Returns number of rows deleted."""
    sock = connect()
    sock.sendall(f"DELETE {table} WHERE {where_clause}\n".encode())
    resp = read_response(sock)
    sock.close()

    for line in resp["lines"]:
        if line.startswith("DELETED"):
            return int(line.split()[1])
    return 0


def cmd_scan(table, col_defs=None):
    """
    Scans table. If col_defs is provided, deserializes rows.
    Returns dict: status, rows (list of (row_id, bytes)),
                  decoded (list of lists if col_defs given), metrics.
    """
    sock = connect()
    sock.sendall(f"SCAN {table}\n".encode())
    resp = read_scan_response(sock)
    sock.close()

    if col_defs is not None:
        resp["decoded"] = [deserialize_row(col_defs, data) for _, data in resp["rows"]]
    return resp


# ============================================================================
# check helper
# ============================================================================

def check(description, condition):
    global passed, failed
    if condition:
        print(f"  PASS: {description}")
        passed += 1
    else:
        print(f"  FAIL: {description}")
        failed += 1


# ============================================================================
# Block 1 — Basic UPDATE (same size, in-place)
# ============================================================================

print("=== TEST UPDATE ===\n")
print("-- Block 1: basic UPDATE (same size, in-place) --")

TABLE1 = f"tu_b1_{TS}"
COLS1  = [
    ("id",  "INT", 4, 0, 1),
    ("val", "INT", 4, 0, 0),
]

resp = cmd_create(TABLE1, COLS1)
check("CREATE table", resp["status"] == "OK")

rid1 = cmd_insert(TABLE1, COLS1, [1, 100])
rid2 = cmd_insert(TABLE1, COLS1, [2, 200])
rid3 = cmd_insert(TABLE1, COLS1, [3, 300])
check("INSERT 3 rows (all row_ids valid)", None not in (rid1, rid2, rid3))

n_upd = cmd_update(TABLE1, rid2, COLS1, [2, 999])
check("UPDATE returns UPDATED 1", n_upd == 1)

schema1 = cmd_schema(TABLE1)
resp    = cmd_scan(TABLE1, schema1)
check("SCAN returns OK", resp["status"] == "OK")
check("SCAN returns exactly 3 rows", len(resp["rows"]) == 3)

decoded1 = resp["decoded"]
ids1 = [r[0] for r in decoded1 if r]
check("no duplicate ids (no REDIRECT slot leaked into results)", len(ids1) == len(set(ids1)))

updated = next((r for r in decoded1 if r and r[0] == 2), None)
check("updated row present", updated is not None)
check("updated row has new val=999", updated is not None and updated[1] == 999)

others = [r for r in decoded1 if r and r[0] != 2]
check("non-updated rows unchanged", all(r[1] == r[0] * 100 for r in others))


# ============================================================================
# Block 2 — HOT UPDATE (different size, SLOT_REDIRECT)
# ============================================================================

print("\n-- Block 2: HOT UPDATE (different size, SLOT_REDIRECT) --")

TABLE2 = f"tu_b2_{TS}"
COLS2  = [
    ("id",   "INT",     4,  0, 1),
    ("name", "VARCHAR", 50, 0, 0),
]

resp = cmd_create(TABLE2, COLS2)
check("CREATE table", resp["status"] == "OK")

rid_a = cmd_insert(TABLE2, COLS2, [1, "A"])
rid_b = cmd_insert(TABLE2, COLS2, [2, "BB"])
rid_c = cmd_insert(TABLE2, COLS2, [3, "CCC"])
check("INSERT 3 rows with varying VARCHAR lengths", None not in (rid_a, rid_b, rid_c))

# "A" (1 B payload) → "AAAAAAAAAA" (10 B payload): different size forces HOT update
n_upd = cmd_update(TABLE2, rid_a, COLS2, [1, "AAAAAAAAAA"])
check("HOT UPDATE (longer VARCHAR) returns UPDATED 1", n_upd == 1)

schema2 = cmd_schema(TABLE2)
resp    = cmd_scan(TABLE2, schema2)
check("SCAN after HOT UPDATE returns OK", resp["status"] == "OK")
check("SCAN after HOT UPDATE returns exactly 3 rows (REDIRECT slot not duplicated)",
      len(resp["rows"]) == 3)

decoded2 = resp["decoded"]
ids2 = [r[0] for r in decoded2 if r]
check("no duplicate ids after HOT UPDATE", len(ids2) == len(set(ids2)))
check("all 3 logical rows present (1, 2, 3)", sorted(ids2) == [1, 2, 3])

updated2 = next((r for r in decoded2 if r and r[0] == 1), None)
check("HOT-updated row (id=1) is present", updated2 is not None)
check("HOT-updated row has new name='AAAAAAAAAA'",
      updated2 is not None and updated2[1] == "AAAAAAAAAA")

# Chain: REDIRECT → REDIRECT — update same row again with shorter name
n_upd = cmd_update(TABLE2, rid_a, COLS2, [1, "X"])
check("second HOT UPDATE (chain REDIRECT→REDIRECT) returns UPDATED 1", n_upd == 1)

resp    = cmd_scan(TABLE2, schema2)
check("SCAN after chained HOT UPDATE returns OK", resp["status"] == "OK")
check("SCAN after chained HOT UPDATE returns exactly 3 rows (no intermediate REDIRECT leaked)",
      len(resp["rows"]) == 3)

decoded2b = resp["decoded"]
ids2b = [r[0] for r in decoded2b if r]
check("no duplicate ids after chained HOT UPDATE", len(ids2b) == len(set(ids2b)))
check("all 3 logical rows still present (1, 2, 3)", sorted(ids2b) == [1, 2, 3])

final2 = next((r for r in decoded2b if r and r[0] == 1), None)
check("doubly-updated row (id=1) is present", final2 is not None)
check("doubly-updated row has final name='X'",
      final2 is not None and final2[1] == "X")

unchanged2 = {r[0]: r[1] for r in decoded2b if r and r[0] != 1}
check("non-updated rows unaffected after chain",
      unchanged2.get(2) == "BB" and unchanged2.get(3) == "CCC")


# ============================================================================
# Block 3 — UPDATE non-existent row
# ============================================================================

print("\n-- Block 3: UPDATE non-existent row --")

TABLE3 = f"tu_b3_{TS}"
COLS3  = [("id", "INT", 4, 0, 1), ("v", "INT", 4, 1, 0)]
cmd_create(TABLE3, COLS3)
rid_real = cmd_insert(TABLE3, COLS3, [1, 10])
check("setup: INSERT for block 3", rid_real is not None)

# Use a row_id that encodes page 0xDE, slot 0xAD — neither will exist
bogus_row_id = (0xDE << 16) | 0xAD
n_upd = cmd_update(TABLE3, bogus_row_id, COLS3, [99, 99])
check("UPDATE non-existent row_id returns UPDATED 0", n_upd == 0)

schema3 = cmd_schema(TABLE3)
resp    = cmd_scan(TABLE3, schema3)
check("SCAN after failed UPDATE still returns 1 row", len(resp["rows"]) == 1)

surviving = resp["decoded"][0] if resp["decoded"] else None
check("original row untouched after failed UPDATE",
      surviving is not None and surviving[0] == 1 and surviving[1] == 10)


# ============================================================================
# Block 4 — UPDATE then DELETE
# ============================================================================

print("\n-- Block 4: UPDATE then DELETE --")

TABLE4 = f"tu_b4_{TS}"
COLS4  = [
    ("id",   "INT",     4,  0, 1),
    ("name", "VARCHAR", 64, 0, 0),
]

resp = cmd_create(TABLE4, COLS4)
check("CREATE table for block 4", resp["status"] == "OK")

rid4 = cmd_insert(TABLE4, COLS4, [42, "original"])
check("INSERT row for block 4", rid4 is not None)

# HOT update: different-length VARCHAR forces SLOT_REDIRECT
n_upd = cmd_update(TABLE4, rid4, COLS4, [42, "updated_with_a_longer_name"])
check("HOT UPDATE for block 4 returns UPDATED 1", n_upd == 1)

# Verify updated value is visible before DELETE
schema4  = cmd_schema(TABLE4)
resp     = cmd_scan(TABLE4, schema4)
pre_del  = next((r for r in resp["decoded"] if r and r[0] == 42), None)
check("updated value visible before DELETE", pre_del is not None and pre_del[1] == "updated_with_a_longer_name")

# DELETE by column value — scans all slots including REDIRECT
n_del = cmd_delete(TABLE4, "id = 42")
check("DELETE after HOT UPDATE deletes at least 1 row", n_del >= 1)

resp4   = cmd_scan(TABLE4, schema4)
check("SCAN after DELETE returns OK", resp4["status"] == "OK")
remaining = [r for r in resp4["decoded"] if r and r[0] == 42]
check("deleted row (id=42) is gone from SCAN", len(remaining) == 0)


# ============================================================================
# Block 5 — Multiple rows, UPDATE only one
# ============================================================================

print("\n-- Block 5: multiple rows, UPDATE only one --")

TABLE5 = f"tu_b5_{TS}"
COLS5  = [
    ("id",  "INT",     4,  0, 1),
    ("val", "VARCHAR", 64, 0, 0),
]

resp = cmd_create(TABLE5, COLS5)
check("CREATE table for block 5", resp["status"] == "OK")

rids5 = []
for i in range(1, 6):
    r = cmd_insert(TABLE5, COLS5, [i, f"row{i}"])
    rids5.append(r)
check("INSERT 5 rows", None not in rids5)

# Update middle row (id=3) with a longer VARCHAR (HOT update)
n_upd = cmd_update(TABLE5, rids5[2], COLS5, [3, "updated_row3_with_longer_value"])
check("UPDATE middle row (id=3) returns UPDATED 1", n_upd == 1)

schema5 = cmd_schema(TABLE5)
resp5   = cmd_scan(TABLE5, schema5)
check("SCAN returns OK", resp5["status"] == "OK")
check("SCAN returns exactly 5 rows (no REDIRECT duplicates)", len(resp5["rows"]) == 5)

decoded5 = resp5["decoded"]
ids5     = [r[0] for r in decoded5 if r]
check("all 5 distinct ids present", sorted(ids5) == [1, 2, 3, 4, 5])
check("no duplicate ids", len(ids5) == len(set(ids5)))

updated5 = next((r for r in decoded5 if r and r[0] == 3), None)
check("updated row (id=3) present", updated5 is not None)
check("updated row has new value",
      updated5 is not None and updated5[1] == "updated_row3_with_longer_value")

unchanged5 = {r[0]: r[1] for r in decoded5 if r and r[0] != 3}
check("all other 4 rows have original values",
      all(unchanged5.get(i) == f"row{i}" for i in [1, 2, 4, 5]))


# ============================================================================
# Final result
# ============================================================================

print(f"\n=== RESULT: {passed} passed, {failed} failed, {passed + failed} total ===")
