# storage_server.py — TCP-based storage backend
# Replaces DiskStorage by communicating with the C server via MINIDBMS-RESP.
#
# The Python executor calls load_table(name) exactly as before.
# Internally this class:
#   1. Sends SCHEMA <table> → gets column definitions
#   2. Sends SCAN <table>   → gets binary row data
#   3. Deserializes each row using the schema
#   4. Returns a Table object with a list of dicts

import socket
import struct
import math

from table import Table


# ============================================================================
# ServerStorage
# ============================================================================

class ServerStorage:
    """TCP-based storage implementation using MINIDBMS-RESP protocol."""

    def __init__(self, host: str = "localhost", port: int = 5433):
        self.host = host
        self.port = port

    # =========================================================================
    # Public API — same interface as MemoryStorage and DiskStorage
    # =========================================================================

    def load_table(self, table_name: str) -> Table:
        """
        Load all rows from a table via the C server.

        Sends SCHEMA + SCAN, deserializes binary rows, returns Table.
        """
        columns = self._fetch_schema(table_name)
        rows    = self._fetch_rows(table_name, columns)
        return Table(table_name, rows)

    # =========================================================================
    # Schema — SCHEMA <table_name>
    # =========================================================================

    def _fetch_schema(self, table_name: str) -> list:
        """
        Send SCHEMA <table_name> and parse the column definitions.

        Returns list of dicts:
          [{"name": "id", "type": "INT", "max_size": 4,
            "nullable": 0, "pk": 1}, ...]
        """
        sock = self._connect()
        try:
            self._send(sock, f"SCHEMA {table_name}")
            resp = self._read_response(sock)
        finally:
            sock.close()

        if resp["status"] == "ERR":
            raise RuntimeError(
                f"SCHEMA {table_name} failed: {resp.get('err_line', '')}"
            )

        return resp["columns"]

    # =========================================================================
    # Rows — SCAN <table_name>
    # =========================================================================

    def _fetch_rows(self, table_name: str, columns: list) -> list:
        """
        Send SCAN <table_name>, read binary rows, deserialize each one.

        Returns list of row dicts.
        """
        sock = self._connect()
        try:
            self._send(sock, f"SCAN {table_name}")
            resp = self._read_response(sock, columns=columns)
        finally:
            sock.close()

        if resp["status"] == "ERR":
            raise RuntimeError(
                f"SCAN {table_name} failed: {resp.get('err_line', '')}"
            )

        return resp["rows"]

    # =========================================================================
    # Deserialization — binary row → dict
    # =========================================================================

    def _deserialize_row(self, data: bytes, columns: list) -> dict:
        """
        Convert a binary row (serialized by schema.c row_serialize) to a dict.

        Binary format:
          [null_bitmap: ceil(n_cols/8) bytes]
          per column (if not NULL):
            INT:     4 bytes little-endian int32
            FLOAT:   4 bytes little-endian float32
            BOOL:    1 byte
            VARCHAR: 2 bytes little-endian uint16 (length) + N bytes UTF-8
        """
        n_cols          = len(columns)
        null_bitmap_len = math.ceil(n_cols / 8)
        offset          = 0

        # Read null bitmap
        null_bitmap = data[offset: offset + null_bitmap_len]
        offset     += null_bitmap_len

        def is_null(i):
            byte_idx = i // 8
            bit_idx  = i % 8
            return bool(null_bitmap[byte_idx] & (1 << bit_idx))

        row = {}
        for i, col in enumerate(columns):
            name     = col["name"]
            col_type = col["type"]

            if is_null(i):
                row[name] = None
                continue

            if col_type == "INT":
                val     = struct.unpack_from("<i", data, offset)[0]
                offset += 4
                row[name] = val

            elif col_type == "FLOAT":
                # schema.c uses float (4 bytes), not double
                val     = struct.unpack_from("<f", data, offset)[0]
                offset += 4
                row[name] = val

            elif col_type == "BOOL":
                val     = struct.unpack_from("B", data, offset)[0]
                offset += 1
                row[name] = bool(val)

            elif col_type == "VARCHAR":
                # 2-byte length prefix (unsigned short)
                length  = struct.unpack_from("<H", data, offset)[0]
                offset += 2
                val     = data[offset: offset + length].decode("utf-8", errors="replace")
                offset += length
                row[name] = val

            else:
                raise ValueError(f"Unknown column type: {col_type}")

        return row

    # =========================================================================
    # Protocol I/O
    # =========================================================================

    def _connect(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))
        return sock

    def _send(self, sock: socket.socket, cmd: str):
        sock.sendall((cmd + "\n").encode())

    def _recv_line(self, sock: socket.socket) -> str:
        """Read one line from socket (terminated by \\n)."""
        buf = b""
        while True:
            c = sock.recv(1)
            if not c:
                raise ConnectionError("server disconnected")
            if c == b"\n":
                return buf.decode("utf-8", errors="replace")
            buf += c

    def _recv_bytes(self, sock: socket.socket, n: int) -> bytes:
        """Read exactly n bytes from socket."""
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("server disconnected")
            buf += chunk
        return buf

    def _read_response(self, sock: socket.socket,
                       columns: list = None) -> dict:
        """
        Read a complete server response up to END\\n.

        Returns dict:
          status:   "OK" or "ERR"
          err_line: full ERR line if status == "ERR"
          columns:  list of column dicts (for SCHEMA responses)
          rows:     list of row dicts (for SCAN responses)
          metrics:  dict with hits, misses, evictions, hit_rate
        """
        status_line = self._recv_line(sock)
        if status_line.startswith("OK"):
            status   = "OK"
            err_line = ""
        else:
            status   = "ERR"
            err_line = status_line

        result_columns      = []
        result_rows         = []
        result_row_ids      = []
        result_trace_events = []
        metrics             = {}
        lines               = []

        while True:
            line = self._recv_line(sock)

            # ---- END marker ----
            if line == "END":
                break

            # ---- METRICS line ----
            if line.startswith("METRICS "):
                for part in line.split()[1:]:
                    key, val = part.split("=")
                    try:
                        metrics[key] = float(val) if "." in val else int(val)
                    except ValueError:
                        metrics[key] = val
                continue

            # ---- COLUMNS (SCHEMA response) ----
            if line.startswith("COLUMNS "):
                col_count = int(line.split()[1])
                for _ in range(col_count):
                    col_line = self._recv_line(sock)
                    parts    = col_line.split(":")
                    result_columns.append({
                        "name":     parts[0],
                        "type":     parts[1],
                        "max_size": int(parts[2]),
                        "nullable": int(parts[3]),
                        "pk":       int(parts[4]),
                    })
                continue

            # ---- TRACE_EVENTS (TRACE_STOP response) ----
            if line.startswith("TRACE_EVENTS "):
                event_count = int(line.split()[1])
                for _ in range(event_count):
                    ev_line = self._recv_line(sock)
                    parts   = ev_line.split()
                    if len(parts) == 5:
                        result_trace_events.append({
                            "timestamp": int(parts[0]),
                            "table":     parts[1],
                            "page_id":   int(parts[2]),
                            "hit":       int(parts[3]),
                            "frame_id":  int(parts[4]),
                        })
                continue

            # ---- ROW (SCAN response) — line is "<row_id> <size>" ----
            if columns is not None:
                try:
                    parts = line.split()
                    if len(parts) == 2:
                        row_id_val = int(parts[0])
                        row_size   = int(parts[1])
                    elif len(parts) == 1:
                        row_id_val = -1
                        row_size   = int(parts[0])
                    else:
                        raise ValueError
                    row_data = self._recv_bytes(sock, row_size)
                    result_rows.append(self._deserialize_row(row_data, columns))
                    result_row_ids.append(row_id_val)
                    continue
                except (ValueError, IndexError):
                    pass

            lines.append(line)

        return {
            "status":        status,
            "err_line":      err_line,
            "columns":       result_columns,
            "rows":          result_rows,
            "row_ids":       result_row_ids,
            "trace_events":  result_trace_events,
            "metrics":       metrics,
            "lines":         lines,
        }

    # =========================================================================
    # Metrics access (optional — for monitor)
    # =========================================================================

    def get_metrics(self) -> dict:
        """
        Send METRICS command and return current buffer pool metrics.
        """
        sock = self._connect()
        try:
            self._send(sock, "METRICS")
            resp = self._read_response(sock)
        finally:
            sock.close()
        return resp.get("metrics", {})

    def reset_metrics(self) -> dict:
        """Send RESET_METRICS to the server. Returns the zeroed metrics dict."""
        sock = self._connect()
        try:
            self._send(sock, "RESET_METRICS")
            resp = self._read_response(sock)
        finally:
            sock.close()
        if resp["status"] != "OK":
            raise RuntimeError(f"RESET_METRICS failed: {resp.get('err_line', '')}")
        return resp.get("metrics", {})

    def trace_start(self) -> None:
        """Send TRACE_START — clear any existing trace and begin recording."""
        sock = self._connect()
        try:
            self._send(sock, "TRACE_START")
            resp = self._read_response(sock)
        finally:
            sock.close()
        if resp["status"] != "OK":
            raise RuntimeError(f"TRACE_START failed: {resp.get('err_line', '')}")

    def trace_stop(self) -> list:
        """Send TRACE_STOP — stop recording and return all events.

        Returns a list of dicts:
          {"timestamp": int, "table": str, "page_id": int,
           "hit": int, "frame_id": int}
        """
        sock = self._connect()
        try:
            self._send(sock, "TRACE_STOP")
            resp = self._read_response(sock)
        finally:
            sock.close()
        if resp["status"] != "OK":
            raise RuntimeError(f"TRACE_STOP failed: {resp.get('err_line', '')}")
        return resp.get("trace_events", [])

    def trace_clear(self) -> None:
        """Send TRACE_CLEAR — stop recording and discard all events."""
        sock = self._connect()
        try:
            self._send(sock, "TRACE_CLEAR")
            resp = self._read_response(sock)
        finally:
            sock.close()
        if resp["status"] != "OK":
            raise RuntimeError(f"TRACE_CLEAR failed: {resp.get('err_line', '')}")

    def ping(self) -> bool:
        """Returns True if server is alive."""
        try:
            sock = self._connect()
            self._send(sock, "PING")
            resp = self._read_response(sock)
            sock.close()
            return resp["status"] == "OK"
        except Exception:
            return False

    def create_table(self, table_name: str, columns: list):
        """
        Send CREATE <table_name> <col1:type:max_size:nullable:pk> ...
        columns: list of ColumnDef from ast_nodes
        """
        col_parts = []
        for col in columns:
            nullable = 0 if not col.nullable else 1
            pk       = 1 if col.primary_key else 0
            col_parts.append(
                f"{col.name}:{col.col_type}:{col.max_size}:{nullable}:{pk}"
            )

        cmd = f"CREATE {table_name} " + " ".join(col_parts)
        
        sock = self._connect()
        try:
            self._send(sock, cmd)
            resp = self._read_response(sock)
        finally:
            sock.close()

        if resp["status"] == "ERR":
            raise RuntimeError(
                f"CREATE {table_name} failed: {resp.get('err_line', '')}"
            )

        return resp

    def insert_row(self, table_name: str, columns: list, values: list) -> int:
        """
        Serialize a row and send INSERT <table> <size>\n<bytes>.
        columns: list of column names  ["id", "nombre", "salario"]
        values:  list of values        [1, "Juan", 2500.0]
        Returns row_id assigned by the server.
        """
        # 1. Get schema to know column types and order
        schema_cols = self._fetch_schema(table_name)

        # 2. Build value dict: column_name -> value
        if columns is None:
            col_names = [col["name"] for col in schema_cols]
        else:
            col_names = columns
        val_dict = dict(zip(col_names, values))

        # 3. Serialize row in binary (same format as schema.c row_serialize)
        row_bytes = self._serialize_row(schema_cols, val_dict)

        # 4. Send INSERT <table> <size>\n<bytes>
        sock = self._connect()
        try:
            header = f"INSERT {table_name} {len(row_bytes)}\n"
            sock.sendall(header.encode())
            sock.sendall(row_bytes)
            resp = self._read_response(sock)
        finally:
            sock.close()

        if resp["status"] == "ERR":
            raise RuntimeError(
                f"INSERT {table_name} failed: {resp.get('err_line', '')}"
            )

        # Parse ROW_ID from response lines
        for line in resp["lines"]:
            if line.startswith("ROW_ID "):
                return int(line.split()[1])
        return -1

    def delete_rows(self, table_name: str, where) -> int:
        """
        Send DELETE <table> [WHERE <condition>] to the server.
        Returns the number of rows deleted.
        """
        from ast_nodes import Condition, LogicalCondition, NotCondition

        def serialize_condition(cond) -> str:
            if cond is None:
                return ""
            if isinstance(cond, Condition):
                return f"{cond.column} {cond.operator} {repr(cond.value)}"
            if isinstance(cond, LogicalCondition):
                left  = serialize_condition(cond.left)
                right = serialize_condition(cond.right)
                return f"({left}) {cond.operator.upper()} ({right})"
            if isinstance(cond, NotCondition):
                return f"NOT ({serialize_condition(cond.condition)})"
            return ""

        where_str = serialize_condition(where)
        if where_str:
            cmd = f"DELETE {table_name} WHERE {where_str}"
        else:
            cmd = f"DELETE {table_name}"

        sock = self._connect()
        try:
            self._send(sock, cmd)
            resp = self._read_response(sock)
        finally:
            sock.close()

        if resp["status"] == "ERR":
            raise RuntimeError(
                f"DELETE {table_name} failed: {resp.get('err_line', '')}"
            )

        for line in resp["lines"]:
            if line.startswith("DELETED "):
                return int(line.split()[1])
        return 0

    def _serialize_row(self, schema_cols: list, val_dict: dict) -> bytes:
        """
        Serialize a row dict to binary using schema.c row_serialize format:
        [null_bitmap: ceil(n_cols/8) bytes]
        per column (if not NULL):
            INT:     4 bytes little-endian int32
            FLOAT:   4 bytes little-endian float32  ← 4 bytes, not 8
            BOOL:    1 byte
            VARCHAR: 2 bytes uint16 (length) + N bytes UTF-8
        """
        n_cols          = len(schema_cols)
        null_bitmap_len = math.ceil(n_cols / 8)

        # Build null bitmap
        null_bitmap = bytearray(null_bitmap_len)
        for i, col in enumerate(schema_cols):
            val = val_dict.get(col["name"])
            if val is None:
                null_bitmap[i // 8] |= (1 << (i % 8))

        # Serialize each column
        data = bytearray()
        for i, col in enumerate(schema_cols):
            val      = val_dict.get(col["name"])
            col_type = col["type"]

            if val is None:
                continue  # NULL — already marked in bitmap

            if col_type == "INT":
                data += struct.pack("<i", int(val))

            elif col_type == "FLOAT":
                data += struct.pack("<f", float(val))  # 4 bytes float32

            elif col_type == "BOOL":
                data += struct.pack("B", 1 if val else 0)

            elif col_type == "VARCHAR":
                encoded = str(val).encode("utf-8")
                max_size = col["max_size"]
                if len(encoded) > max_size:
                    encoded = encoded[:max_size]
                data += struct.pack("<H", len(encoded))  # 2 bytes length
                data += encoded

            else:
                raise ValueError(f"Unknown type: {col_type}")

        return bytes(null_bitmap) + bytes(data)

    # =========================================================================
    # Row-level UPDATE — single row by row_id
    # =========================================================================

    def _update_row_bytes(self, table_name: str, row_id: int, row_bytes: bytes):
        """Send UPDATE <table> <row_id> <size>\n<bytes> to the server."""
        sock = self._connect()
        try:
            header = f"UPDATE {table_name} {row_id} {len(row_bytes)}\n"
            sock.sendall(header.encode())
            sock.sendall(row_bytes)
            resp = self._read_response(sock)
        finally:
            sock.close()

        if resp["status"] == "ERR":
            raise RuntimeError(
                f"UPDATE {table_name} failed: {resp.get('err_line', '')}"
            )

    def update_row(self, table_name: str, row_id: int,
                   columns: list, values: list) -> int:
        """
        Serialize a row and send UPDATE <table> <row_id> <size>\n<bytes>.
        columns: list of column names (or None to use schema order)
        values:  list of values (same order as columns)
        Returns row_id.
        """
        schema_cols = self._fetch_schema(table_name)
        col_names   = columns if columns is not None else [c["name"] for c in schema_cols]
        val_dict    = dict(zip(col_names, values))
        row_bytes   = self._serialize_row(schema_cols, val_dict)
        self._update_row_bytes(table_name, row_id, row_bytes)
        return row_id

    def _fetch_rows_with_ids(self, table_name: str, columns: list) -> list:
        """
        Send SCAN <table>, return list of (row_dict, row_id) pairs.
        """
        sock = self._connect()
        try:
            self._send(sock, f"SCAN {table_name}")
            resp = self._read_response(sock, columns=columns)
        finally:
            sock.close()

        if resp["status"] == "ERR":
            raise RuntimeError(
                f"SCAN {table_name} failed: {resp.get('err_line', '')}"
            )

        return list(zip(resp["rows"], resp["row_ids"]))

    def _evaluate_condition(self, row: dict, condition) -> bool:
        """Evaluate a WHERE condition against a row dict."""
        from ast_nodes import Condition, LogicalCondition, NotCondition

        if isinstance(condition, Condition):
            val  = row.get(condition.column)
            op   = condition.operator
            cval = condition.value
            if op == '=':  return val == cval
            if op == '>':  return val is not None and val > cval
            if op == '<':  return val is not None and val < cval
            if op == '>=': return val is not None and val >= cval
            if op == '<=': return val is not None and val <= cval
            return False

        if isinstance(condition, LogicalCondition):
            left  = self._evaluate_condition(row, condition.left)
            right = self._evaluate_condition(row, condition.right)
            op    = condition.operator.upper()
            if op == 'AND': return left and right
            if op == 'OR':  return left or right
            return False

        if isinstance(condition, NotCondition):
            return not self._evaluate_condition(row, condition.condition)

        return True

    def update_rows(self, table_name: str, assignments, where) -> int:
        """
        Scan table, filter by WHERE, apply assignments, update each matching row.
        assignments: list of (column_name, value) tuples
        where: optional Condition/LogicalCondition/NotCondition
        Returns count of updated rows.
        """
        schema_cols    = self._fetch_schema(table_name)
        rows_with_ids  = self._fetch_rows_with_ids(table_name, schema_cols)

        updated = 0
        for row, row_id in rows_with_ids:
            if where is not None and not self._evaluate_condition(row, where):
                continue

            new_values = dict(row)
            for col, val in assignments:
                new_values[col] = val

            row_bytes = self._serialize_row(schema_cols, new_values)
            self._update_row_bytes(table_name, row_id, row_bytes)
            updated += 1

        return updated