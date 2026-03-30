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

        result_columns = []
        result_rows    = []
        metrics        = {}

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

            # ---- ROWS (SCAN response) ----
            if line.startswith("ROWS "):
                row_count = int(line.split()[1])
                for _ in range(row_count):
                    size_line = self._recv_line(sock)
                    row_size  = int(size_line.strip())
                    row_data  = self._recv_bytes(sock, row_size)
                    if columns:
                        row = self._deserialize_row(row_data, columns)
                        result_rows.append(row)
                continue

        return {
            "status":   status,
            "err_line": err_line,
            "columns":  result_columns,
            "rows":     result_rows,
            "metrics":  metrics,
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

    def reset_metrics(self):
        """Send RESET_METRICS to clear buffer pool counters."""
        sock = self._connect()
        try:
            self._send(sock, "RESET_METRICS")
            self._read_response(sock)
        finally:
            sock.close()

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

        print(f"[ServerStorage] → {cmd}")
        
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