# Server

The C server is a TCP daemon that listens on port 5433, accepts SQL engine connections, processes requests through the storage layer, and returns binary or text responses. It owns the buffer manager and is the only process that accesses `.db` files directly.

## File overview

| File | Responsibility |
|------|---------------|
| `server.c / server.h` | Lifecycle, main loop, signal handling |
| `protocol.c / protocol.h` | Request parsing, response buffer |
| `handlers.c / handlers.h` | One handler per operation |

---

## Building and running

```bash
cd server/
make          # builds minidbms-server
make run      # builds and runs with ../data, 64 frames, LRU policy
make run POLICY=clock
make clean    # removes all .o files and the binary
```

The Makefile compiles the server objects (`server.o`, `protocol.o`, `handlers.o`) and all storage engine objects (`buffer_manager.o`, `eviction_policy.o`, `page_table.o`, `buffer_frame.o`, `heap.o`, `page.o`, `disk.o`, `schema.o`) and links them into a single binary.

Command line arguments:

```bash
./minidbms-server [data_dir] [num_frames] [policy]
# defaults: data_dir=../data  num_frames=64  policy=lru
```

Supported startup policies:

```bash
./minidbms-server ../data 64 lru
./minidbms-server ../data 64 clock
./minidbms-server ../data 64 nocache
```

---

## Server lifecycle — `server.c`

### Startup

`main` registers signal handlers, initializes the server, and enters the accept loop:

```c
signal(SIGPIPE, SIG_IGN);       // don't crash on broken client connection
signal(SIGINT,  handle_signal); // Ctrl+C
signal(SIGTERM, handle_signal); // kill

Server srv;
g_srv = &srv;
server_init(&srv, data_dir, num_frames, create_policy(policy_name, num_frames));
server_run(&srv);
server_destroy(&srv);
```

`server_init` initializes the buffer manager, creates the TCP socket, sets `SO_REUSEADDR` (allows immediate restart without waiting for TIME_WAIT), binds to port 5433, and starts listening with a backlog of 8.

### Accept loop

`server_run` blocks on `accept`. When a client connects, it calls `server_handle_connection` which reads requests in a loop until the client disconnects:

```c
while (1) {
    int r = protocol_read_request(client_fd, &req);
    if (r < 0) break;  // client disconnected
    handler_dispatch(srv, client_fd, &req);
}
close(client_fd);
```

v1 is single-client — while a client is connected, no new connections are accepted. The next `accept` call only happens after `server_handle_connection` returns.

### Shutdown

When SIGINT or SIGTERM is received, `handle_signal` calls `server_stop` (closes listen socket, sets `running = 0`) and then `server_destroy`:

```c
void server_destroy(Server *srv) {
    bm_destroy(&srv->bm);   // flush all dirty pages to disk
    close(client_fd);
    close(listen_fd);
}
```

`bm_destroy` is the critical call — it flushes all dirty frames to disk before the process exits. Without it, data modified since the last eviction would be lost.

### Server struct

```c
typedef struct {
    int           listen_fd;   // listening socket
    int           client_fd;   // current client socket
    BufferManager bm;          // owns the buffer pool
    int           running;     // 1 while running, 0 to stop
    char          data_dir[256];
} Server;
```

---

## Protocol — `protocol.c`

### MINIDBMS-RESP v1.2

The protocol is line-oriented for requests and mixed text/binary for responses. All request lines are terminated by `\n`. All responses end with `END\n`.

### Request format

`protocol_read_request` reads one line from the socket and parses the operation:

| Operation | Request line | Binary payload |
|-----------|-------------|----------------|
| PING | `PING\n` | none |
| SCAN | `SCAN <table>\n` | none |
| SCHEMA | `SCHEMA <table>\n` | none |
| CREATE | `CREATE <table> <col:type:size:nullable:pk> ...\n` | none |
| INSERT | `INSERT <table> <size>\n` | `<size>` bytes of binary row data |

For INSERT, after reading the header line, the server reads exactly `payload_size` bytes into `req->payload`:

```c
while (total < req->payload_size) {
    int r = read(client_fd, req->payload + total, req->payload_size - total);
    if (r <= 0) return -1;
    total += r;
}
```

The loop handles partial reads — TCP does not guarantee that all bytes arrive in a single `read` call.

### Request struct

```c
typedef struct {
    OperationType op;
    char          table_name[64];
    char          args[1024];     // column definitions for CREATE
    int           payload_size;   // byte count for INSERT payload
    char          payload[8192];  // binary row data for INSERT
    char          raw[1024];      // original line for debug logging
} Request;
```

### Response format

All responses follow this structure:

```
OK\n                          (or ERR <code> <message>\n)
<operation-specific lines>
METRICS hits=N misses=N evictions=N hit_rate=F\n
END\n
```

Responses are built with `ResponseBuf` — a heap-allocated buffer that grows automatically:

```c
protocol_response_init(&rb);
protocol_response_append(&rb, "OK\n");
protocol_response_append(&rb, "ROWS 3\n");
protocol_response_append_binary(&rb, row_data, row_size);
protocol_response_send(&rb, client_fd);
protocol_response_free(&rb);
```

`ResponseBuf` starts at 4096 bytes and doubles when needed. `protocol_response_send` writes the entire buffer to the socket in a loop to handle partial writes, then resets the buffer for reuse.

### Error responses

```
ERR INVALID_ARGS missing table name\n
ERR TABLE_NOT_FOUND table does not exist\n
ERR TABLE_EXISTS table already exists\n
ERR IO_ERROR failed to insert row\n
ERR INVALID_OP unknown operation\n
```

---

## Handlers — `handlers.c`

`handler_dispatch` routes requests to the correct handler based on `req->op`. Each handler validates the request, calls the storage layer, builds a response, and sends it.

### handler_ping

```
Request:  PING
Response: OK\nPONG\nMETRICS ...\nEND\n
```

### handler_schema

Loads the schema from page 0 using `schema_load` and returns one line per column:

```
Response: OK\nCOLUMNS 3\nid:INT:4:0:1\nnombre:VARCHAR:50:0:0\nsalario:INT:4:1:0\nMETRICS ...\nEND\n
```

Column line format: `name:type:max_size:nullable:pk`

### handler_scan

Two-pass scan over all data pages via the buffer manager:

**Pass 1** — counts total rows (needed for the `ROWS N` header):
```c
for each page: bm_fetch_page → count non-deleted slots → bm_unpin_page(dirty=0)
```

**Pass 2** — sends rows:
```c
for each page: bm_fetch_page → for each slot: send size\n + binary data → bm_unpin_page(dirty=0)
```

Each row is sent as:
```
<row_size>\n
<row_size bytes of binary data>
```

### handler_create

Parses column definitions from `req->args` using `sscanf` with format `name:type:max_size:nullable:pk`. Validates each column, builds a `Schema` struct, and calls `schema_save` to write it to page 0.

Fails with `ERR TABLE_EXISTS` if `get_num_pages > 0`.

### handler_insert

Validates `req->table_name` and `req->payload_size`, then calls `heap_insert_bm`:

```c
int row_id = heap_insert_bm(srv->data_dir, req->table_name,
                            req->payload, req->payload_size,
                            &srv->bm);
```

Returns `OK\nROW_ID <n>\n...END\n` on success.

---

## Constants

| Constant | Value | Defined in |
|----------|-------|-----------|
| `SERVER_PORT` | 5433 | `server.h` |
| `SERVER_BACKLOG` | 8 | `server.h` |
| `MAX_RESPONSE_SIZE` | 16 MB | `server.h` |
| `MAX_TABLE_NAME` | 64 | `server.h` |
| `MAX_LINE_LEN` | 1024 | `server.h` |

---

## Running with the SQL engine

Start the server in one terminal:

```bash
cd server/
make run
```

Start the SQL client in another:

```bash
cd sql-engine/
python3 main.py
```

The client connects to `localhost:5433` on each operation. Both processes must be running simultaneously. The server logs each received request to stdout.
