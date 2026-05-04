#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "protocol.h"

// ============================================================================
// Internal: read one line from socket (terminated by \n)
// ============================================================================

static int read_line(int fd, char *buf, int maxlen) {
    int  n    = 0;
    char c    = 0;
    int  last = maxlen - 1;

    while (n < last) {
        int r = read(fd, &c, 1);
        if (r <= 0) return -1;   // connection closed or error
        if (c == '\n') break;
        buf[n++] = c;
    }
    buf[n] = '\0';
    return n;
}

// ============================================================================
// protocol_read_request
// ============================================================================

int protocol_read_request(int client_fd, Request *req) {
    if (client_fd < 0 || !req) return -1;

    memset(req, 0, sizeof(Request));

    char line[1024];
    if (read_line(client_fd, line, sizeof(line)) < 0)
        return -1;

    // Store raw line for debugging
    strncpy(req->raw, line, sizeof(req->raw) - 1);
    req->raw[sizeof(req->raw) - 1] = '\0';

    // Parse operation — just PING for now
    if (strcmp(line, "PING") == 0) {
        req->op = OP_PING;

        return 0;
    } else if (strncmp(line, "SCAN ", 5) == 0) { // SCAN <table_name>
        req->op = OP_SCAN;

        strncpy(req->table_name, line + 5, sizeof(req->table_name) - 1);
        req->table_name[sizeof(req->table_name) - 1] = '\0';
        return 0;
    } else if (strncmp(line, "SCHEMA ", 7) == 0) { // SCHEMA <table_name>
        req->op = OP_SCHEMA;
        
        strncpy(req->table_name, line + 7, sizeof(req->table_name) - 1);
        req->table_name[sizeof(req->table_name) - 1] = '\0';
        return 0;
        
    } else if (strncmp(line, "CREATE ", 7) == 0) { // CREATE <table_name> <col1:type1> <col2:type2> ...\n
        req->op            = OP_CREATE;
        req->table_name[0] = '\0';
        req->args[0]       = '\0';

        // Extraer table_name — primer token después de "CREATE "
        sscanf(line + 7, "%63s", req->table_name);

        // args = solo las columnas, sin el nombre de tabla
        // skip = 7 (CREATE ) + len(table_name) + 1 (espacio)
        int skip = 7 + (int)strlen(req->table_name) + 1;
        if (skip < (int)strlen(line)) {
            strncpy(req->args, line + skip, sizeof(req->args) - 1);
            req->args[sizeof(req->args) - 1] = '\0';
        }
        return 0;
    } else if (strncmp(line, "INSERT ", 7) == 0) { // INSERT <table_name> <payload_size>\n
        req->op = OP_INSERT;                       // <payload_size>\n
        req->table_name[0] = '\0';                 //<binary serialized row>
        req->args[0]       = '\0';
        req->payload_size  = 0;

        sscanf(line + 7, "%63s %d", req->table_name, &req->payload_size);

        if (req->payload_size > 0 &&
            req->payload_size < (int)sizeof(req->payload)) {
            int  total = 0;
            while (total < req->payload_size) {
                int r = read(client_fd, req->payload + total,
                            req->payload_size - total);
                if (r <= 0) return -1;
                total += r;
            }
        }

        return 0;
    } else if (strncmp(line, "DELETE ", 7) == 0) {
        req->op = OP_DELETE;
        req->table_name[0] = '\0';
        req->args[0]       = '\0';

        sscanf(line + 7, "%63s", req->table_name);

        int skip = 7 + (int)strlen(req->table_name) + 1;
        if (skip < (int)strlen(line)) {
            strncpy(req->args, line + skip, sizeof(req->args) - 1);
            req->args[sizeof(req->args) - 1] = '\0';
        }
        return 0;
    } else if (strcmp(line, "RESET_METRICS") == 0) {
        req->op = OP_RESET_METRICS;
        return 0;
    } else if (strcmp(line, "TRACE_START") == 0) {
        req->op = OP_TRACE_START;
        return 0;
    } else if (strcmp(line, "TRACE_STOP") == 0) {
        req->op = OP_TRACE_STOP;
        return 0;
    } else if (strcmp(line, "TRACE_CLEAR") == 0) {
        req->op = OP_TRACE_CLEAR;
        return 0;
    } else if (strncmp(line, "UPDATE ", 7) == 0) { // UPDATE <table_name> <row_id> <payload_size>\n
        req->op            = OP_UPDATE;             // <binary serialized row>
        req->table_name[0] = '\0';
        req->args[0]       = '\0';
        req->payload_size  = 0;

        int row_id = 0;
        sscanf(line + 7, "%63s %d %d", req->table_name, &row_id, &req->payload_size);
        snprintf(req->args, sizeof(req->args), "%d", row_id);

        if (req->payload_size > 0 &&
            req->payload_size < (int)sizeof(req->payload)) {
            int  total = 0;
            while (total < req->payload_size) {
                int r = read(client_fd, req->payload + total,
                            req->payload_size - total);
                if (r <= 0) return -1;
                total += r;
            }
        }
        return 0;
    }


    req->op = OP_UNKNOWN;
    return 0;
}

// ============================================================================
// ResponseBuf
// ============================================================================

int protocol_response_init(ResponseBuf *rb) {
    if (!rb) return -1;
    rb->cap = 4096;
    rb->len = 0;
    rb->buf = malloc(rb->cap);
    if (!rb->buf) return -1;
    rb->buf[0] = '\0';
    return 0;
}

int protocol_response_append(ResponseBuf *rb, const char *text) {
    if (!rb || !text) return -1;

    size_t tlen = strlen(text);

    // Grow buffer if needed
    while (rb->len + tlen + 1 > rb->cap) {
        rb->cap *= 2;
        char *newbuf = realloc(rb->buf, rb->cap);
        if (!newbuf) return -1;
        rb->buf = newbuf;
    }

    memcpy(rb->buf + rb->len, text, tlen);
    rb->len += tlen;
    rb->buf[rb->len] = '\0';
    return 0;
}

int protocol_response_append_binary(ResponseBuf *rb,
                                     const char *data, size_t len) {
    if (!rb || !data) return -1;

    // Need len bytes + 1 for safety (no \0 appended for binary)
    while (rb->len + len + 1 > rb->cap) {
        rb->cap *= 2;
        char *newbuf = realloc(rb->buf, rb->cap);
        if (!newbuf) return -1;
        rb->buf = newbuf;
    }

    memcpy(rb->buf + rb->len, data, len);
    rb->len += len;
    return 0;
}

int protocol_response_send(ResponseBuf *rb, int client_fd) {
    if (!rb || client_fd < 0) return -1;

    size_t sent = 0;
    while (sent < rb->len) {
        int r = write(client_fd, rb->buf + sent, rb->len - sent);
        if (r <= 0) return -1;
        sent += r;
    }

    // Reset buffer for reuse
    rb->len    = 0;
    rb->buf[0] = '\0';
    return 0;
}

void protocol_response_free(ResponseBuf *rb) {
    if (!rb) return;
    free(rb->buf);
    rb->buf = NULL;
    rb->len = 0;
    rb->cap = 0;
}
