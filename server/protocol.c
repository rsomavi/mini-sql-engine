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
