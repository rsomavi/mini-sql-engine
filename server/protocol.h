#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stddef.h>

// ============================================================================
// Operation types
// ============================================================================

typedef enum {
    OP_PING,
    OP_SCAN,
    OP_SCHEMA,
    OP_CREATE,
    OP_INSERT,
    OP_DELETE,
    OP_RESET_METRICS,
    OP_UPDATE,
    OP_UNKNOWN
} OperationType;

// ============================================================================
// Request — a parsed request from the client
// ============================================================================

typedef struct {
    OperationType  op;
    char           table_name[64];
    char           args[1024];
    int            payload_size;
    char           payload[8192];
    char           raw[1024];   // original line for debugging
} Request;

// ============================================================================
// Response buffer — used to build responses before sending
// ============================================================================

typedef struct {
    char  *buf;       // heap-allocated buffer
    size_t len;       // bytes written so far
    size_t cap;       // total capacity
} ResponseBuf;

// ============================================================================
// API
// ============================================================================

// Read one request from the socket and parse it into req.
// Returns 0 on success, -1 on error or connection closed.
int  protocol_read_request(int client_fd, Request *req);

// Initialize a response buffer. Must call protocol_response_free when done.
int  protocol_response_init(ResponseBuf *rb);

// Append text to the response buffer.
int  protocol_response_append(ResponseBuf *rb, const char *text);

int  protocol_response_append_binary(ResponseBuf *rb,
                                     const char *data, size_t len);

// Send the response buffer to the client and reset it.
int  protocol_response_send(ResponseBuf *rb, int client_fd);

// Free the response buffer.
void protocol_response_free(ResponseBuf *rb);

#endif
