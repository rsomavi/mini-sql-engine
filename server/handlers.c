#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "handlers.h"

// ============================================================================
// Internal helper: append metrics footer to response
// ============================================================================

static void append_metrics(ResponseBuf *rb, Server *srv) {
    char metrics[256];
    snprintf(metrics, sizeof(metrics),
             "METRICS hits=%lld misses=%lld evictions=%lld hit_rate=%.3f\n",
             bm_get_hits(&srv->bm),
             bm_get_misses(&srv->bm),
             bm_get_evictions(&srv->bm),
             bm_get_hit_rate(&srv->bm));
    protocol_response_append(rb, metrics);
}

// ============================================================================
// handler_dispatch
// ============================================================================

void handler_dispatch(Server *srv, int client_fd, Request *req) {
    switch (req->op) {
        case OP_PING:
            handler_ping(srv, client_fd);
            break;
        case OP_UNKNOWN:
        default: {
            ResponseBuf rb;
            protocol_response_init(&rb);
            protocol_response_append(&rb, "ERR INVALID_OP unknown operation\n");
            append_metrics(&rb, srv);
            protocol_response_append(&rb, "END\n");
            protocol_response_send(&rb, client_fd);
            protocol_response_free(&rb);
            break;
        }
    }
}

// ============================================================================
// handler_ping
// ============================================================================

void handler_ping(Server *srv, int client_fd) {
    ResponseBuf rb;
    protocol_response_init(&rb);

    protocol_response_append(&rb, "OK\n");
    protocol_response_append(&rb, "PONG\n");
    append_metrics(&rb, srv);
    protocol_response_append(&rb, "END\n");

    protocol_response_send(&rb, client_fd);
    protocol_response_free(&rb);
}
