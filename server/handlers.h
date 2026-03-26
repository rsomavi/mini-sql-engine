#ifndef HANDLERS_H
#define HANDLERS_H

#include "server.h"
#include "protocol.h"

// ============================================================================
// Request dispatcher
// ============================================================================

// Dispatch a parsed request to the correct handler.
// This is the only function server.c needs to call.
void handler_dispatch(Server *srv, int client_fd, Request *req);

// ============================================================================
// Individual handlers — one per operation
// ============================================================================

// PING — verify server is alive
void handler_ping(Server *srv, int client_fd);

#endif
