#ifndef HANDLERS_H
#define HANDLERS_H

#include "server.h"
#include "protocol.h"
#include "../storage-engine/heap.h"

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

void handler_scan(Server *srv, int client_fd, Request *req);

void handler_schema(Server *srv, int client_fd, Request *req);

void handler_create(Server *srv, int client_fd, Request *req);

void handler_insert(Server *srv, int client_fd, Request *req);

void handler_delete(Server *srv, int client_fd, Request *req);

void handler_reset_metrics(Server *srv, int client_fd);

void handler_update(Server *srv, int client_fd, Request *req);

#endif
