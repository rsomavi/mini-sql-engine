#ifndef SERVER_H
#define SERVER_H

#include <stdint.h>
#include "../storage-engine/buffer_manager.h"

// ============================================================================
// Constants
// ============================================================================

#define SERVER_PORT         5433
#define SERVER_BACKLOG      8
#define MAX_LINE_LEN        1024   // max length of a request header line
#define MAX_TABLE_NAME      64
#define MAX_RESPONSE_SIZE   (1024 * 1024 * 16)  // 16MB max response buffer

// ============================================================================
// Server state
// ============================================================================

typedef struct {
    int            listen_fd;      // listening socket
    int            client_fd;      // current client socket (v1: single connection)
    BufferManager  bm;             // buffer manager — owns the buffer pool
    int            running;        // 1 while server is running, 0 to stop
    char           data_dir[256];  // directory where .db files are stored
} Server;

// ============================================================================
// Lifecycle
// ============================================================================

// Initialize the server: create socket, bind to port, init buffer manager.
// policy: eviction policy to use (server takes ownership)
// Returns 0 on success, -1 on error.
int server_init(Server *srv, const char *data_dir,
                int num_frames, EvictionPolicy *policy);

// Start the main accept loop. Blocks until server_stop() is called.
void server_run(Server *srv);

// Signal the server to stop after the current request completes.
void server_stop(Server *srv);

// Destroy the server: flush dirty pages, close sockets, free resources.
void server_destroy(Server *srv);

// ============================================================================
// Connection handling (internal — used by server_run)
// ============================================================================

// Handle a single client connection until it disconnects.
void server_handle_connection(Server *srv, int client_fd);

#endif
