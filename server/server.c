#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "server.h"
#include "protocol.h"
#include "handlers.h"

static Server *g_srv = NULL;

static void handle_signal(int sig) {
    fprintf(stderr, "\n[server] signal %d — flushing and shutting down\n", sig);
    if (g_srv) {
        server_stop(g_srv);
        server_destroy(g_srv);
    }
    exit(0);
}

static EvictionPolicy *create_policy(const char *name, int num_frames) {
    if (!name || strcmp(name, "lru") == 0) {
        return policy_lru_create();
    }
    if (strcmp(name, "clock") == 0) {
        return policy_clock_create(num_frames);
    }
    if (strcmp(name, "nocache") == 0) {
        return policy_nocache_create();
    }
    return NULL;
}


// ============================================================================
// server_init
// ============================================================================

int server_init(Server *srv, const char *data_dir,
                int num_frames, EvictionPolicy *policy) {
    if (!srv || !data_dir || !policy) return -1;

    // Initialize buffer manager
    if (bm_init(&srv->bm, num_frames, data_dir, policy) != 0) {
        fprintf(stderr, "[server] failed to init buffer manager\n");
        return -1;
    }

    strncpy(srv->data_dir, data_dir, 255);
    srv->data_dir[255] = '\0';
    srv->running   = 0;
    srv->client_fd = -1;

    // Create TCP socket
    srv->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (srv->listen_fd < 0) {
        perror("[server] socket");
        return -1;
    }

    // Allow reuse of port immediately after restart
    int opt = 1;
    setsockopt(srv->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind to port
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(SERVER_PORT);

    if (bind(srv->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("[server] bind");
        close(srv->listen_fd);
        return -1;
    }

    // Start listening
    if (listen(srv->listen_fd, SERVER_BACKLOG) < 0) {
        perror("[server] listen");
        close(srv->listen_fd);
        return -1;
    }

    printf("[server] listening on port %d\n", SERVER_PORT);
    printf("[server] data_dir: %s\n", data_dir);
    printf("[server] buffer pool: %d frames\n", num_frames);
    return 0;
}

// ============================================================================
// server_handle_connection
// ============================================================================

void server_handle_connection(Server *srv, int client_fd) {
    printf("[server] client connected\n");

    Request req;

    while (1) {
        // Read and parse one request
        int r = protocol_read_request(client_fd, &req);
        if (r < 0) {
            printf("[server] client disconnected\n");
            break;
        }

        printf("[server] received: %s\n", req.raw);

        // Dispatch to handler
        handler_dispatch(srv, client_fd, &req);
    }

    close(client_fd);
}

// ============================================================================
// server_run
// ============================================================================

void server_run(Server *srv) {
    if (!srv) return;
    srv->running = 1;

    printf("[server] ready — waiting for connections\n");

    while (srv->running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(srv->listen_fd,
                               (struct sockaddr *)&client_addr,
                               &client_len);
        if (client_fd < 0) {
            if (srv->running)
                perror("[server] accept");
            break;
        }

        printf("[server] connection from %s\n",
               inet_ntoa(client_addr.sin_addr));

        srv->client_fd = client_fd;
        server_handle_connection(srv, client_fd);
        srv->client_fd = -1;
    }
}

// ============================================================================
// server_stop
// ============================================================================
void server_stop(Server *srv) {
    if (!srv) return;
    srv->running = 0;
    if (srv->listen_fd >= 0) {
        close(srv->listen_fd);
        srv->listen_fd = -1;
    }
}

// ============================================================================
// server_destroy
// ============================================================================

void server_destroy(Server *srv) {
    if (!srv) return;
    bm_destroy(&srv->bm);
    if (srv->client_fd >= 0) close(srv->client_fd);
    if (srv->listen_fd >= 0) close(srv->listen_fd);
    srv->client_fd = -1;
    srv->listen_fd = -1;
}

// ============================================================================
// main
// ============================================================================

int main(int argc, char *argv[]) {
    const char *data_dir  = "../data";
    int         num_frames = 64;
    const char *policy_name = "lru";

    if (argc >= 2) data_dir   = argv[1];
    if (argc >= 3) num_frames = atoi(argv[2]);
    if (argc >= 4) policy_name = argv[3];

    // Ignore SIGPIPE — don't crash when client disconnects mid-write
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  handle_signal);
    signal(SIGTERM, handle_signal);

    EvictionPolicy *policy = create_policy(policy_name, num_frames);
    if (!policy) {
        fprintf(stderr,
                "[server] unknown policy '%s' (supported: lru, clock, nocache)\n",
                policy_name);
        return 1;
    }

    Server srv;
    g_srv = &srv;
    if (server_init(&srv, data_dir, num_frames, policy) != 0) {
        fprintf(stderr, "[server] init failed\n");
        return 1;
    }

    printf("[server] eviction policy: %s\n", policy_name);
    server_run(&srv);
    server_destroy(&srv);
    return 0;
}
