#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "handlers.h"
#include "../storage-engine/page.h"
#include "../storage-engine/disk.h"
#include "../storage-engine/schema.h"

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
        case OP_SCAN:
            handler_scan(srv, client_fd, req);
            break;
        case OP_SCHEMA:
            handler_schema(srv, client_fd, req);
            break;
        case OP_CREATE:
            handler_create(srv, client_fd, req);
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

void handler_scan(Server *srv, int client_fd, Request *req) {
    ResponseBuf rb;
    protocol_response_init(&rb);

    // Validate table name
    if (req->table_name[0] == '\0') {
        protocol_response_append(&rb, "ERR INVALID_ARGS missing table name\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    // Get number of pages for this table
    int num_pages = get_num_pages(srv->data_dir, req->table_name);
    if (num_pages <= 1) {
        // No data pages (page 0 is schema only)
        protocol_response_append(&rb, "OK\n");
        protocol_response_append(&rb, "ROWS 0\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    // First pass: count rows to send ROWS <count> header
    int total_rows = 0;
    for (int page_id = 1; page_id < num_pages; page_id++) {
        char *page = bm_fetch_page(&srv->bm, req->table_name, page_id);
        if (!page) continue;

        PageHeader *header   = (PageHeader *)page;
        int        *slot_dir = (int *)(page + sizeof(PageHeader));

        for (int slot_id = 0; slot_id < header->num_slots; slot_id++) {
            if (slot_dir[slot_id] != -1)
                total_rows++;
        }
        bm_unpin_page(&srv->bm, req->table_name, page_id, 0);
    }

    // Send header
    protocol_response_append(&rb, "OK\n");

    char rows_line[64];
    snprintf(rows_line, sizeof(rows_line), "ROWS %d\n", total_rows);
    protocol_response_append(&rb, rows_line);
    protocol_response_send(&rb, client_fd);
    protocol_response_free(&rb);

    // Second pass: send rows via buffer pool
    for (int page_id = 1; page_id < num_pages; page_id++) {
        char *page = bm_fetch_page(&srv->bm, req->table_name, page_id);
        if (!page) continue;

        PageHeader *header   = (PageHeader *)page;
        int        *slot_dir = (int *)(page + sizeof(PageHeader));

        for (int slot_id = 0; slot_id < header->num_slots; slot_id++) {
            if (slot_dir[slot_id] == -1) continue;  // deleted slot

            int   row_size = get_row_size(page, slot_id);
            char *row      = read_row(page, slot_id);

            if (!row || row_size <= 0) continue;

            // Send: "<row_size>\n<binary row data>"
            ResponseBuf row_rb;
            protocol_response_init(&row_rb);

            char size_line[32];
            snprintf(size_line, sizeof(size_line), "%d\n", row_size);
            protocol_response_append(&row_rb, size_line);
            protocol_response_append_binary(&row_rb, row, row_size);

            protocol_response_send(&row_rb, client_fd);
            protocol_response_free(&row_rb);
        }

        bm_unpin_page(&srv->bm, req->table_name, page_id, 0);
    }

    // Send metrics and END
    ResponseBuf end_rb;
    protocol_response_init(&end_rb);
    append_metrics(&end_rb, srv);
    protocol_response_append(&end_rb, "END\n");
    protocol_response_send(&end_rb, client_fd);
    protocol_response_free(&end_rb);
}

void handler_schema(Server *srv, int client_fd, Request *req) {
    ResponseBuf rb;
    protocol_response_init(&rb);

    if (req->table_name[0] == '\0') {
        protocol_response_append(&rb, "ERR INVALID_ARGS missing table name\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    // Load schema from page 0
    Schema schema;
    if (schema_load(&schema, req->table_name, srv->data_dir) != 0) {
        protocol_response_append(&rb, "ERR TABLE_NOT_FOUND table does not exist\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    // Send header
    protocol_response_append(&rb, "OK\n");

    char col_count[32];
    snprintf(col_count, sizeof(col_count), "COLUMNS %d\n", schema.num_columns);
    protocol_response_append(&rb, col_count);

    // Send one line per column: name:type:max_size:nullable:pk
    for (int i = 0; i < schema.num_columns; i++) {
        ColumnDef *col = &schema.columns[i];

        const char *type_str;
        switch (col->type) {
            case TYPE_INT:     type_str = "INT";     break;
            case TYPE_FLOAT:   type_str = "FLOAT";   break;
            case TYPE_BOOLEAN: type_str = "BOOL";    break;
            case TYPE_VARCHAR: type_str = "VARCHAR"; break;
            default:           type_str = "UNKNOWN"; break;
        }

        char col_line[256];
        snprintf(col_line, sizeof(col_line), "%s:%s:%d:%d:%d\n",
                 col->name,
                 type_str,
                 col->max_size,
                 col->nullable,
                 col->is_primary_key);
        protocol_response_append(&rb, col_line);
    }

    append_metrics(&rb, srv);
    protocol_response_append(&rb, "END\n");
    protocol_response_send(&rb, client_fd);
    protocol_response_free(&rb);
}

void handler_create(Server *srv, int client_fd, Request *req) {
    ResponseBuf rb;
    protocol_response_init(&rb);

    if (req->table_name[0] == '\0') {
        protocol_response_append(&rb, "ERR INVALID_ARGS missing table name\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    // Check table does not already exist
    if (get_num_pages(srv->data_dir, req->table_name) > 0) {
        protocol_response_append(&rb, "ERR TABLE_EXISTS table already exists\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    // Parse columns from args
    // args = "ciudades id:INT:4:0:1 nombre:VARCHAR:100:0:0 poblacion:INT:4:1:0"
    Schema schema;
    memset(&schema, 0, sizeof(Schema));
    strncpy(schema.table_name, req->table_name, MAX_TABLE_NAME - 1);
    schema.num_columns = 0;

    // Skip the table name token and parse column definitions
    char args_copy[1024];
    strncpy(args_copy, req->args, sizeof(args_copy) - 1);
    args_copy[sizeof(args_copy) - 1] = '\0';

    char *token = strtok(args_copy, " ");  // first column directly

    while (token != NULL && schema.num_columns < MAX_COLUMNS) {
        // token = "id:INT:4:0:1"
        char col_name[MAX_COL_NAME];
        char type_str[16];
        int  max_size  = 0;
        int  nullable  = 1;
        int  pk        = 0;

        if (sscanf(token, "%63[^:]:%15[^:]:%d:%d:%d",
                   col_name, type_str, &max_size, &nullable, &pk) != 5) {
            protocol_response_append(&rb, "ERR INVALID_ARGS malformed column\n");
            append_metrics(&rb, srv);
            protocol_response_append(&rb, "END\n");
            protocol_response_send(&rb, client_fd);
            protocol_response_free(&rb);
            return;
        }

        ColumnDef *col = &schema.columns[schema.num_columns];
        strncpy(col->name, col_name, MAX_COL_NAME - 1);
        col->name[MAX_COL_NAME - 1] = '\0';
        col->max_size      = max_size;
        col->nullable      = nullable;
        col->is_primary_key = pk;

        if      (strcmp(type_str, "INT")     == 0) col->type = TYPE_INT;
        else if (strcmp(type_str, "FLOAT")   == 0) col->type = TYPE_FLOAT;
        else if (strcmp(type_str, "BOOL")    == 0) col->type = TYPE_BOOLEAN;
        else if (strcmp(type_str, "VARCHAR") == 0) col->type = TYPE_VARCHAR;
        else {
            protocol_response_append(&rb, "ERR INVALID_ARGS unknown type\n");
            append_metrics(&rb, srv);
            protocol_response_append(&rb, "END\n");
            protocol_response_send(&rb, client_fd);
            protocol_response_free(&rb);
            return;
        }

        schema.num_columns++;
        token = strtok(NULL, " ");
    }

    if (schema.num_columns == 0) {
        protocol_response_append(&rb, "ERR INVALID_ARGS no columns defined\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    // Save schema to disk (page 0)
    if (schema_save(&schema, srv->data_dir) != 0) {
        protocol_response_append(&rb, "ERR IO_ERROR failed to save schema\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    // Success
    char created_line[128];
    snprintf(created_line, sizeof(created_line),
             "OK\nCREATED %s\n", req->table_name);
    protocol_response_append(&rb, created_line);
    append_metrics(&rb, srv);
    protocol_response_append(&rb, "END\n");
    protocol_response_send(&rb, client_fd);
    protocol_response_free(&rb);
}