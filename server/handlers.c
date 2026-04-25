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
        case OP_INSERT:
            handler_insert(srv, client_fd, req);
            break;
        case OP_DELETE:
            handler_delete(srv, client_fd, req);
            break;
        case OP_RESET_METRICS:
            handler_reset_metrics(srv, client_fd);
            break;
        case OP_UPDATE:
            handler_update(srv, client_fd, req);
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

    if (req->table_name[0] == '\0') {
        protocol_response_append(&rb, "ERR INVALID_ARGS missing table name\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    int num_pages = get_num_pages(srv->data_dir, req->table_name);
    if (num_pages <= 1) {
        protocol_response_append(&rb, "OK\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    // Single pass: copy every row to heap and unpin each page before sending,
    // so the second traversal (sending) does not register artificial cache hits.
    int    cap       = 256;
    int    n_rows    = 0;
    char **row_data  = malloc(cap * sizeof(char *));
    int   *row_sizes = malloc(cap * sizeof(int));
    int   *row_ids   = malloc(cap * sizeof(int));

    if (!row_data || !row_sizes || !row_ids) {
        free(row_data);
        free(row_sizes);
        free(row_ids);
        protocol_response_append(&rb, "ERR INTERNAL out of memory\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    for (int page_id = 1; page_id < num_pages; page_id++) {
        char *page = bm_fetch_page(&srv->bm, req->table_name, page_id);
        if (!page) continue;

        PageHeader *header   = (PageHeader *)page;
        SlotEntry  *slot_dir = (SlotEntry *)(page + sizeof(PageHeader));

        for (int slot_id = 0; slot_id < header->num_slots; slot_id++) {
            if (slot_dir[slot_id].state == SLOT_DELETED)  continue;
            if (slot_dir[slot_id].state == SLOT_REDIRECT) continue;

            int   sz  = get_row_size(page, slot_id);
            char *row = read_row(page, slot_id);
            if (!row || sz <= 0) continue;

            if (n_rows == cap) {
                int    new_cap = cap * 2;
                char **d  = realloc(row_data,  new_cap * sizeof(char *));
                int   *s  = realloc(row_sizes, new_cap * sizeof(int));
                int   *ri = realloc(row_ids,   new_cap * sizeof(int));
                if (!d || !s || !ri) {
                    char **cd  = d  ? d  : row_data;
                    int   *cs  = s  ? s  : row_sizes;
                    int   *cri = ri ? ri : row_ids;
                    for (int i = 0; i < n_rows; i++) free(cd[i]);
                    free(cd);
                    free(cs);
                    free(cri);
                    bm_unpin_page(&srv->bm, req->table_name, page_id, 0);
                    protocol_response_append(&rb, "ERR INTERNAL out of memory\n");
                    append_metrics(&rb, srv);
                    protocol_response_append(&rb, "END\n");
                    protocol_response_send(&rb, client_fd);
                    protocol_response_free(&rb);
                    return;
                }
                row_data  = d;
                row_sizes = s;
                row_ids   = ri;
                cap       = new_cap;
            }

            char *copy = malloc(sz);
            if (!copy) continue;
            memcpy(copy, row, sz);
            row_data[n_rows]  = copy;
            row_sizes[n_rows] = sz;
            row_ids[n_rows]   = encode_rowid(page_id, slot_id);
            n_rows++;
        }

        bm_unpin_page(&srv->bm, req->table_name, page_id, 0);
    }

    // All pages unpinned. Build and send response from heap copies.
    protocol_response_append(&rb, "OK\n");
    protocol_response_send(&rb, client_fd);
    protocol_response_free(&rb);

    for (int i = 0; i < n_rows; i++) {
        ResponseBuf row_rb;
        protocol_response_init(&row_rb);
        char size_line[48];
        snprintf(size_line, sizeof(size_line), "%d %d\n", row_ids[i], row_sizes[i]);
        protocol_response_append(&row_rb, size_line);
        protocol_response_append_binary(&row_rb, row_data[i], row_sizes[i]);
        protocol_response_send(&row_rb, client_fd);
        protocol_response_free(&row_rb);
        free(row_data[i]);
    }
    free(row_data);
    free(row_sizes);
    free(row_ids);

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

    // No table name
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

void handler_insert(Server *srv, int client_fd, Request *req) {
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

    if (req->payload_size <= 0) {
        protocol_response_append(&rb, "ERR INVALID_ARGS missing payload\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    if (get_num_pages(srv->data_dir, req->table_name) == 0) {
        protocol_response_append(&rb, "ERR TABLE_NOT_FOUND table does not exist\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    int row_id = heap_insert_bm(srv->data_dir, req->table_name,
                                req->payload, req->payload_size,
                                &srv->bm);

    if (row_id < 0) {
        protocol_response_append(&rb, "ERR IO_ERROR failed to insert row\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    char row_id_line[64];
    snprintf(row_id_line, sizeof(row_id_line), "OK\nROW_ID %d\n", row_id);
    protocol_response_append(&rb, row_id_line);
    append_metrics(&rb, srv);
    protocol_response_append(&rb, "END\n");
    protocol_response_send(&rb, client_fd);
    protocol_response_free(&rb);
}

void handler_delete(Server *srv, int client_fd, Request *req) {
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

    if (get_num_pages(srv->data_dir, req->table_name) == 0) {
        protocol_response_append(&rb, "ERR TABLE_NOT_FOUND table does not exist\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    // Cargar schema para deserializar filas
    Schema schema;
    if (schema_load(&schema, req->table_name, srv->data_dir) != 0) {
        protocol_response_append(&rb, "ERR IO_ERROR failed to load schema\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    // Parsear WHERE clause de req->args
    // Formato: "WHERE col op val"
    char where_col[64] = {0};
    char where_op[4]   = {0};
    char where_val[256] = {0};
    int  has_where = 0;

    if (req->args[0] != '\0') {
        char *clause = req->args;
        if (strncmp(clause, "WHERE ", 6) == 0) clause += 6;

        if (sscanf(clause, "%63s", where_col) == 1) {
            char *after_col = clause + strlen(where_col);
            while (*after_col == ' ') after_col++;
            if      (strncmp(after_col, ">=", 2) == 0) { strcpy(where_op, ">="); after_col += 2; }
            else if (strncmp(after_col, "<=", 2) == 0) { strcpy(where_op, "<="); after_col += 2; }
            else if (*after_col == '=')  { strcpy(where_op, "=");  after_col += 1; }
            else if (*after_col == '>')  { strcpy(where_op, ">");  after_col += 1; }
            else if (*after_col == '<')  { strcpy(where_op, "<");  after_col += 1; }
            while (*after_col == ' ') after_col++;
            strncpy(where_val, after_col, sizeof(where_val) - 1);
            has_where = 1;
        }
    }

    int deleted = 0;
    int num_pages = get_num_pages(srv->data_dir, req->table_name);

    for (int page_id = 1; page_id < num_pages; page_id++) {
        char *page = bm_fetch_page(&srv->bm, req->table_name, page_id);
        if (!page) continue;

        PageHeader *header   = (PageHeader *)page;
        SlotEntry  *slot_dir = (SlotEntry *)(page + sizeof(PageHeader));
        int         dirty    = 0;

        for (int slot = 0; slot < header->num_slots; slot++) {
            if (slot_dir[slot].state == SLOT_DELETED) continue;

            int   row_size = get_row_size(page, slot);
            char *row      = read_row(page, slot);
            if (!row || row_size <= 0) continue;

            int match = !has_where; // sin WHERE borra todo

            if (has_where) {
                int col_idx = schema_get_column_index(&schema, where_col);
                if (col_idx >= 0) {
                    void *values[MAX_COLUMNS];
                    int   sizes[MAX_COLUMNS];
                    char  bufs[MAX_COLUMNS][256];
                    for (int i = 0; i < schema.num_columns; i++) {
                        values[i] = bufs[i];
                        sizes[i]  = 0;
                    }
                    if (row_deserialize(&schema, row, row_size, values, sizes) >= 0
                        && sizes[col_idx] > 0) {
                        if (schema.columns[col_idx].type == TYPE_INT) {
                            int row_val  = *(int *)values[col_idx];
                            int cond_val = atoi(where_val);
                            if      (strcmp(where_op, "=")  == 0) match = row_val == cond_val;
                            else if (strcmp(where_op, ">")  == 0) match = row_val >  cond_val;
                            else if (strcmp(where_op, "<")  == 0) match = row_val <  cond_val;
                            else if (strcmp(where_op, ">=") == 0) match = row_val >= cond_val;
                            else if (strcmp(where_op, "<=") == 0) match = row_val <= cond_val;
                        } else if (schema.columns[col_idx].type == TYPE_VARCHAR) {
                            char cond_val_str[256];
                            strncpy(cond_val_str, where_val, sizeof(cond_val_str) - 1);
                            int len = strlen(cond_val_str);
                            if (len >= 2 && cond_val_str[0] == '\''
                                && cond_val_str[len-1] == '\'') {
                                memmove(cond_val_str, cond_val_str+1, len-2);
                                cond_val_str[len-2] = '\0';
                            }
                            unsigned short vlen;
                            memcpy(&vlen, values[col_idx], 2);
                            char row_str[256] = {0};
                            int copy_len = vlen < 255 ? vlen : 255;
                            memcpy(row_str, (char *)values[col_idx] + 2, copy_len);
                            if (strcmp(where_op, "=") == 0)
                                match = strcmp(row_str, cond_val_str) == 0;
                        }
                    }
                }
            }

            if (match) {
                delete_row(page, slot);
                deleted++;
                dirty = 1;
            }
        }

        bm_unpin_page(&srv->bm, req->table_name, page_id, dirty);
    }

    char deleted_line[64];
    snprintf(deleted_line, sizeof(deleted_line), "OK\nDELETED %d\n", deleted);
    protocol_response_append(&rb, deleted_line);
    append_metrics(&rb, srv);
    protocol_response_append(&rb, "END\n");
    protocol_response_send(&rb, client_fd);
    protocol_response_free(&rb);
}

void handler_reset_metrics(Server *srv, int client_fd) {
    ResponseBuf rb;
    protocol_response_init(&rb);
    bm_reset_metrics(&srv->bm);
    protocol_response_append(&rb, "OK\n");
    append_metrics(&rb, srv);
    protocol_response_append(&rb, "END\n");
    protocol_response_send(&rb, client_fd);
    protocol_response_free(&rb);
}

void handler_update(Server *srv, int client_fd, Request *req) {
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

    if (req->payload_size <= 0) {
        protocol_response_append(&rb, "ERR INVALID_ARGS missing payload\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    if (get_num_pages(srv->data_dir, req->table_name) == 0) {
        protocol_response_append(&rb, "ERR TABLE_NOT_FOUND table does not exist\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    int row_id  = atoi(req->args);
    int page_id = decode_rowid_page(row_id);
    int slot_id = decode_rowid_slot(row_id);

    if (page_id <= 0) {
        protocol_response_append(&rb, "OK\nUPDATED 0\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    char *page = bm_fetch_page(&srv->bm, req->table_name, page_id);
    if (!page) {
        protocol_response_append(&rb, "OK\nUPDATED 0\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    PageHeader *header   = (PageHeader *)page;
    SlotEntry  *slot_dir = (SlotEntry *)(page + sizeof(PageHeader));

    if (slot_id < 0 || slot_id >= header->num_slots) {
        bm_unpin_page(&srv->bm, req->table_name, page_id, 0);
        protocol_response_append(&rb, "OK\nUPDATED 0\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    if (slot_dir[slot_id].state == SLOT_DELETED) {
        bm_unpin_page(&srv->bm, req->table_name, page_id, 0);
        protocol_response_append(&rb, "OK\nUPDATED 0\n");
        append_metrics(&rb, srv);
        protocol_response_append(&rb, "END\n");
        protocol_response_send(&rb, client_fd);
        protocol_response_free(&rb);
        return;
    }

    // Follow SLOT_REDIRECT chain to find the current live slot.
    // SLOT_REDIRECT.offset encodes the target as encode_rowid(page_id, slot_id).
    int depth = 0;
    while (slot_dir[slot_id].state == SLOT_REDIRECT) {
        if (++depth > 64) {
            bm_unpin_page(&srv->bm, req->table_name, page_id, 0);
            protocol_response_append(&rb, "OK\nUPDATED 0\n");
            append_metrics(&rb, srv);
            protocol_response_append(&rb, "END\n");
            protocol_response_send(&rb, client_fd);
            protocol_response_free(&rb);
            return;
        }

        int target   = slot_dir[slot_id].offset;
        int tgt_page = decode_rowid_page(target);
        int tgt_slot = decode_rowid_slot(target);

        if (tgt_page == page_id) {
            slot_id = tgt_slot;
        } else {
            bm_unpin_page(&srv->bm, req->table_name, page_id, 0);
            page_id  = tgt_page;
            page     = bm_fetch_page(&srv->bm, req->table_name, page_id);
            if (!page) {
                protocol_response_append(&rb, "OK\nUPDATED 0\n");
                append_metrics(&rb, srv);
                protocol_response_append(&rb, "END\n");
                protocol_response_send(&rb, client_fd);
                protocol_response_free(&rb);
                return;
            }
            header   = (PageHeader *)page;
            slot_dir = (SlotEntry *)(page + sizeof(PageHeader));
            slot_id  = tgt_slot;
        }

        if (slot_id < 0 || slot_id >= header->num_slots) {
            bm_unpin_page(&srv->bm, req->table_name, page_id, 0);
            protocol_response_append(&rb, "OK\nUPDATED 0\n");
            append_metrics(&rb, srv);
            protocol_response_append(&rb, "END\n");
            protocol_response_send(&rb, client_fd);
            protocol_response_free(&rb);
            return;
        }
    }

    // slot_dir[slot_id].state is SLOT_NORMAL — perform the update.
    int existing_size = get_row_size(page, slot_id);

    if (existing_size == req->payload_size) {
        // Same size: overwrite in place
        char *row_ptr = read_row(page, slot_id);
        if (!row_ptr) {
            bm_unpin_page(&srv->bm, req->table_name, page_id, 0);
            protocol_response_append(&rb, "OK\nUPDATED 0\n");
            append_metrics(&rb, srv);
            protocol_response_append(&rb, "END\n");
            protocol_response_send(&rb, client_fd);
            protocol_response_free(&rb);
            return;
        }
        memcpy(row_ptr, req->payload, req->payload_size);
        bm_unpin_page(&srv->bm, req->table_name, page_id, 1);
    } else {
        // Different size: HOT update — insert new row, redirect old slot
        int new_slot = insert_row(page, req->payload, req->payload_size);

        if (new_slot >= 0) {
            // Inserted on the same page
            slot_dir[slot_id].state  = SLOT_REDIRECT;
            slot_dir[slot_id].offset = encode_rowid(page_id, new_slot);
            bm_unpin_page(&srv->bm, req->table_name, page_id, 1);
        } else {
            // No space on this page — insert via heap (may create a new page)
            int new_row_id = heap_insert_bm(srv->data_dir, req->table_name,
                                             req->payload, req->payload_size,
                                             &srv->bm);
            if (new_row_id < 0) {
                bm_unpin_page(&srv->bm, req->table_name, page_id, 0);
                protocol_response_append(&rb, "ERR IO_ERROR failed to insert updated row\n");
                append_metrics(&rb, srv);
                protocol_response_append(&rb, "END\n");
                protocol_response_send(&rb, client_fd);
                protocol_response_free(&rb);
                return;
            }
            slot_dir[slot_id].state  = SLOT_REDIRECT;
            slot_dir[slot_id].offset = new_row_id;
            bm_unpin_page(&srv->bm, req->table_name, page_id, 1);
        }
    }

    protocol_response_append(&rb, "OK\nUPDATED 1\n");
    append_metrics(&rb, srv);
    protocol_response_append(&rb, "END\n");
    protocol_response_send(&rb, client_fd);
    protocol_response_free(&rb);
}