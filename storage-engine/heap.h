#ifndef HEAP_H
#define HEAP_H

#include "page.h"
#include "disk.h"
#include "buffer_manager.h"

// RowID encoding/decoding helpers
static inline int encode_rowid(int page_id, int slot_id) {
    return (page_id << 16) | slot_id;
}

static inline int decode_rowid_page(int rowid) {
    return rowid >> 16;
}

static inline int decode_rowid_slot(int rowid) {
    return rowid & 0xFFFF;
}

int insert_into_table(const char *data_dir, const char *table, const void *data, int size);
void scan_table(const char *data_dir, const char *table);

// Scan all rows from a table and return them as raw binary data.
// rows_out:  array of pointers to row data (caller must NOT free — points into pages)
// sizes_out: array of row sizes in bytes
// max_rows:  maximum number of rows to return
// Returns the number of rows found, or -1 on error.
//
// NOTE: This function uses load_page directly (no buffer manager).
//       The server's handler_scan is responsible for going through bm_fetch_page.
int scan_table_raw(const char *data_dir, const char *table,
                   char **rows_out, int *sizes_out, int max_rows);

// Insert a row via buffer pool — page stays in cache marked dirty
// Returns RowID = (page_id << 16) | slot_id, or -1 on error
int heap_insert_bm(const char *data_dir, const char *table_name,
                   const void *data, int size, BufferManager *bm);

int heap_delete_bm(const char *data_dir, const char *table_name,
                   BufferManager *bm,
                   int (*predicate)(const char *row, int size, void *ctx),
                   void *ctx);
                   
void debug_print_table(const char *data_dir, const char *table);



#endif /* HEAP_H */
