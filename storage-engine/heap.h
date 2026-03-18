#ifndef HEAP_H
#define HEAP_H

#include "page.h"
#include "disk.h"

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

int insert_into_table(const char *table, const void *data, int size);
void scan_table(const char *table);
void debug_print_table(const char *table);

#endif /* HEAP_H */
