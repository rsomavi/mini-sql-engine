#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "heap.h"

#define DEFAULT_DATA_DIR "../data"

// ============================================================================
// Heap File Functions
// ============================================================================

int insert_into_table(const char *data_dir, const char *table, const void *data, int size) {
    const char *dir = data_dir ? data_dir : DEFAULT_DATA_DIR;
    char page[PAGE_SIZE];
    int num_pages = get_num_pages(dir, table);
    
    // Page 0 is reserved for schema, start scanning from page 1
    int start_page = 1;
    
    // Try to insert into existing pages (starting from page 1)
    for (int page_id = start_page; page_id < num_pages; page_id++) {
        load_page(dir, table, page_id, page);
        
        int slot_id = insert_row(page, data, size);
        if (slot_id >= 0) {
            write_page(dir, table, page_id, page);
            return encode_rowid(page_id, slot_id);
        }
    }
    
    // No page has space, create new page (starting from page 1 if no pages exist)
    int new_page_id = (num_pages >= 1) ? num_pages : 1;
    init_page(page);
    int slot_id = insert_row(page, data, size);
    
    // Validate insert_row on new page
    if (slot_id < 0) {
        fprintf(stderr, "ERROR: row too large to fit in page\n");
        exit(1);
    }
    
    write_page(dir, table, new_page_id, page);
    
    return encode_rowid(new_page_id, slot_id);
}

void scan_table(const char *data_dir, const char *table) {
    const char *dir = data_dir ? data_dir : DEFAULT_DATA_DIR;
    int num_pages = get_num_pages(dir, table);
    
    printf("=== Scanning table '%s' (%d pages) ===\n\n", table, num_pages);
    
    char page[PAGE_SIZE];
    
    // Page 0 is reserved for schema, start scanning from page 1
    for (int page_id = 1; page_id < num_pages; page_id++) {
        printf("--- Page %d ---\n", page_id);
        
        load_page(dir, table, page_id, page);
        
        PageHeader *header = (PageHeader *)page;
        printf("num_slots: %d\n", header->num_slots);
        
        // Compute slot_dir once
        int *slot_dir = (int *)(page + sizeof(PageHeader));
        
        for (int slot_id = 0; slot_id < header->num_slots; slot_id++) {
            if (slot_dir[slot_id] == -1) {
                printf("  slot %d: DELETED\n", slot_id);
                continue;
            }
            
            int row_size = get_row_size(page, slot_id);
            char *row = read_row(page, slot_id);
            
            if (row && row_size > 0) {
                printf("  slot %d (size=%d): ", slot_id, row_size);
                for (int i = 0; i < row_size; i++) {
                    printf("%c", row[i]);
                }
                printf("\n");
            }
        }
        printf("\n");
    }
}

int scan_table_raw(const char *data_dir, const char *table,
                   char **rows_out, int *sizes_out, int max_rows) {
    if (!data_dir || !table || !rows_out || !sizes_out || max_rows <= 0)
        return -1;

    const char *dir = data_dir ? data_dir : DEFAULT_DATA_DIR;
    int num_pages   = get_num_pages(dir, table);
    int num_rows    = 0;

    static char page_bufs[1024][PAGE_SIZE];  // static — safe for single-threaded v1
    int page_buf_idx = 0;

    for (int page_id = 1; page_id < num_pages && num_rows < max_rows; page_id++) {
        if (page_buf_idx >= 1024) break;

        char *page = page_bufs[page_buf_idx++];
        load_page(dir, table, page_id, page);

        PageHeader *header  = (PageHeader *)page;
        int        *slot_dir = (int *)(page + sizeof(PageHeader));

        for (int slot_id = 0; slot_id < header->num_slots && num_rows < max_rows; slot_id++) {
            if (slot_dir[slot_id] == -1) continue;  // deleted slot

            int   row_size = get_row_size(page, slot_id);
            char *row      = read_row(page, slot_id);

            if (row && row_size > 0) {
                rows_out[num_rows]  = row;
                sizes_out[num_rows] = row_size;
                num_rows++;
            }
        }
    }

    return num_rows;
}

int heap_insert_bm(const char *data_dir, const char *table_name,
                   const void *data, int size, BufferManager *bm) {
    if (!data_dir || !table_name || !data || size <= 0 || !bm)
        return -1;

    int num_pages = get_num_pages(data_dir, table_name);
    printf("[heap_insert_bm] table=%s num_pages=%d size=%d\n",
           table_name, num_pages, size);


    // Primera pasada — buscar espacio en páginas existentes
    for (int page_id = 1; page_id < num_pages; page_id++) {
        char *page = bm_fetch_page(bm, table_name, page_id);
        if (!page) continue;
        printf("[heap_insert_bm] fetch page_id=%d page=%p\n", page_id, page);

        int slot_id = insert_row(page, data, size);

        if (slot_id >= 0) {
            bm_unpin_page(bm, table_name, page_id, 1);  // dirty
            return encode_rowid(page_id, slot_id);
        }

        bm_unpin_page(bm, table_name, page_id, 0);  // no dirty
    }

    // Ninguna página tiene espacio — crear página nueva en disco primero
    int new_page_id = (num_pages >= 1) ? num_pages : 1;
    printf("[heap_insert_bm] creating new page_id=%d\n", new_page_id);

    // Inicializar y escribir página vacía en disco para que load_page no falle
    char empty_page[PAGE_SIZE];
    init_page(empty_page);
    int wr = write_page(data_dir, table_name, new_page_id, empty_page);
    printf("[heap_insert_bm] write_page result=%d\n", wr);

    // Ahora sí podemos cargarla via buffer pool
    char *page = bm_fetch_page(bm, table_name, new_page_id);
    printf("[heap_insert_bm] fetch new page=%p\n", page);
    if (!page) return -1;

    int slot_id = insert_row(page, data, size);

    if (slot_id < 0) {
        bm_unpin_page(bm, table_name, new_page_id, 0);
        return -1;
    }

    bm_unpin_page(bm, table_name, new_page_id, 1);  // dirty
    return encode_rowid(new_page_id, slot_id);
}

void debug_print_table(const char *data_dir, const char *table) {
    const char *dir = data_dir ? data_dir : DEFAULT_DATA_DIR;
    int num_pages = get_num_pages(dir, table);
    
    printf("=== Debug: Table '%s' (%d pages) ===\n\n", table, num_pages);
    
    char page[PAGE_SIZE];
    
    // Page 0 is reserved for schema, start scanning from page 1
    for (int page_id = 1; page_id < num_pages; page_id++) {
        printf("=== Page %d ===\n", page_id);
        load_page(dir, table, page_id, page);
        print_page(page);
        printf("\n");
    }
}

int heap_delete_bm(const char *data_dir, const char *table_name,
                   BufferManager *bm,
                   int (*predicate)(const char *row, int size, void *ctx),
                   void *ctx) {
    if (!data_dir || !table_name || !bm || !predicate)
        return -1;

    int num_pages = get_num_pages(data_dir, table_name);
    int deleted   = 0;

    for (int page_id = 1; page_id < num_pages; page_id++) {
        char *page = bm_fetch_page(bm, table_name, page_id);
        if (!page) continue;

        PageHeader *header   = (PageHeader *)page;
        int        *slot_dir = (int *)(page + sizeof(PageHeader));
        int         dirty    = 0;

        for (int slot_id = 0; slot_id < header->num_slots; slot_id++) {
            if (slot_dir[slot_id] == -1) continue;

            int   row_size = get_row_size(page, slot_id);
            char *row      = read_row(page, slot_id);
            if (!row || row_size <= 0) continue;

            if (predicate(row, row_size, ctx)) {
                delete_row(page, slot_id);
                deleted++;
                dirty = 1;
            }
        }

        bm_unpin_page(bm, table_name, page_id, dirty);
    }

    return deleted;
}