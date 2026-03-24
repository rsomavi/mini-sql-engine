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
