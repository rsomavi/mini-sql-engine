#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "heap.h"

// ============================================================================
// Heap File Functions
// ============================================================================

int insert_into_table(const char *table, const void *data, int size) {
    char page[PAGE_SIZE];
    int num_pages = get_num_pages(table);
    
    // Try to insert into existing pages
    for (int page_id = 0; page_id < num_pages; page_id++) {
        load_page(table, page_id, page);
        
        int slot_id = insert_row(page, data, size);
        if (slot_id >= 0) {
            write_page(table, page_id, page);
            return encode_rowid(page_id, slot_id);
        }
    }
    
    // No page has space, create new page
    int new_page_id = num_pages;
    init_page(page);
    int slot_id = insert_row(page, data, size);
    
    // Validate insert_row on new page
    if (slot_id < 0) {
        fprintf(stderr, "ERROR: row too large to fit in page\n");
        exit(1);
    }
    
    write_page(table, new_page_id, page);
    
    return encode_rowid(new_page_id, slot_id);
}

void scan_table(const char *table) {
    int num_pages = get_num_pages(table);
    
    printf("=== Scanning table '%s' (%d pages) ===\n\n", table, num_pages);
    
    char page[PAGE_SIZE];
    
    for (int page_id = 0; page_id < num_pages; page_id++) {
        printf("--- Page %d ---\n", page_id);
        
        load_page(table, page_id, page);
        
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

void debug_print_table(const char *table) {
    int num_pages = get_num_pages(table);
    
    printf("=== Debug: Table '%s' (%d pages) ===\n\n", table, num_pages);
    
    char page[PAGE_SIZE];
    
    for (int page_id = 0; page_id < num_pages; page_id++) {
        printf("=== Page %d ===\n", page_id);
        load_page(table, page_id, page);
        print_page(page);
        printf("\n");
    }
}
