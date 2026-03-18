#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "disk.h"
#include "page.h"
#include "heap.h"

#define MAX_LINE 1024

// ============================================================================
// Helper Functions
// ============================================================================

void read_table(const char *table) {
    char filename[256];
    sprintf(filename, "../data/%s.tbl", table);
    
    FILE *file = fopen(filename, "r");
    
    if (!file) {
        fprintf(stderr, "ERROR: table not found\n");
        exit(1);
    }
    
    char line[MAX_LINE];
    
    while (fgets(line, MAX_LINE, file)) {
        printf("%s", line);
    }
    
    fclose(file);
}

// ============================================================================
// Test Functions
// ============================================================================

void test_disk_page() {
    char page[PAGE_SIZE];
    
    // init page
    init_page(page);
    
    // insert rows
    insert_row(page, "row1", 5);
    insert_row(page, "hello world", 12);
    insert_row(page, "abc", 4);
    
    // delete slot 1
    delete_row(page, 1);
    
    // write page to disk
    write_page("users", 0, page);
    
    printf("Page written to disk\n");
    
    // clear memory
    memset(page, 0, PAGE_SIZE);
    
    // load page from disk
    load_page("users", 0, page);
    
    printf("Page loaded from disk\n\n");
    
    // print page structure
    print_page(page);
}

void test_page() {
    char page[PAGE_SIZE];
    
    // Initialize page
    init_page(page);
    printf("=== Testing Slotted Page ===\n\n");
    
    // Insert rows (binary-safe: null terminator NOT included in data_size)
    insert_row(page, "row1", 4);
    insert_row(page, "hello world", 12);
    insert_row(page, "abc", 4);
    
    printf("=== Before Delete ===\n");
    print_page(page);
    
    printf("\n=== Delete slot 1 ===\n");
    delete_row(page, 1);
    print_page(page);
    
    printf("\n=== Reuse deleted slot ===\n");
    insert_row(page, "newrow", 7);
    print_page(page);
    
    printf("\n=== Reading individual rows ===\n");
    for (int i = 0; i < 4; i++) {
        char *row = read_row(page, i);
        if (row) {
            int size = get_row_size(page, i);
            printf("read_row(page, %d) = ", i);
            for (int j = 0; j < size; j++) {
                printf("%c", row[j]);
            }
            printf("\n");
        } else {
            printf("read_row(page, %d) = NULL (deleted)\n", i);
        }
    }
}

void test_heap_file() {
    printf("=== Testing Heap File ===\n\n");
    
    // Insert rows
    int id1 = insert_into_table("users", "row1", 4);
    printf("Inserted row1, RowID=%d (page=%d, slot=%d)\n", 
           id1, decode_rowid_page(id1), decode_rowid_slot(id1));
    
    int id2 = insert_into_table("users", "row2", 4);
    printf("Inserted row2, RowID=%d (page=%d, slot=%d)\n", 
           id2, decode_rowid_page(id2), decode_rowid_slot(id2));
    
    int id3 = insert_into_table("users", "row3", 4);
    printf("Inserted row3, RowID=%d (page=%d, slot=%d)\n\n", 
           id3, decode_rowid_page(id3), decode_rowid_slot(id3));
    
    // Scan table
    scan_table("users");
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    
    if (argc < 2) {
        fprintf(stderr, "Usage: disk read <table>\n");
        fprintf(stderr, "       disk read_page <table> <page_id>\n");
        fprintf(stderr, "       disk write_page <table> <page_id> <data>\n");
        fprintf(stderr, "       disk test_page\n");
        fprintf(stderr, "       disk test_disk_page\n");
        fprintf(stderr, "       disk test_heap_file\n");
        fprintf(stderr, "       disk scan <table>\n");
        fprintf(stderr, "       disk debug <table>\n");
        return 1;
    }
    
    if (strcmp(argv[1], "test_page") == 0) {
        test_page();
    }
    else if (strcmp(argv[1], "test_disk_page") == 0) {
        test_disk_page();
    }
    else if (strcmp(argv[1], "test_heap_file") == 0) {
        test_heap_file();
    }
    else if (strcmp(argv[1], "scan") == 0) {
        scan_table(argv[2]);
    }
    else if (strcmp(argv[1], "debug") == 0) {
        debug_print_table(argv[2]);
    }
    else if (argc < 3) {
        fprintf(stderr, "Error: missing table name\n");
        return 1;
    }
    else if (strcmp(argv[1], "read") == 0) {
        read_table(argv[2]);
    }
    else if (strcmp(argv[1], "read_page") == 0) {
        if (argc < 4) {
            fprintf(stderr, "Usage: disk read_page <table> <page_id>\n");
            return 1;
        }
        int page_id = atoi(argv[3]);
        read_page(argv[2], page_id);
    }
    else if (strcmp(argv[1], "write_page") == 0) {
        if (argc < 5) {
            fprintf(stderr, "Usage: disk write_page <table> <page_id> <data>\n");
            return 1;
        }
        int page_id = atoi(argv[3]);
        write_page(argv[2], page_id, argv[4]);
    }
    
    return 0;
}