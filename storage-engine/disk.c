#include <stdio.h>
#include <stdlib.h>
#include "disk.h"

#define MAX_LINE 1024

// ============================================================================
// Disk I/O Functions
// ============================================================================

void read_page(const char *table, int page_id) {
    char filename[256];
    sprintf(filename, "../data/%s.db", table);
    
    FILE *file = fopen(filename, "rb");
    
    if (!file) {
        fprintf(stderr, "ERROR: file not found\n");
        exit(1);
    }
    
    char buffer[PAGE_SIZE];
    
    fseek(file, page_id * PAGE_SIZE, SEEK_SET);
    
    size_t bytes_read = fread(buffer, 1, PAGE_SIZE, file);
    
    if (bytes_read == 0) {
        fprintf(stderr, "ERROR: empty page or out of bounds\n");
        fclose(file);
        exit(1);
    }
    
    fwrite(buffer, 1, bytes_read, stdout);
    
    fclose(file);
}

void write_page(const char *table, int page_id, char *page) {
    char filename[256];
    sprintf(filename, "../data/%s.db", table);
    
    // Open in read+binary mode, create if doesn't exist
    FILE *file = fopen(filename, "rb+");
    if (!file) {
        file = fopen(filename, "wb");
        if (!file) {
            fprintf(stderr, "ERROR: cannot create file\n");
            exit(1);
        }
        fclose(file);
        file = fopen(filename, "rb+");
    }
    
    fseek(file, page_id * PAGE_SIZE, SEEK_SET);
    
    // Ensure full write
    size_t written = fwrite(page, 1, PAGE_SIZE, file);
    if (written != PAGE_SIZE) {
        fprintf(stderr, "ERROR: failed to write full page\n");
        fclose(file);
        exit(1);
    }
    
    fclose(file);
}

void load_page(const char *table, int page_id, char *page) {
    char filename[256];
    sprintf(filename, "../data/%s.db", table);
    
    FILE *file = fopen(filename, "rb");
    if (!file) {
        fprintf(stderr, "ERROR: file not found\n");
        exit(1);
    }
    
    // Validate page_id bounds
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    int num_pages = file_size / PAGE_SIZE;
    
    if (page_id >= num_pages) {
        fprintf(stderr, "ERROR: page_id %d out of bounds (max: %d)\n", page_id, num_pages - 1);
        fclose(file);
        exit(1);
    }
    
    fseek(file, page_id * PAGE_SIZE, SEEK_SET);
    
    size_t bytes_read = fread(page, 1, PAGE_SIZE, file);
    
    if (bytes_read != PAGE_SIZE) {
        fprintf(stderr, "ERROR: incomplete read (%zu bytes)\n", bytes_read);
        fclose(file);
        exit(1);
    }
    
    fclose(file);
}

int get_num_pages(const char *table) {
    char filename[256];
    sprintf(filename, "../data/%s.db", table);
    
    FILE *file = fopen(filename, "rb");
    if (!file) {
        return 0;  // File does not exist
    }
    
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    fclose(file);
    
    if (file_size <= 0) {
        return 0;  // Empty file
    }
    
    // Validate file integrity
    if (file_size % PAGE_SIZE != 0) {
        fprintf(stderr, "ERROR: corrupted table file (invalid size %ld)\n", file_size);
        exit(1);
    }
    
    return file_size / PAGE_SIZE;
}
