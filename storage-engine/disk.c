#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE 1024
#define PAGE_SIZE 4096

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

void write_page(const char *table, int page_id, const char *data) {
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
    
    char buffer[PAGE_SIZE];
    memset(buffer, 0, PAGE_SIZE);
    strncpy(buffer, data, PAGE_SIZE - 1);
    
    fwrite(buffer, 1, PAGE_SIZE, file);
    
    fclose(file);
}

int main(int argc, char *argv[]) {
    
    if (argc < 3) {
        fprintf(stderr, "Usage: disk read <table>\n");
        fprintf(stderr, "       disk read_page <table> <page_id>\n");
        fprintf(stderr, "       disk write_page <table> <page_id> <data>\n");
        return 1;
    }
    
    if (strcmp(argv[1], "read") == 0) {
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
