#ifndef PAGE_H
#define PAGE_H

#include "disk.h"

typedef struct {
    int num_slots;        // number of rows
    int free_space_start; // where slot directory ends
    int free_space_end;   // where row data starts
} PageHeader;

void init_page(char *page);
int insert_row(char *page, const void *data, int data_size);
void delete_row(char *page, int slot_id);
char* read_row(char *page, int slot_id);
int get_row_size(char *page, int slot_id);
void print_page(char *page);

#endif /* PAGE_H */
