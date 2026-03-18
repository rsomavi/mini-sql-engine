#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "page.h"

// ============================================================================
// Slotted Page Functions
// ============================================================================

void init_page(char *page) {
    memset(page, 0, PAGE_SIZE);
    PageHeader *header = (PageHeader *)page;
    header->num_slots = 0;
    header->free_space_start = sizeof(PageHeader);
    header->free_space_end = PAGE_SIZE;
}

int insert_row(char *page, const void *data, int data_size) {
    PageHeader *header = (PageHeader *)page;
    
    int total_size = sizeof(int) + data_size;  // size header + data
    
    // Calculate available space
    int available = header->free_space_end - header->free_space_start;
    int required = total_size + sizeof(int);  // row + slot entry
    
    if (available < required) {
        return -1;  // Not enough space
    }
    
    // Check for deleted slot to reuse
    int *slot_dir = (int *)(page + sizeof(PageHeader));
    int slot_index = -1;
    
    for (int i = 0; i < header->num_slots; i++) {
        if (slot_dir[i] == -1) {
            slot_index = i;
            break;
        }
    }
    
    // If no deleted slot found, append new one
    if (slot_index == -1) {
        slot_index = header->num_slots;
    }
    
    // Insert row: decrease free_space_end by total_size
    header->free_space_end -= total_size;
    
    // Write data_size at the start of the row using memcpy
    memcpy(page + header->free_space_end, &data_size, sizeof(int));
    
    // Write row_data immediately after the size using memcpy
    memcpy(page + header->free_space_end + sizeof(int), data, data_size);
    
    // Add slot: store offset at free_space_start
    slot_dir[slot_index] = header->free_space_end;
    
    // Update header (only if appending new slot)
    if (slot_index == header->num_slots) {
        header->num_slots++;
        header->free_space_start += sizeof(int);
    }
    
    return slot_index;
}

void delete_row(char *page, int slot_id) {
    PageHeader *header = (PageHeader *)page;
    
    // Validate slot_id
    if (slot_id >= header->num_slots || slot_id < 0) {
        return;  // Invalid slot
    }
    
    // Access slot directory and set to -1
    int *slot_dir = (int *)(page + sizeof(PageHeader));
    slot_dir[slot_id] = -1;
}

char* read_row(char *page, int slot_id) {
    PageHeader *header = (PageHeader *)page;
    
    if (slot_id >= header->num_slots || slot_id < 0) {
        return NULL;  // Invalid slot
    }
    
    int *slot_dir = (int *)(page + sizeof(PageHeader));
    int offset = slot_dir[slot_id];
    
    // Check if slot is deleted
    if (offset == -1) {
        return NULL;
    }
    
    // Return pointer to row_data (skip size header)
    return page + offset + sizeof(int);
}

int get_row_size(char *page, int slot_id) {
    PageHeader *header = (PageHeader *)page;
    
    if (slot_id >= header->num_slots || slot_id < 0) {
        return -1;  // Invalid slot
    }
    
    int *slot_dir = (int *)(page + sizeof(PageHeader));
    int offset = slot_dir[slot_id];
    
    // Check if slot is deleted
    if (offset == -1) {
        return -1;
    }
    
    // Read row_size from the beginning of the row using memcpy
    int size;
    memcpy(&size, page + offset, sizeof(int));
    return size;
}

void print_page(char *page) {
    PageHeader *header = (PageHeader *)page;
    // Compute slot_dir once
    int *slot_dir = (int *)(page + sizeof(PageHeader));
    
    printf("=== Page Header ===\n");
    printf("num_slots: %d\n", header->num_slots);
    printf("free_space_start: %d\n", header->free_space_start);
    printf("free_space_end: %d\n", header->free_space_end);
    printf("free_space: %d bytes\n\n", header->free_space_end - header->free_space_start);
    
    printf("=== Rows ===\n");
    for (int i = 0; i < header->num_slots; i++) {
        int offset = slot_dir[i];
        
        if (offset == -1) {
            printf("slot %d → DELETED\n", i);
        } else {
            int row_size = get_row_size(page, i);
            char *row = read_row(page, i);
            if (row) {
                // Print row using size (binary-safe)
                printf("slot %d → size=%d data=", i, row_size);
                for (int j = 0; j < row_size; j++) {
                    printf("%c", row[j]);
                }
                printf("\n");
            }
        }
    }
}
