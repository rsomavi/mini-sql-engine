#ifndef DISK_H
#define DISK_H

#define PAGE_SIZE 4096

void write_page(const char *table, int page_id, char *page);
void load_page(const char *table, int page_id, char *page);
void read_page(const char *table, int page_id);
int get_num_pages(const char *table);

#endif /* DISK_H */
