#pragma once

#include "file_manager.h"
#include "bpt.h"

typedef struct buffer
{
    int table_id, is_dirty, is_pinned;
    struct buffer *next, *prev;
    pagenum_t page_num;
    page_t page;
} buffer;

buffer *head, *tail, *tend;

int init_db(int num_buf);
int close_table(int table_id);
int shutdown_db(void);

buffer* frame_delete(buffer *ptr);
buffer* frame_insert(buffer *ptr);

pagenum_t buffer_alloc_page(int table_id);
void buffer_free_page(int table_id, pagenum_t pagenum);
buffer* buffer_read_page(int table_id, pagenum_t pagenum, page_t* dest);
buffer* buffer_write_page(int table_id, pagenum_t pagenum, const page_t* src);