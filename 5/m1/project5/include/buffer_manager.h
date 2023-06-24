#include "file_manager.h"

typedef struct buffer
{
    int table_id, is_dirty, is_inlist, pin;
    struct buffer *next, *prev;
    pagenum_t page_num;
    page_t page;
} buffer;

extern buffer *head, *tail, *tend;
#ifdef MAP
extern std::map<std::pair<int, pagenum_t>, buffer*> bufmap;
#endif

int init_db(int num_buf);
int close_table(int table_id);
int shutdown_db();

buffer* frame_delete(buffer *ptr);
buffer* frame_insert(buffer *ptr);

pagenum_t buffer_alloc_page(int table_id);
void buffer_free_page(int table_id, buffer *ptr);
buffer* buffer_read_page(int table_id, pagenum_t pagenum);
int buffer_pin_check();
void buffer_write(buffer *ptr);
//void buffer_write_page(int table_id, pagenum_t pagenum, const page_t* src);