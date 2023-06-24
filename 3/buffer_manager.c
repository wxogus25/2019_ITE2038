#include "bpt.h"
#include "buffer_manager.h"

int init_db(int num_buf)
{
    if (num_buf <= 0)
        return -1;
    
    head = tail = tend = NULL;
    for (int i = 0; i < num_buf; ++i) {
        buffer* buffer1 = (buffer*)malloc(sizeof(buffer));
        if (buffer1 == NULL) return -1;
        buffer1->is_dirty = 0;
        buffer1->is_pinned = 0;
        buffer1->table_id = 0;
        buffer1->page_num = 0;
        buffer1->prev = NULL;
        buffer1->next = NULL;
        
        if (head == NULL) {
            head = buffer1;
        }
        
        if (tail != NULL)
        {
            tail->next = buffer1;
            buffer1->prev = tail;
        }
        tail = buffer1;
    }
    return 0;
}

int close_table(int table_id)
{
    buffer* ptr = head;
    while(tend != NULL && ptr != tend->next && ptr != NULL)
    {
        if (ptr->table_id == table_id)
        {
            if (ptr->is_pinned)
                return -1;
            ptr = frame_delete(ptr);
        }else
            ptr = ptr->next;
    }
    if (used_fd[table_id]) {
        used_fd[table_id] = 0;
        free(table_name[table_id]);
        fclose(fd[table_id]);
    }
    return 0;
}

int shutdown_db(void)
{
    if (head == NULL)
        return -1;
    int ck = 0;
    for (int i = 1; i <= 10 ; ++i) {
        if (used_fd[i]) {
            ck = close_table(i);
            if (ck == -1)
                return ck;
        }
    }
    buffer *ptr = head, *tmp;
    while(ptr != tail)
    {
        tmp = ptr->next;
        free(ptr);
        ptr = tmp;
    }
    free(ptr);
    return 0;
}

buffer* frame_delete(buffer *ptr)
{
    if (ptr == tend)
    {
        if (ptr == head)
            tend = NULL;
        else
            tend = ptr->prev;
        
        if (ptr->is_dirty)
            file_write_page(ptr->table_id, ptr->page_num, &ptr->page);
        
        return NULL;
    }
    buffer* tmp = ptr->next;
    if (ptr == head)
        head = ptr->next;
    else
        ptr->prev->next = ptr->next;
    
    ptr->next->prev = ptr->prev;
    ptr->next = tend->next;
    ptr->prev = tend;
    if (tend != tail)
        tend->next->prev = ptr;
    else
        tail = ptr;
    tend->next = ptr;
    
    if (ptr->is_dirty)
        file_write_page(ptr->table_id, ptr->page_num, &ptr->page);
    return tmp;
}

buffer* frame_insert(buffer *ptr)
{
    if (ptr == head) {
        if (tend == NULL)
            tend = ptr;
        return ptr;
    }
    if (ptr == tend)
        tend = ptr->prev;
    if (ptr != tail)
        ptr->next->prev = ptr->prev;
    else
        tail = ptr->prev;
    ptr->prev->next = ptr->next;
    ptr->next = head;
    head->prev = ptr;
    ptr->prev = NULL;
    head = ptr;
    
    return ptr;
}

pagenum_t buffer_alloc_page(int table_id)
{
    pagenum_t siz = page_size[table_id];
    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer* ptr;
    if (siz != 0)
    {
        buffer_read_page(table_id, 0, page);
        pagenum_t next_free_page = get_free_pagenum(page);
        if (next_free_page != 0)
        {
            ptr = buffer_read_page(table_id, next_free_page, page);
            ptr->is_dirty = 1;
            pagenum_t next = get_free_pagenum(page);
            buffer_read_page(table_id, 0, page);
            set_free_pagenum(next, page);
            buffer_write_page(table_id,0, page);
            free(page);
            return next_free_page;
        }
        set_number_of_page(siz + 1, page);
        buffer_write_page(table_id, 0, page);
    }else
        set_number_of_page(1, page);
    
    page_size[table_id]++;
    
    if (tend == tail)
        frame_delete(tend);
    
    if (tend == NULL)
        ptr = head;
    else
        ptr = tend->next;
    
    ptr->page_num = siz;
    ptr->table_id = table_id;
    ptr->is_dirty = 1;
    frame_insert(ptr);
    
    fseek(fd[table_id], 0, SEEK_END);
    fwrite(page, sizeof(page_t), 1, fd[table_id]);
    fflush(fd[table_id]);
    free(page);
    return siz;
}

void buffer_free_page(int table_id, pagenum_t pagenum)
{
    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, 0, page);
    pagenum_t freepage = get_free_pagenum(page);
    set_free_pagenum(pagenum, page);
    buffer_write_page(table_id, 0, page);
    buffer_read_page(0, pagenum, page);
    set_free_pagenum(freepage, page);
    buffer_write_page(table_id, pagenum, page);
    free(page);
}

buffer* buffer_read_page(int table_id, pagenum_t pagenum, page_t* dest)
{
    buffer *ptr = head;
    while(tend != NULL && ptr != tend->next)
    {
        if (ptr->table_id == table_id && ptr->page_num == pagenum)
        {
            memcpy(dest, &ptr->page, sizeof(page_t));
            frame_insert(ptr);
            return ptr;
        }
        ptr = ptr->next;
    }
    
    if (tail == tend)
        frame_delete(tend);
    
    ptr = tend == NULL ? head : tend->next;
    file_read_page(table_id, pagenum, &ptr->page);
    ptr->table_id = table_id;
    ptr->page_num = pagenum;
    ptr->is_dirty = 0;
    memcpy(dest, &ptr->page, sizeof(page_t));
    frame_insert(ptr);
    return ptr;
}

buffer* buffer_write_page(int table_id, pagenum_t pagenum, const page_t* src)
{
    buffer *ptr = head;
    while(tend != NULL && ptr != tend->next)
    {
        if (ptr->table_id == table_id && ptr->page_num == pagenum)
        {
            memcpy(&ptr->page, src, sizeof(page_t));
            ptr->is_dirty = 1;
            frame_insert(ptr);
            return ptr;
        }
        ptr = ptr->next;
    }
    
    if (tail == tend)
        frame_delete(tend);
    
    ptr = tend == NULL ? head : tend->next;
    ptr->table_id = table_id;
    ptr->page_num = pagenum;
    ptr->is_dirty = 1;
    memcpy(&ptr->page, src, sizeof(page_t));
    frame_insert(ptr);
    return ptr;
}