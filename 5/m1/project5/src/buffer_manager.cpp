#include "buffer_manager.h"

buffer *head, *tail, *tend;
#ifdef MAP
std::map<std::pair<int, pagenum_t>, buffer*> bufmap;
#endif

int init_db(int num_buf)
{
    if (num_buf <= 0)
        return -1;
    
//    ull prime = 0x1999999999999993;
//    hash = (buffer **)malloc(num_buf * sizeof(buffer*));
    
    head = tail = tend = nullptr;
    for (int i = 0; i < num_buf; ++i) {
        buffer *buffer1;
        buffer1 = (buffer *) malloc(sizeof(buffer));
        if (buffer1 == nullptr) return -1;
        buffer1->is_dirty = 0;
        buffer1->is_inlist = 0;
        buffer1->pin = 0;
        buffer1->table_id = 0;
        buffer1->page_num = 0;
        buffer1->prev = nullptr;
        buffer1->next = nullptr;
        
        if (head == nullptr)
            head = buffer1;
        
        if (tail != nullptr) {
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
    while(tend != nullptr && ptr != nullptr && ptr != tend->next)
    {
        if (ptr->table_id == table_id)
        {
            if (ptr->pin != 0) {
                puts("in close_table, buffer is pinned");
                printf("table_id : %d, pagenum : %lld\n", table_id, ptr->page_num);
                return -1;
            }
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

int shutdown_db()
{
    if (head == nullptr)
        return -1;
    buffer *ptr = head, *tmp;
    while(ptr != nullptr)
    {
        tmp = ptr->next;
        if (ptr->is_dirty)
            file_write_page(ptr->table_id, ptr->page_num, &ptr->page);
        if (ptr->pin != 0)
        {
            puts("in close_table, buffer is pinned");
            printf("table_id : %d, pagenum : %lld\n", ptr->table_id, ptr->page_num);
            return -1;
        }
        free(ptr);
        ptr = tmp;
    }
    return 0;
}

buffer* frame_delete(buffer *ptr)
{
#ifdef MAP
    bufmap.erase({ptr->table_id, ptr->page_num});
#endif
    ptr->is_inlist = 0;
    if (ptr->is_dirty)
        file_write_page(ptr->table_id, ptr->page_num, &ptr->page);
    ptr->is_dirty = 0;
    if (ptr == tend)
    {
        if (ptr == head)
            tend = nullptr;
        else
            tend = ptr->prev;
        return nullptr;
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
    return tmp;
}

buffer* frame_insert(buffer *ptr)
{
#ifdef MAP
    if (ptr->is_inlist == 0)
        bufmap.insert({{ptr->table_id, ptr->page_num}, ptr});
#endif
    ptr->is_inlist = 1;
    if (ptr == head) {
        if (tend == nullptr)
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
    ptr->prev = nullptr;
    head = ptr;
    
    return ptr;
}

pagenum_t buffer_alloc_page(int table_id)
{
    pagenum_t siz = page_size[table_id];
    buffer *ptr, *header, *freepage;
    page_t *new_page;
    new_page = (page_t *)malloc(sizeof(page_t));
    if (siz != 0)
    {
        header = buffer_read_page(table_id, 0);
        pagenum_t next_free_page = get_free_pagenum(&header->page);
        if (next_free_page != 0)
        {
            freepage = buffer_read_page(table_id, next_free_page);
            pagenum_t next = get_free_pagenum(&freepage->page);
            --(freepage->pin);
            set_free_pagenum(next, &header->page);
            --(header->pin);
            buffer_write(freepage);
            buffer_write(header);
            return next_free_page;
        }
        set_number_of_page(siz + 1, &header->page);
        --(header->pin);
        buffer_write(header);
    }else
        set_number_of_page(1, new_page);
    
    page_size[table_id]++;
    
    if (tail == tend) {
        ptr = tend;
        while (ptr != nullptr && ptr->pin != 0)
            ptr = ptr->prev;
        if(ptr == nullptr) {
            puts("in buffer_alloc_page, all buffer is pinned");
            exit(EXIT_FAILURE);
        }
        frame_delete(ptr);
    }
    
    if (tend == nullptr)
        ptr = head;
    else
        ptr = tend->next;
    
    memcpy(&ptr->page, new_page, sizeof(page_t));
    free(new_page);
    ptr->page_num = siz;
    ptr->table_id = table_id;
    buffer_write(ptr);
//    fseek(fd[table_id], 0, SEEK_END);
//    fwrite(new_page, sizeof(page_t), 1, fd[table_id]);
//    fflush(fd[table_id]);
    return siz;
}

void buffer_free_page(int table_id, buffer *ptr)
{
    buffer* buffer1;
    buffer1 = buffer_read_page(table_id, 0);
    pagenum_t freepage = get_free_pagenum(&buffer1->page);
    set_free_pagenum(ptr->page_num, &buffer1->page);
    --(buffer1->pin);
    buffer_write(buffer1);
    set_free_pagenum(freepage, &ptr->page);
    buffer_write(ptr);
}

buffer* buffer_read_page(int table_id, pagenum_t pagenum)
{
    buffer *ptr = head;
#ifdef LINEAR_SEARCH
    while(tend != nullptr && ptr != nullptr && ptr != tend->next)
    {
        if (ptr->table_id == table_id && ptr->page_num == pagenum)
        {
            ++(ptr->pin);
            frame_insert(ptr);
            return ptr;
        }
        ptr = ptr->next;
    }
#endif
#ifdef MAP
    std::map<std::pair<int, pagenum_t>, buffer*>::iterator iter;
    iter = bufmap.find({table_id, pagenum});
    if (iter != bufmap.end())
    {
        ptr = iter->second;
        ++(ptr->pin);
        frame_insert(ptr);
        return ptr;
    }
#endif
    if (tail == tend) {
        ptr = tend;
        while (ptr != nullptr && ptr->pin != 0)
            ptr = ptr->prev;
        if(ptr == nullptr) {
            puts("in buffer_read_page, all buffer is pinned");
            exit(EXIT_FAILURE);
        }
        frame_delete(ptr);
    }
    
    ptr = tend == nullptr ? head : tend->next;
    file_read_page(table_id, pagenum, &ptr->page);
    ptr->table_id = table_id;
    ptr->page_num = pagenum;
    ptr->is_dirty = 0;
    ++(ptr->pin);
    frame_insert(ptr);
    return ptr;
}

int buffer_pin_check()
{
    buffer *ptr = head;
    int cnt = 0;
    while (ptr != tend->next)
    {
        if (ptr->pin != 0)
            cnt++;
        ptr = ptr->next;
    }
    return cnt;
}

void buffer_write(buffer *ptr)
{
    ptr->is_dirty = 1;
    frame_insert(ptr);
}

//void buffer_write_page(int table_id, pagenum_t pagenum, const page_t *src)
//{
//    buffer *ptr = head;
//    while(tend != nullptr && ptr != nullptr && ptr != tend->next)
//    {
//        if (ptr->table_id == table_id && ptr->page_num == pagenum)
//        {
//            memcpy(&ptr->page, src, sizeof(page_t));
//            ptr->is_dirty = 1;
//            frame_insert(ptr);
//            return;
//        }
//        ptr = ptr->next;
//    }
//
//    if (tail == tend) {
//        ptr = tend;
//        while (ptr != nullptr && ptr->is_pinned)
//            ptr = ptr->prev;
//        if(ptr == nullptr) {
//            puts("in buffer_write_page, all buffer is pinned");
//            exit(EXIT_FAILURE);
//        }
//        frame_delete(ptr);
//    }
//
//    ptr = tend == nullptr ? head : tend->next;
//    ptr->table_id = table_id;
//    ptr->page_num = pagenum;
//    ptr->is_dirty = 1;
//    memcpy(&ptr->page, src, sizeof(page_t));
//    frame_insert(ptr);
//}