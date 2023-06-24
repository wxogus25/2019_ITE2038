#include "file_manager.h"

FILE* fd[11];
int used_fd[11], page_size[11];
char* table_name[11];

pagenum_t get_0byte_to_pagenum_t(page_t* page)
{
    return *(pagenum_t*)(page->d);
}
pagenum_t get_120byte_to_pagenum_t(page_t* page)
{
    return *(pagenum_t*)(page->d + 120);
}

void set_0byte_to_pagenum_t(pagenum_t pagenum, page_t* page)
{
    *(pagenum_t*)(page->d) = pagenum;
}
void set_120byte_to_pagenum_t(pagenum_t pagenum, page_t* page)
{
    *(pagenum_t*)(page->d + 120) = pagenum;
}

pagenum_t (*get_free_pagenum)(page_t*) = get_0byte_to_pagenum_t;
void (*set_free_pagenum)(pagenum_t, page_t*) = set_0byte_to_pagenum_t;
pagenum_t (*get_parent_pagenum)(page_t*) = get_0byte_to_pagenum_t;
void (*set_parent_pagenum)(pagenum_t, page_t*) = set_0byte_to_pagenum_t;
pagenum_t (*get_right_sibling_pagenum)(page_t*) = get_120byte_to_pagenum_t;
void (*set_right_sibling_pagenum)(pagenum_t, page_t*) = set_120byte_to_pagenum_t;
pagenum_t (*get_one_more_pagenum)(page_t*) = get_120byte_to_pagenum_t;
void (*set_one_more_pagenum)(pagenum_t, page_t*) = set_120byte_to_pagenum_t;

// Allocate an on-disk page from the free page list
//pagenum_t file_alloc_page(int table_id)
//{
//    page_t* temp = (page_t*)malloc(sizeof(page_t));
//    fseek(fd, 0, SEEK_END);
//    pagenum_t offset = ftell(fd), siz;
//    siz = offset / (pagenum_t )sizeof(page_t);
//    if (siz != 0) {
//        file_read_page(0, temp);
//        pagenum_t next_free_page = get_free_pagenum(temp);
//        if (next_free_page != 0) {
//            file_read_page(next_free_page, temp);
//            pagenum_t next = get_free_pagenum(temp);
//            file_read_page(0, temp);
//            set_free_pagenum(next, temp);
//            file_write_page(0, temp);
//            free(temp);
//            return next_free_page;
//        }
//        set_number_of_page(siz + 1, temp);
//        file_write_page(0, temp);
//    }
//    fseek(fd, 0, SEEK_END);
//    fwrite(temp, sizeof(page_t), 1, fd);
//    fflush(fd);
//    free(temp);
//    return offset / (pagenum_t )sizeof(page_t);
//}

// Free an on-disk page to the free page list
//void file_free_page(int table_id, pagenum_t pagenum)
//{
//    page_t* page = malloc(sizeof(page_t));
//    file_read_page(0, page);
//    pagenum_t freepage = get_free_pagenum(page);
//    set_free_pagenum(pagenum, page);
//    file_write_page(0, page);
//    file_read_page(pagenum, page);
//    set_free_pagenum(freepage, page);
//    file_write_page(pagenum, page);
//    free(page);
//}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int table_id, pagenum_t pagenum, page_t* dest)
{
    fseek(fd[table_id], pagenum * (pagenum_t)sizeof(page_t), SEEK_SET);
    fread(dest, sizeof(page_t), 1, fd[table_id]);
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src)
{
    fseek(fd[table_id], pagenum * (pagenum_t)sizeof(page_t), SEEK_SET);
    fwrite(src, sizeof(page_t), 1, fd[table_id]);
    fflush(fd[table_id]);
}


// using Header Page
pagenum_t get_root_pagenum(page_t* page)
{
    return *(pagenum_t*)(page->d + 8);
}
pagenum_t get_number_of_page(page_t* page)
{
    return *(pagenum_t*)(page->d + 16);
}

void set_root_pagenum(pagenum_t root_pagenum, page_t* page)
{
    *(pagenum_t*)(page->d + 8) = root_pagenum;
}
void set_number_of_page(pagenum_t number_of_page, page_t* page)
{
    *(pagenum_t*)(page->d + 16) = number_of_page;
}

// using Page Header

int get_is_leaf(page_t* page)
{
    return *(int*)(page->d + 8);
}
int get_number_of_key(page_t* page)
{
    return *(int*)(page->d + 12);
}

void set_is_leaf(int is_leaf, page_t* page)
{
    *(int*)(page->d + 8) = is_leaf;
}
void set_number_of_key(int number_of_key, page_t* page)
{
    *(int*)(page->d + 12) = number_of_key;
}

// using Leaf Page
char* get_key_value(int index, page_t* page)
{
    return (page->d + (index + 1) * 128);
}

void set_key_value(int index, int64_t key, char* value, page_t* page)
{
    memcpy(page->d + (index + 1) * 128, (void*)&key, 8);
    memcpy(page->d + (index + 1) * 128 + 8, (void*)value, 120);
}

// using Internal Page
char* get_key_pagenum(int index, page_t* page)
{
    return (page->d + 128 + index * 16);
}

void set_key_pagenum(int index, int64_t key, pagenum_t pagenum, page_t* page)
{
    memcpy(page->d + 128 + index * 16, (void*)&key, 8);
    memcpy(page->d + 128 + index * 16 + 8, (void*)&pagenum, 8);
}

int64_t get_key_from_record(record* record1)
{
    return *(int64_t *)(record1->d);
}

char* get_value_from_record(record* record1)
{
    return (record1->d + 8);
}

void set_key_to_record(int64_t key, record* record1)
{
    memcpy(record1->d, (void*)&key, sizeof(key));
}

void set_value_to_record(char* value, record* record1)
{
    memcpy(record1->d + 8, value, 120);
}