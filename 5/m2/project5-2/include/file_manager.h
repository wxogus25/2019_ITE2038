#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <list>
#include <thread>
#include <mutex>
#include <vector>
#include <queue>
#include <unordered_map>
#include <condition_variable>

#define INTERNAL_PAGE_ORDER 249 // 249
#define LEAF_PAGE_ORDER 32 // 32
#define LINEAR_SEARCH
//#define MAP
//#define HASH_TABLE

#ifdef MAP
#include <iostream>
#include <algorithm>
#include <utility>
#include <map>
#endif

typedef uint64_t pagenum_t;

extern FILE* fd[11];
extern int used_fd[11], page_size[11];
extern char* table_name[11];

typedef struct
{
    char d[4096];
} page_t;

typedef struct{
    char d[128];
} record;
// Allocate an on-disk page from the free page list
//pagenum_t file_alloc_page(int table_id);

// Free an on-disk page to the free page list
//void file_free_page(int table_id, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int table_id, pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src);

// equal function

pagenum_t get_0byte_to_pagenum_t(page_t* page);
pagenum_t get_120byte_to_pagenum_t(page_t* page);

void set_0byte_to_pagenum_t(pagenum_t pagenum, page_t* page);
void set_120byte_to_pagenum_t(pagenum_t pagenum, page_t* page);

pagenum_t get_root_pagenum(page_t* page);
pagenum_t get_number_of_page(page_t* page);

void set_root_pagenum(pagenum_t root_pagenum, page_t* page);
void set_number_of_page(pagenum_t number_of_page, page_t* page);

// using Free Page

int get_is_leaf(page_t* page);
int get_number_of_key(page_t* page);

void set_is_leaf(int is_leaf, page_t* page);
void set_number_of_key(int number_of_key, page_t* page);

char* get_key_value(int index, page_t* page);

void set_key_value(int index, int64_t key, char* value, page_t* page);

char* get_key_pagenum(int index, page_t* page);

void set_key_pagenum(int index, int64_t key, pagenum_t pagenum, page_t* page);

int64_t get_key_from_record(record* record1);
char* get_value_from_record(record* record1);

void set_key_to_record(int64_t key, record* record1);
void set_value_to_record(char* value, record* record1);

extern pagenum_t (*get_free_pagenum)(page_t*);
extern void (*set_free_pagenum)(pagenum_t, page_t*);
extern pagenum_t (*get_parent_pagenum)(page_t*);
extern void (*set_parent_pagenum)(pagenum_t, page_t*);
extern pagenum_t (*get_right_sibling_pagenum)(page_t*);
extern void (*set_right_sibling_pagenum)(pagenum_t, page_t*);
extern pagenum_t (*get_one_more_pagenum)(page_t*);
extern void (*set_one_more_pagenum)(pagenum_t, page_t*);