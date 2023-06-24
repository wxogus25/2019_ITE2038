#pragma once

#include "bpt.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif

#define INTERNAL_PAGE_ORDER 249 // 249
#define LEAF_PAGE_ORDER 32 // 32

typedef uint64_t pagenum_t;

FILE* fd[11];
int used_fd[11], page_size[11];
char* table_name[11];

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

// using Header Page
pagenum_t (*get_free_pagenum)(page_t*);
pagenum_t get_root_pagenum(page_t* page);
pagenum_t get_number_of_page(page_t* page);

void (*set_free_pagenum)(pagenum_t, page_t*);
void set_root_pagenum(pagenum_t root_pagenum, page_t* page);
void set_number_of_page(pagenum_t number_of_page, page_t* page);

// using Free Page

// using Page Header
pagenum_t (*get_parent_pagenum)(page_t*);
int get_is_leaf(page_t* page);
int get_number_of_key(page_t* page);

void (*set_parent_pagenum)(pagenum_t, page_t*);
void set_is_leaf(int is_leaf, page_t* page);
void set_number_of_key(int number_of_key, page_t* page);

// using Leaf Page
pagenum_t (*get_right_sibling_pagenum)(page_t*);
char* get_key_value(int index, page_t* page);

void (*set_right_sibling_pagenum)(pagenum_t, page_t*);
void set_key_value(int index, int64_t key, char* value, page_t* page);

// using Internal Page
pagenum_t (*get_one_more_pagenum)(page_t*);
char* get_key_pagenum(int index, page_t* page);

void (*set_one_more_pagenum)(pagenum_t, page_t*);
void set_key_pagenum(int index, int64_t key, pagenum_t pagenum, page_t* page);

int64_t get_key_from_record(record* record1);
char* get_value_from_record(record* record1);

void set_key_to_record(int64_t key, record* record1);
void set_value_to_record(char* value, record* record1);
