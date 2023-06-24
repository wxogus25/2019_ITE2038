/*
 *  bpt.c
 */
#define Version "1.14"
/*
 *
 *  bpt:  B+ Tree Implementation
 *  Copyright (C) 2010-2016  Amittai Aviram  http://www.amittai.com
 *  All rights reserved.
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice,
 *  this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright notice,
 *  this list of conditions and the following disclaimer in the documentation
 *  and/or other materials provided with the distribution.
 
 *  3. Neither the name of the copyright holder nor the names of its
 *  contributors may be used to endorse or promote products derived from this
 *  software without specific prior written permission.
 
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 
 *  Author:  Amittai Aviram
 *    http://www.amittai.com
 *    amittai.aviram@gmail.edu or afa13@columbia.edu
 *  Original Date:  26 June 2010
 *  Last modified: 17 June 2016
 *
 *  This implementation demonstrates the B+ tree data structure
 *  for educational purposes, includin insertion, deletion, search, and display
 *  of the search path, the leaves, or the whole tree.
 *
 *  Must be compiled with a C99-compliant C compiler such as the latest GCC.
 *
 *  Usage:  bpt [order]
 *  where order is an optional argument
 *  (integer MIN_ORDER <= order <= MAX_ORDER)
 *  defined as the maximal number of pointers in any node.
 *
 */

#include "bpt.h"
#include "buffer_manager.h"

// GLOBALS.

/* The order determines the maximum and minimum
 * number of entries (keys and pointers) in any
 * node.  Every node has at most order - 1 keys and
 * at least (roughly speaking) half that number.
 * Every leaf has as many pointers to data as keys,
 * and every internal node has one more pointer
 * to a subtree than the number of keys.
 * This global variable is initialized to the
 * default value.
 */
int order = DEFAULT_ORDER;

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
queue_node * queue = NULL;

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
bool verbose_output = false;


// FUNCTION DEFINITIONS.

// Milestone 2

int open_table(char* pathname)
{
    int ck = 0;
    for (int i = 1; i <= 10; ++i) {
        if (used_fd[i] && !strcmp(table_name[i], pathname))
        {
            puts("table alraedy open");
            return i;
        }
        if (used_fd[i] == 0) {
            ck = i;
            break;
        }
    }
    fd[ck] = fopen(pathname, "r+b");
    if (fd[ck] == NULL)
    {
        fd[ck] = fopen(pathname, "w");
        fclose(fd[ck]);
        fd[ck] = fopen(pathname, "r+b");
    }else
    {
        page_t *page = (page_t *) malloc(sizeof(page_t));
        buffer_read_page(ck, 0, page);
        page_size[ck] = get_number_of_page(page);
        free(page);
        used_fd[ck] = 1;
        table_name[ck] = (char *)malloc(120);
        strcpy(table_name[ck], pathname);
        return ck;
    }
    if(fd[ck] == NULL)
    {
        puts("fail file open");
        return -1;
    }
    used_fd[ck] = 1;
    table_name[ck] = (char *)malloc(120);
    strcpy(table_name[ck], pathname);
    
    puts("table create");
    puts("open table");
    
    buffer_alloc_page(ck);
    page_t *page = (page_t *) malloc(sizeof(page_t));
    buffer_read_page(ck, 0, page);
    
    set_free_pagenum(0, page);
    set_root_pagenum(0, page);
    set_number_of_page(0, page);
    buffer_write_page(ck, 0, page);
    
    free(page);
    return 0;
}

int db_insert( int table_id, int64_t key, char* value)
{
    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, 0, page);
    pagenum_t root = get_root_pagenum(page), ck;
    free(page);
    ck = insert(table_id, root, key, value);
    
    if (ck == -1)
        return 1;
    
    if (ck == root)
    {
        if(ck == 0)
            return 1;
        return 0;
    }
    
    page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, 0, page);
    set_root_pagenum(ck, page);
    buffer_write_page(table_id, 0, page);
    free(page);
    
    return 0;
}

int db_find( int table_id, int64_t key, char* ret_val)
{
    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, 0, page);
    int root = get_root_pagenum(page);
    free(page);
    record* record1 = (record*)malloc(sizeof(record));
    int ck = find(table_id, root, key, false, record1);
    if (!ck)
        memcpy(ret_val, get_value_from_record(record1), 120);
    free(record1);
    return ck;
}

int db_delete(int table_id, int64_t key)
{
    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, 0, page);
    pagenum_t root = get_root_pagenum(page), ck;
    free(page);
    ck = delete(table_id, root, key);
    
    if (ck == -1)
        return 1;
    
    if(ck == root)
    {
        if (root == 0)
            return 1;
        return 0;
    }

    page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, 0, page);
    set_root_pagenum(ck, page);
    buffer_write_page(table_id, 0, page);
    free(page);

    return 0;
}

/* Helper function for printing the
 * tree out.  See print_tree.
 */
void enqueue( queue_node * new_node ) {
    queue_node * c;
    if (queue == NULL) {
        queue = new_node;
        queue->next = NULL;
    }
    else {
        c = queue;
        while(c->next != NULL) {
            c = c->next;
        }
        c->next = new_node;
        new_node->next = NULL;
    }
}


/* Helper function for printing the
 * tree out.  See print_tree.
 */
queue_node * dequeue( void ) {
    queue_node * n = queue;
    queue = queue->next;
    n->next = NULL;
    return n;
}

/* Prints the bottom row of keys
 * of the tree (with their respective
 * pointers, if the verbose_output flag is set.
 */

void change(int64_t num, char* buf)
{
    int64_t ck = 1;
    while (ck <= num)
        ck*=10;
    ck/=10;
    int i = 0;
    while (ck)
    {
        buf[i++] = '0' + (num/ck);
        num%=ck;
        ck/=10;
    }
    buf[i] = '\0';
}

int print_leaves( int table_id ) {
    int i;
    page_t * page = malloc(sizeof(page_t));
    record* record1 = malloc(sizeof(record));
    buffer_read_page(table_id, 0, page);
    pagenum_t c = get_root_pagenum(page);
    if (c == 0) {
        free(page);
        free(record1);
        printf("Empty tree.\n");
        return 0;
    }
    char ck[120], value[120];
    buffer_read_page(table_id, c, page);
    int64_t pre = -1, key;
    while (!get_is_leaf(page)) {
        c = get_one_more_pagenum(page);
        buffer_read_page(table_id, c, page);
    }
    int sum = 0;
    while (true) {
        int num_keys = get_number_of_key(page);
        for (i = 0; i < num_keys; i++) {
            memcpy(record1, get_key_value(i, page), sizeof(record));
            key = get_key_from_record(record1);
            change(key, ck);
            memcpy(value, get_value_from_record(record1) + 1, 116);
            if (strcmp(value, ck) || key < pre) {
                printf("value : %s, key : %lld, pre : %lld\n", value, key, pre);
                return -1;
            }
            pre = key;
        }
        sum += num_keys;
        if (get_right_sibling_pagenum(page) != 0) {
            c = get_right_sibling_pagenum(page);
            buffer_read_page(table_id, c, page);
        }
        else
            break;
    }
    free(record1);
    free(page);
    return sum;
}

/* Utility function to give the height
 * of the tree, which length in number of edges
 * of the path from the root to any leaf.
 */
//int height( node * root ) {
//    int h = 0;
//    node * c = root;
//    while (!c->is_leaf) {
//        c = c->pointers[0];
//        h++;
//    }
//    return h;
//}


/* Utility function to give the length in edges
 * of the path from any node to the root.
 */
//int path_to_root( node * root, node * child ) {
//    int length = 0;
//    node * c = child;
//    while (c != root) {
//        c = c->parent;
//        length++;
//    }
//    return length;
//}


/* Prints the B+ tree in the command
 * line in level (rank) order, with the
 * keys in each node and the '|' symbol
 * to separate nodes.
 * With the verbose_output flag set.
 * the values of the pointers corresponding
 * to the keys also appear next to their respective
 * keys, in hexadecimal notation.
 */
void print_tree( int table_id ) {
    queue_node * n = NULL;
    int i = 0;
    int rank = 0;
    int new_rank = 0;
    page_t* temp = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id,0, temp);
    pagenum_t rootnum = get_root_pagenum(temp);
    free(temp);
    printf("table_id : %d, table_name : %s root : %lld\n", table_id, table_name[table_id], rootnum);
    if (rootnum == 0) {
        printf("Empty tree.\n");
        return;
    }
    queue_node* root = (queue_node*)malloc(sizeof(queue_node));
    root->is_first_pointer = root->rank = 0;
    root->now_page = rootnum;
    root->next = NULL;
    queue = NULL;
    record* record1 = (record*)malloc(sizeof(record));
    enqueue(root);
    while( queue != NULL ) {
        n = dequeue();
        temp = (page_t*)malloc((sizeof(page_t)));
        buffer_read_page(table_id, n->now_page, temp);
        if (n->rank != 0 && n->is_first_pointer) {
            new_rank = n->rank;
            if (new_rank != rank) {
                rank = new_rank;
                printf("\n");
            }
        }
//        if (verbose_output)
//            printf("(%lx)", (unsigned long)n);
        int num_keys = get_number_of_key(temp);
        int is_leaf = get_is_leaf(temp);
//        printf("%lld -> ", n->now_page);
        for (i = 0; i < num_keys; i++) {
//                if (verbose_output)
//                    printf("%lx ", (unsigned long) n->pointers[i]);
            if (is_leaf)
                memcpy(record1, (void*)get_key_value(i, temp), 128);
            else
                memcpy(record1, (void*)get_key_pagenum(i, temp), 16);
//            printf("[(%lld, %lld), ", n->now_page, get_parent_pagenum(temp));
            if(is_leaf)
            {
                char d[120];
                memcpy(d, get_value_from_record(record1), 120);
                printf("(%lld, %s) ", get_key_from_record(record1), d);
            } else
                printf("%lld ", get_key_from_record(record1));
//            printf("]");
        }
        if (!is_leaf)
            for (i = -1; i < num_keys; i++)
            {
                queue_node* x = (queue_node*)malloc(sizeof(queue_node));
                x->rank = n->rank + 1;
                x->is_first_pointer = (i == -1);
                memcpy(record1, (void*)get_key_pagenum(i,temp), 16);
                x->now_page = *(pagenum_t*)(get_value_from_record(record1));
                enqueue(x);
            }
//        if (verbose_output) {
//            if (n->is_leaf)
//                printf("%lx ", (unsigned long)n->pointers[order - 1]);
//            else
//                printf("%lx ", (unsigned long)n->pointers[n->num_keys]);
//        }
        printf("| ");
        free(n);
        free(temp);
    }
    free(record1);
    printf("\n");
}


/* Finds the record under a given key and prints an
 * appropriate message to stdout.
 */
//void find_and_print(node * root, int key, bool verbose) {
//    record * r = find(root, key, verbose);
//    if (r == NULL)
//        printf("Record not found under key %d.\n", key);
//    else
//        printf("Record at %lx -- key %d, value %d.\n",
//               (unsigned long)r, key, r->value);
//}


/* Finds and prints the keys, pointers, and values within a range
 * of keys between key_start and key_end, including both bounds.
 */
//void find_and_print_range( node * root, int key_start, int key_end,
//                           bool verbose ) {
//    int i;
//    int array_size = key_end - key_start + 1;
//    int returned_keys[array_size];
//    void * returned_pointers[array_size];
//    int num_found = find_range( root, key_start, key_end, verbose,
//                                returned_keys, returned_pointers );
//    if (!num_found)
//        printf("None found.\n");
//    else {
//        for (i = 0; i < num_found; i++)
//            printf("Key: %d   Location: %lx  Value: %d\n",
//                   returned_keys[i],
//                   (unsigned long)returned_pointers[i],
//                   ((record *)
//                           returned_pointers[i])->value);
//    }
//}


/* Finds keys and their pointers, if present, in the range specified
 * by key_start and key_end, inclusive.  Places these in the arrays
 * returned_keys and returned_pointers, and returns the number of
 * entries found.
 */
//int find_range( node * root, int key_start, int key_end, bool verbose,
//                int returned_keys[], void * returned_pointers[]) {
//    int i, num_found;
//    num_found = 0;
//    node * n = find_leaf( root, key_start, verbose );
//    if (n == NULL) return 0;
//    for (i = 0; i < n->num_keys && n->keys[i] < key_start; i++) ;
//    if (i == n->num_keys) return 0;
//    while (n != NULL) {
//        for ( ; i < n->num_keys && n->keys[i] <= key_end; i++) {
//            returned_keys[num_found] = n->keys[i];
//            returned_pointers[num_found] = n->pointers[i];
//            num_found++;
//        }
//        n = n->pointers[order - 1];
//        i = 0;
//    }
//    return num_found;
//}


/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 */
pagenum_t find_leaf( int table_id, pagenum_t root, int64_t key, bool verbose ) {
    if (root == 0) {
        if (verbose)
            printf("Empty tree.\n");
        return 0;
    }
    int i = 0, now = root;
    page_t* page = (page_t*)malloc(sizeof(page_t));
    record* record1 = (record*)malloc(sizeof(record));
    buffer_read_page(table_id, root, page);
    int is_leaf = get_is_leaf(page);
    int num_keys = get_number_of_key(page);
    while (!is_leaf) {
//        if (verbose) {
//            printf("[");
//            for (i = 0; i < c->num_keys - 1; i++)
//                printf("%d ", c->keys[i]);
//            printf("%d] ", c->keys[i]);
//        }
        i = 0;
        memcpy(record1,(void*)get_key_pagenum(-1,page),16);
        pagenum_t first_pagenum = *(pagenum_t *)(get_value_from_record(record1));
        while (i < num_keys) {
            memcpy(record1,(void*)get_key_pagenum(i,page), 16);
            if (key >= get_key_from_record(record1))
                i++;
            else
                break;
        }
//        if (verbose)
//            printf("%d ->\n", i);
        if (i == 0)
            now = first_pagenum;
        else
            now = *(pagenum_t*)(get_value_from_record((record*)get_key_pagenum(i-1,page)));
        buffer_read_page(table_id, now, page);
        is_leaf = get_is_leaf(page);
        num_keys = get_number_of_key(page);
    }
//    if (verbose) {
//        printf("Leaf [");
//        for (i = 0; i < c->num_keys - 1; i++)
//            printf("%d ", c->keys[i]);
//        printf("%d] ->\n", c->keys[i]);
//    }
    free(record1);
    free(page);
    return now;
}


/* Finds and returns the record to which
 * a key refers.
 */
int find( int table_id, pagenum_t root, int64_t key, bool verbose, record* record1 ) {
    int i = 0;
    pagenum_t c = find_leaf( table_id, root, key, verbose );
    if (verbose)
        printf("leaf page : %lld\n", c);
    if (c == 0) return 1;
    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, c, page);
    int num_keys = get_number_of_key(page);
    for (i = 0; i < num_keys; i++) {
        memcpy(record1, (void*)get_key_value(i,page),sizeof(record));
        if(verbose)
        {
            printf("%lld ", get_key_from_record(record1));
        }
        if (get_key_from_record(record1) == key) break;
    }
//    puts("");
    free(page);
    return i == num_keys;
}

/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}


// INSERTION

/* Creates a new record to hold the value
 * to which a key refers.
 */
record * make_record(char* value) {
    record * new_record = (record *)malloc(sizeof(record));
    if (new_record == NULL) {
        perror("Record creation.");
        exit(EXIT_FAILURE);
    }
    else {
        set_value_to_record(value, new_record);
    }
    return new_record;
}


/* Creates a new general node, which can be adapted
 * to serve as either a leaf or an internal node.
 */
pagenum_t make_node( int table_id ) {
    pagenum_t new_node;
    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, 0, page);
    free(page);
    new_node = buffer_alloc_page(table_id);
    if (new_node < 0) {
        perror("Node creation.");
        exit(EXIT_FAILURE);
    }
    page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, new_node, page);
    set_parent_pagenum(0, page);
    set_is_leaf(0, page);
    set_number_of_key(0, page);
    set_one_more_pagenum(0, page);
//    new_node->is_leaf = false;
//    new_node->num_keys = 0;
//    new_node->parent = NULL;
//    new_node->next = NULL;
    buffer_write_page(table_id, new_node, page);
    free(page);
    return new_node;
}

/* Creates a new leaf by creating a node
 * and then adapting it appropriately.
 */
pagenum_t make_leaf( int table_id ) {
    pagenum_t leaf = make_node(table_id);
    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, leaf, page);
    set_is_leaf(1,page);
    buffer_write_page(table_id, leaf, page);
    free(page);
    return leaf;
}


/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to
 * the node to the left of the key to be inserted.
 */
int get_left_index( int table_id, pagenum_t parent, pagenum_t left) {
    
    int left_index = 0, num_keys;
    page_t* page = (page_t*)malloc(sizeof(page_t));
    record* record1 = (record*)malloc(sizeof(record));
    buffer_read_page(table_id, parent, page);
    num_keys = get_number_of_key(page);
    
    memcpy(record1, get_key_pagenum(-1, page), 16);
    if (*(pagenum_t *)(get_value_from_record(record1)) == left)
    {
        free(page);
        free(record1);
        return 0;
    }
    
    while (left_index < num_keys) {
        memcpy(record1, get_key_pagenum(left_index, page), 16);
        if (*(pagenum_t*)(get_value_from_record(record1)) != left)
            left_index++;
        else
            break;
    }
    free(page);
    free(record1);
    return left_index + 1;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */
pagenum_t insert_into_leaf( int table_id, pagenum_t leaf, record * pointer ) {
    
    int i, insertion_point;
    page_t* page = (page_t*)malloc(sizeof(page_t));
    record* record1 = (record*)malloc(sizeof(record));
    buffer_read_page(table_id, leaf, page);
    int num_keys = get_number_of_key(page);
    insertion_point = 0;
    while (insertion_point < num_keys) {
        memcpy(record1, get_key_value(insertion_point, page), sizeof(record));
        if (get_key_from_record(record1) < get_key_from_record(pointer))
            insertion_point++;
        else
            break;
    }
    for (i = num_keys; i > insertion_point; i--) {
        memcpy(record1, get_key_value(i - 1, page), sizeof(record));
        set_key_value(i, get_key_from_record(record1), get_value_from_record(record1), page);
    }
    
    set_key_value(insertion_point, get_key_from_record(pointer), get_value_from_record(pointer), page);
    set_number_of_key(num_keys+1,page);
    buffer_write_page(table_id, leaf, page);
    
    free(record1);
    free(page);
    return leaf;
}


/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
pagenum_t insert_into_leaf_after_splitting(int table_id, pagenum_t root, pagenum_t leaf, record * pointer) {
    
    pagenum_t new_leaf, right_sibling, parent;
    int64_t new_key;
    int64_t * temp_keys;
    void ** temp_pointers;
    int insertion_index, split, i, j, num_keys;
    
    new_leaf = make_leaf(table_id);
    
    
    temp_pointers = malloc( LEAF_PAGE_ORDER * sizeof(char *) );
    for (int k = 0; k < LEAF_PAGE_ORDER; ++k) {
        temp_pointers[k] = malloc(120);
    }
    temp_keys = (int64_t*)malloc(LEAF_PAGE_ORDER * sizeof(int64_t));
//    if (temp_pointers == NULL) {
//        perror("Temporary pointers array.");
//        exit(EXIT_FAILURE);
//    }
    
    page_t* page = (page_t*)malloc(sizeof(page_t));
    record* record1 = (record*)malloc(sizeof(record));
    
    buffer_read_page(table_id, leaf, page);
    num_keys = get_number_of_key(page);
    right_sibling = get_right_sibling_pagenum(page);
    parent = get_parent_pagenum(page);
    insertion_index = 0;
    
    while (insertion_index < LEAF_PAGE_ORDER - 1) {
        memcpy(record1, get_key_value(insertion_index, page), sizeof(record));
        if (get_key_from_record(record1) < get_key_from_record(pointer))
            insertion_index++;
        else
            break;
    }
    
    for (i = 0, j = 0; i < num_keys; i++, j++) {
        if (j == insertion_index) j++;
        memcpy(record1,get_key_value(i,page), sizeof(record));
        temp_keys[j] = get_key_from_record(record1);
        memcpy(temp_pointers[j], get_value_from_record(record1), 120);
    }
    
    temp_keys[insertion_index] = get_key_from_record(pointer);
    memcpy(temp_pointers[insertion_index],get_value_from_record(pointer), 120);
    
//    printf("split : ");
//    for (int i = 0; i < num_keys + 1; ++i) {
//        printf("%lld ", temp_keys[i]);
//    }
//    puts("");
    num_keys = 0;
    
    split = cut(LEAF_PAGE_ORDER - 1);
    
    for (i = 0; i < split; i++) {
        set_key_value(i, temp_keys[i], temp_pointers[i], page);
        num_keys++;
    }
    
    set_number_of_key(num_keys, page);
    set_right_sibling_pagenum(new_leaf, page);
    buffer_write_page(table_id, leaf, page);

    buffer_read_page(table_id, new_leaf, page);
    num_keys = 0;
    for (i = split, j = 0; i < LEAF_PAGE_ORDER; i++, j++) {
        set_key_value(j, temp_keys[i], temp_pointers[i], page);
        num_keys++;
    }
    set_number_of_key(num_keys, page);
    set_right_sibling_pagenum(right_sibling, page);
    set_parent_pagenum(parent, page);
    buffer_write_page(table_id, new_leaf, page);
    
    for (int k = 0; k < LEAF_PAGE_ORDER; ++k) {
        free(temp_pointers[k]);
    }
    free(temp_pointers);
    free(temp_keys);
    
    memcpy(record1, get_key_value(0, page), sizeof(record));
    new_key = get_key_from_record(record1);
//    printf("new_key : %lld\n", new_key);
    free(record1);
    free(page);
    free(pointer);
//    new_leaf->pointers[order - 1] = leaf->p   ointers[order - 1];
//    leaf->pointers[order - 1] = new_leaf;

//    for (i = leaf->num_keys; i < order - 1; i++)
//        leaf->pointers[i] = NULL;
//    for (i = new_leaf->num_keys; i < order - 1; i++)
//        new_leaf->pointers[i] = NULL;

//    new_leaf->parent = leaf->parent;
//    new_key = new_leaf->keys[0];
    return insert_into_parent(table_id, root, leaf, new_key, new_leaf);
}


/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
pagenum_t insert_into_node(int table_id, pagenum_t root, pagenum_t n,
                           int left_index, int64_t key, pagenum_t right) {
    int i, num_keys;
    
    page_t* page = (page_t*)malloc(sizeof(page_t));
    record* record1 = (record*)malloc(sizeof(record));
    buffer_read_page(table_id, n, page);
    num_keys = get_number_of_key(page);
    for (i = num_keys - 1; i >= left_index; i--) {
        memcpy(record1, get_key_pagenum(i, page), 16);
        set_key_pagenum(i + 1, get_key_from_record(record1), *(pagenum_t *)(get_value_from_record(record1)), page);
    }
    set_key_pagenum(left_index, key, right, page);
    set_number_of_key(num_keys+1,page);
    buffer_write_page(table_id, n, page);
    
    free(record1);
    free(page);
    return root;
}


/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
pagenum_t insert_into_node_after_splitting(int table_id, pagenum_t root, pagenum_t old_node, int left_index,
                                           int64_t key, pagenum_t right) {
    
    int i, j, split;
    pagenum_t new_node, child, parent, * temp_pointers;
    int64_t * temp_keys, k_prime;
    
    /* First create a temporary set of keys and pointers
     * to hold everything in order, including
     * the new key and pointer, inserted in their
     * correct places.
     * Then create a new node and copy half of the
     * keys and pointers to the old node and
     * the other half to the new.
     */
    
    temp_pointers = (pagenum_t *)malloc( (INTERNAL_PAGE_ORDER + 1) * sizeof(pagenum_t) );
    if (temp_pointers == NULL) {
        perror("Temporary pointers array for splitting nodes.");
        exit(EXIT_FAILURE);
    }
    temp_keys = (int64_t *)malloc( INTERNAL_PAGE_ORDER * sizeof(int64_t) );
    if (temp_keys == NULL) {
        perror("Temporary keys array for splitting nodes.");
        exit(EXIT_FAILURE);
    }
    
    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, old_node, page);
    record* record1 = (record*)malloc(sizeof(record));
    
    parent = get_parent_pagenum(page);
    int num_keys = get_number_of_key(page);
    
    temp_pointers[0] = get_one_more_pagenum(page);
    for (i = 0, j = 1; i < num_keys; i++, j++) {
        if (j == left_index + 1) j++;
        memcpy(record1, get_key_pagenum(i,page), 16);
        temp_pointers[j] = *(pagenum_t*)(get_value_from_record(record1));
    }
    
    for (i = 0, j = 0; i < num_keys; i++, j++) {
        if (j == left_index) j++;
        memcpy(record1, get_key_pagenum(i,page), 16);
        temp_keys[j] = get_key_from_record(record1);
    }
    
    temp_pointers[left_index + 1] = right;
    temp_keys[left_index] = key;
    
//    printf("temp pointer : ");
//    for (int i = 0; i < num_keys + 2; ++i) {
//        printf("%lld ", temp_pointers[i]);
//    }
//    printf("\ntemp key : ");
//    for (int i = 0; i < num_keys + 1; ++i) {
//        printf("%lld ", temp_keys[i]);
//    }
//    puts("");
    /* Create the new node and copy
     * half the keys and pointers to the
     * old and half to the new.
     */
    
    split = cut(INTERNAL_PAGE_ORDER);
    free(page);
    new_node = make_node(table_id);
    page = malloc(sizeof(page_t));
    buffer_read_page(table_id, old_node, page);
    
    num_keys = 0;
    set_key_pagenum(-1,0,temp_pointers[0], page);
    for (i = 0; i < split - 1; i++) {
        set_key_pagenum(i,temp_keys[i], temp_pointers[i + 1], page);
        num_keys++;
    }
    set_number_of_key(num_keys, page);
    buffer_write_page(table_id, old_node, page);
    
    k_prime = temp_keys[split - 1];
    
    buffer_read_page(table_id, new_node, page);
    set_key_pagenum(-1,0,temp_pointers[i + 1],page);
    temp_pointers[0] = temp_pointers[i + 1];
    num_keys = 0;
    for (++i, j = 0; i < INTERNAL_PAGE_ORDER; i++, j++) {
        set_key_pagenum(j, temp_keys[i], temp_pointers[i + 1], page);
        temp_pointers[j + 1] = temp_pointers[i + 1];
        num_keys++;
    }
    set_number_of_key(num_keys, page);
    set_parent_pagenum(parent, page);
    buffer_write_page(table_id, new_node, page);
    
    // malloc change
    
    for (i = 0; i <= num_keys; i++) {
        child = temp_pointers[i];
        buffer_read_page(table_id, child, page);
        set_parent_pagenum(new_node, page);
        buffer_write_page(table_id, child, page);
    }
    free(temp_pointers);
    free(temp_keys);
    free(record1);
    free(page);
    
    /* Insert a new key into the parent of the two
     * nodes resulting from the split, with
     * the old node to the left and the new to the right.
     */
    
    return insert_into_parent(table_id, root, old_node, k_prime, new_node);
}



/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
pagenum_t insert_into_parent(int table_id, pagenum_t root, pagenum_t left, int64_t key, pagenum_t right) {
    
    int left_index, num_keys;
    pagenum_t parent;
    
    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, left, page);
    parent = get_parent_pagenum(page);
    num_keys = get_number_of_key(page);
    free(page);
    /* Case: new root. */
    
    if (parent == 0) {
        return insert_into_new_root(table_id, left, key, right);
    }
    
    /* Case: leaf or node. (Remainder of
     * function body.)
     */
    
    /* Find the parent's pointer to the left
     * node.
     */
    
    left_index = get_left_index(table_id, parent, left);
    
    
    /* Simple case: the new key fits into the node.
     */
    
    page = malloc(sizeof(page_t));
    buffer_read_page(table_id, parent, page);
    num_keys = get_number_of_key(page);
    free(page);
    
    if (num_keys < INTERNAL_PAGE_ORDER - 1)
        return insert_into_node(table_id, root, parent, left_index, key, right);
    
    /* Harder case:  split a node in order
     * to preserve the B+ tree properties.
     */
    
    return insert_into_node_after_splitting(table_id, root, parent, left_index, key, right);
}


/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
pagenum_t insert_into_new_root(int table_id, pagenum_t left, int64_t key, pagenum_t right) {
    
    pagenum_t root = make_node(table_id);
    page_t* page = (page_t*)malloc(sizeof(page_t));
    
    buffer_read_page(table_id, root, page);
    set_key_pagenum(-1, 0, left, page);
    set_key_pagenum(0, key, right, page);
    set_number_of_key(get_number_of_key(page) + 1, page);
    buffer_write_page(table_id, root, page);
    
    buffer_read_page(table_id, left, page);
    set_parent_pagenum(root, page);
    
    buffer_write_page(table_id, left, page);
    
    buffer_read_page(table_id, right, page);
    set_parent_pagenum(root, page);
    buffer_write_page(table_id, right, page);
    
    free(page);
    
    return root;
}



/* First insertion:
 * start a new tree.
 */
pagenum_t start_new_tree(int table_id, record * pointer) {
    
    pagenum_t root = make_leaf(table_id);
    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, root, page);
    set_key_value(0, get_key_from_record(pointer), get_value_from_record(pointer), page);
    set_number_of_key(1, page);
    buffer_write_page(table_id, root, page);
    free(page);
    return root;
}



/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
pagenum_t insert( int table_id, pagenum_t root, int64_t key, char* value ) {
    
    record * pointer = (record*)malloc(sizeof(record));
    pagenum_t leaf;
    /* The current implementation ignores
     * duplicates.
     */
    int ck = find(table_id, root, key, false, pointer);
    free(pointer);
    
    if (!ck)
        return -1;
    /* Create a new record for the
     * value.
     */
    pointer = make_record(value);
    set_key_to_record(key, pointer);
    /* Case: the tree does not exist yet.
     * Start a new tree.
     */
    if (root == 0)
        return start_new_tree(table_id, pointer);
    /* Case: the tree already exists.
     * (Rest of function body.)
     */
    
    leaf = find_leaf(table_id, root, key, false);
    /* Case: leaf has room for key and pointer.
     */
    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, leaf, page);
    int num_keys = get_number_of_key(page);
    free(page);
    if (num_keys < LEAF_PAGE_ORDER - 1) {
        insert_into_leaf(table_id, leaf, pointer);
        free(pointer);
        return root;
    }
    
    
    /* Case:  leaf must be split.
     */
    
    return insert_into_leaf_after_splitting(table_id, root, leaf, pointer);
}




// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
int get_my_index( int table_id, pagenum_t n ) {

    int i, num_keys;

    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, n, page);
    pagenum_t parent = get_parent_pagenum(page);
    buffer_read_page(table_id, parent, page);
    num_keys = get_number_of_key(page);

//    printf("parent page : %lld, now page : %lld\n", parent, n);
    
    for (i = -1; i < num_keys; i++) {
//        printf("%lld ", *(pagenum_t *) get_value_from_record((record *) get_key_pagenum(i, page)));
        if (*(pagenum_t *) get_value_from_record((record *) get_key_pagenum(i, page)) == n) {
            free(page);
//            puts("");
            return i;
        }
    }
//    puts("");
    free(page);
    // Error state.
    printf("Search for nonexistent pointer to node in parent.\n");
    printf("Node:  %#lx\n", (unsigned long)n);
    exit(EXIT_FAILURE);
}


pagenum_t remove_entry_from_node(int table_id, pagenum_t n, record* pointer) {

    int i, num_keys, is_leaf;

    i = 0;
    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, n, page);
    num_keys = get_number_of_key(page);
    is_leaf = get_is_leaf(page);

    if (is_leaf) {
        while (i < num_keys && get_key_from_record((record *) get_key_value(i, page)) != get_key_from_record(pointer))
            i++;
        for (++i; i < num_keys; i++)
            set_key_value(i - 1, get_key_from_record((record *) get_key_value(i, page)), get_value_from_record((record *) get_key_value(i, page)), page);
    }else {
        while (i < num_keys && get_key_from_record((record *) get_key_pagenum(i, page)) != get_key_from_record(pointer))
            i++;
        for (++i; i < num_keys; i++)
            set_key_pagenum(i - 1, get_key_from_record((record *) get_key_pagenum(i, page)),
                            *(pagenum_t *) get_value_from_record((record *) get_key_pagenum(i - 1, page)), page);
        i = -1;
        while (*(pagenum_t *) get_value_from_record((record *) get_key_pagenum(i, page)) !=
               *(pagenum_t *) get_value_from_record(pointer))
            i++;
        for (++i; i < num_keys; i++)
            set_key_pagenum(i - 1, get_key_from_record((record *) get_key_pagenum(i - 1, page)),
                            *(pagenum_t *) get_value_from_record((record *) get_key_pagenum(i, page)), page);
    }

    set_number_of_key( num_keys - 1, page);
    buffer_write_page(table_id, n, page);
    free(page);

    return n;
}


pagenum_t adjust_root(int table_id, pagenum_t root) {

    pagenum_t new_root;

    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, root, page);

    int num_keys = get_number_of_key(page), is_leaf = get_is_leaf(page);

    if (num_keys > 0) {
        free(page);
        return root;
    }

    if (!is_leaf) {
        new_root = get_one_more_pagenum(page);
        buffer_read_page(table_id, new_root, page);
        set_parent_pagenum(0, page);
        buffer_write_page(table_id, new_root, page);
    }
    else
        new_root = 0;

    free(page);
    buffer_free_page(table_id, root);

    return new_root;
}


/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
pagenum_t coalesce_nodes(int table_id, pagenum_t root, pagenum_t n, pagenum_t neighbor, int n_index, int neighbor_index) {

    pagenum_t parent, leftmost, rightsibling, parent_n_pointer;

    page_t* page = (page_t*)malloc(sizeof(page_t));
    record* record1 = (record*)malloc(sizeof(record));

    buffer_read_page(table_id, n, page);
    int is_leaf = get_is_leaf(page);
    parent = get_parent_pagenum(page);
    int64_t parent_key;
    
    if (!is_leaf) {
        leftmost = get_one_more_pagenum(page);
        if(n_index < neighbor_index)
        {
            buffer_read_page(table_id, parent, page);
            parent_key = *(int64_t *)get_key_pagenum(0, page);
            parent_n_pointer = get_one_more_pagenum(page);
            buffer_read_page(table_id, neighbor, page);
            for (int k = 1; k >= 0; k--) {
                set_key_pagenum(k, *(int64_t *)get_key_pagenum(k - 1, page), *(pagenum_t*)(get_key_pagenum(k - 1,page) + 8), page);
            }
            set_key_pagenum(0, parent_key, *(pagenum_t *)(get_key_pagenum(0, page) + 8), page);
            set_one_more_pagenum(leftmost, page);
        }else
        {
            buffer_read_page(table_id, parent, page);
            parent_key = *(int64_t*)get_key_pagenum(n_index, page);
            parent_n_pointer = *(pagenum_t*)(get_key_pagenum(n_index, page) + 8);
            buffer_read_page(table_id, neighbor, page);
            set_key_pagenum(1, parent_key, leftmost, page);
        }
        set_key_to_record(parent_key, record1);
        set_value_to_record((char*)&parent_n_pointer, record1);
        set_number_of_key(2, page);
        buffer_write_page(table_id, neighbor, page);
        buffer_read_page(table_id, leftmost, page);
        set_parent_pagenum(neighbor, page);
        buffer_write_page(table_id, leftmost, page);
    }
    else {
        rightsibling = get_right_sibling_pagenum(page);
        if(n_index < neighbor_index)
        {
            buffer_read_page(table_id, parent, page);
            parent_key = *(int64_t*)get_key_pagenum(0, page);
            parent_n_pointer = *(pagenum_t *)(get_key_pagenum(0, page) + 8);
            buffer_read_page(table_id, neighbor, page);
            memcpy(record1, get_key_value(0, page), sizeof(record));
            rightsibling = get_right_sibling_pagenum(page);
            buffer_read_page(table_id, n, page);
            set_key_value(0, get_key_from_record(record1), get_value_from_record(record1), page);
            set_right_sibling_pagenum(rightsibling, page);
            set_number_of_key(1, page);
            buffer_write_page(table_id, n, page);
            n = neighbor;
        }else
        {
            buffer_read_page(table_id, parent, page);
            parent_key = *(int64_t*)get_key_pagenum(n_index, page);
            parent_n_pointer = *(pagenum_t*)(get_key_pagenum(n_index, page) + 8);\
            buffer_read_page(table_id, neighbor, page);
            set_right_sibling_pagenum(rightsibling, page);
            set_number_of_key(1, page);
            buffer_write_page(table_id, neighbor, page);
        }
        set_key_to_record(parent_key, record1);
        set_value_to_record((char *)&parent_n_pointer, record1);
    }
    
    free(page);
    buffer_free_page(table_id, n);
    root = delete_entry(table_id, root, parent, record1);
    free(record1);
    
    return root;
}


/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
pagenum_t redistribute_nodes(int table_id, pagenum_t root, pagenum_t n, pagenum_t neighbor, int my_index, int neighbor_index) {

    int is_leaf, neighbor_num_keys;
    pagenum_t tmp, parent, n_pointer, neighbor_pointer;
    int64_t parent_key, neighbor_key;
    
    page_t* page = (page_t*)malloc(sizeof(page_t));
    record* record1 = (record*)malloc(sizeof(record));
    
    buffer_read_page(table_id, n, page);
    parent = get_parent_pagenum(page);
    is_leaf = get_is_leaf(page);
    
    if (!is_leaf) {
        if (my_index < neighbor_index) {
            buffer_read_page(table_id, parent, page);
            parent_key = *(int64_t *)get_key_pagenum(0, page);
            buffer_read_page(table_id, neighbor, page);
            neighbor_num_keys = get_number_of_key(page);
            neighbor_key = *(int64_t*)get_key_pagenum(0, page);
            neighbor_pointer = get_one_more_pagenum(page);
            buffer_read_page(table_id, n, page);
            set_key_pagenum(0, parent_key, neighbor_pointer, page);
            set_number_of_key(1, page);
            buffer_write_page(table_id, n, page);
            buffer_read_page(table_id, neighbor_pointer, page);
            set_parent_pagenum(n, page);
            buffer_write_page(table_id, neighbor_pointer, page);
            buffer_read_page(table_id, parent, page);
            set_key_pagenum(0, neighbor_key, neighbor, page);
            buffer_write_page(table_id, parent, page);
            buffer_read_page(table_id, neighbor, page);
            set_one_more_pagenum(*(pagenum_t *)(get_key_pagenum(0, page) + 8), page);
            for (int i = 0; i < neighbor_num_keys - 1; ++i) {
                set_key_pagenum(i, *(int64_t *)get_key_pagenum(i + 1, page), *(pagenum_t*)(get_key_pagenum( i + 1, page) + 8), page);
            }
            set_number_of_key(neighbor_num_keys - 1, page);
            buffer_write_page(table_id, neighbor, page);
        } else {
            buffer_read_page(table_id, parent, page);
            parent_key = *(int64_t *)get_key_pagenum(my_index, page);
            buffer_read_page(table_id, neighbor, page);
            neighbor_num_keys = get_number_of_key(page);
            neighbor_key = *(int64_t*)get_key_pagenum(neighbor_num_keys - 1, page);
            neighbor_pointer = *(pagenum_t*)(get_key_pagenum(neighbor_num_keys - 1, page) + 8);
            set_number_of_key(neighbor_num_keys - 1, page);
            buffer_write_page(table_id, neighbor, page);
            buffer_read_page(table_id, neighbor_pointer, page);
            set_parent_pagenum(n, page);
            buffer_write_page(table_id, neighbor_pointer, page);
            buffer_read_page(table_id, parent, page);
            set_key_pagenum(my_index, neighbor_key, n, page);
            buffer_write_page(table_id, parent, page);
            buffer_read_page(table_id, n, page);
            set_key_pagenum(0, parent_key, get_one_more_pagenum(page), page);
            set_one_more_pagenum(neighbor_pointer, page);
            set_number_of_key(1, page);
            buffer_write_page(table_id, n, page);
        }
    }else
    {
        if (my_index < neighbor_index) {
            buffer_read_page(table_id, neighbor, page);
            neighbor_num_keys = get_number_of_key(page);
            memcpy(record1, get_key_value(0, page), sizeof(record));
            for (int i = 0; i < neighbor_num_keys - 1; ++i) {
                set_key_value(i, *(int64_t *)get_key_value(i + 1, page), (get_key_value(i + 1,page) + 8), page);
            }
            neighbor_key = get_key_from_record((record*)get_key_value(0 ,page));
            set_number_of_key(neighbor_num_keys - 1, page);
            buffer_write_page(table_id, neighbor, page);
            buffer_read_page(table_id, parent, page);
            set_key_pagenum(0, neighbor_key, neighbor, page);
            buffer_write_page(table_id, parent, page);
            buffer_read_page(table_id, n, page);
            set_key_value(0, get_key_from_record(record1), get_value_from_record(record1), page);
            set_number_of_key(1, page);
            buffer_write_page(table_id, n, page);
        } else {
            buffer_read_page(table_id, neighbor, page);
            neighbor_num_keys = get_number_of_key(page);
            memcpy(record1, get_key_value(neighbor_num_keys - 1, page), sizeof(record));
            neighbor_key = get_key_from_record(record1);
            set_number_of_key( neighbor_num_keys - 1, page);
            buffer_write_page(table_id, neighbor, page);
            buffer_read_page(table_id, parent, page);
            set_key_pagenum(my_index, neighbor_key, *(pagenum_t *)(get_key_pagenum(my_index, page) + 8), page);
            buffer_write_page(table_id, parent, page);
            buffer_read_page(table_id, n, page);
            set_key_value(0, neighbor_key, get_value_from_record(record1), page);
            set_number_of_key(1, page);
            buffer_write_page(table_id, n, page);
        }
    }
    
    free(record1);
    free(page);
    return root;
}

//void change_key(pagenum_t root, pagenum_t n, record* pointer)
//{
//    int num_keys, is_leaf, my_index;
//    int64_t ch_key;
//    pagenum_t parent;
//
//    page_t* page = malloc(sizeof(page_t));
//    record* record1 = malloc(sizeof(record));
//
//    file_read_page(n, page);
//    num_keys = get_number_of_key(page);
//    is_leaf = get_is_leaf(page);
//    parent = get_parent_pagenum(page);
//
//    for (int i = 0; i < num_keys; ++i) {
//        if (is_leaf)
//            memcpy(record1, get_key_value(i, page), sizeof(record));
//        else
//            memcpy(record1, get_key_pagenum(i, page), 16);
//
//        if (get_key_from_record(record1) > get_key_from_record(pointer))
//        {
//            ch_key = get_key_from_record(record1);
//            break;
//        }
//    }
//    while (n != root) {
//        my_index = get_my_index(n);
//        file_read_page(parent, page);
//        if (my_index != -1)
//        {
//            memcpy(record1, get_key_pagenum(my_index, page), 16);
//            if (get_key_from_record(record1) == get_key_from_record(pointer))
//                set_key_pagenum(my_index, ch_key, n, page);
//            file_write_page(parent, page);
//        }
//        n = parent;
//        parent = get_parent_pagenum(page);
//    }
//    free(record1);
//    free(page);
//}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
pagenum_t delete_entry( int table_id, pagenum_t root, pagenum_t n, record * pointer ) {

    int min_keys;
    pagenum_t neighbor, parent;
    int is_leaf, my_index, neighbor_index;
    int num_keys;
    int64_t key;
    
    n = remove_entry_from_node(table_id, n, pointer);
    
    if (n == root) {
        root = adjust_root(table_id, root);
        return root;
    }

    page_t* page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, n, page);
    num_keys = get_number_of_key(page);
    parent = get_parent_pagenum(page);
    is_leaf = get_is_leaf(page);
    free(page);

//    min_keys = is_leaf ? cut(LEAF_PAGE_ORDER - 1) : cut(INTERNAL_PAGE_ORDER) - 1;
    min_keys = 1; // delayed merge
    if (num_keys >= min_keys) {
//        change_key(root, n, pointer);
        return root;
    }

    /* Case:  node falls below minimum.
     * Either coalescence or redistribution
     * is needed.
     */

    /* Find the appropriate neighbor node with which
     * to coalesce.
     * Also find the key (k_prime) in the parent
     * between the pointer to node n and the pointer
     * to the neighbor.
     */
    my_index = get_my_index( table_id, n );
    page = (page_t*)malloc(sizeof(page_t));
    buffer_read_page(table_id, parent, page);
    neighbor = my_index == -1 ? *(pagenum_t *)get_value_from_record((record*)get_key_pagenum(0,page)) :
               *(pagenum_t *)get_value_from_record((record*)get_key_pagenum(my_index - 1,page));
    neighbor_index = my_index == -1 ? 0 : my_index - 1;

//    capacity = is_leaf ? LEAF_PAGE_ORDER : INTERNAL_PAGE_ORDER - 1;

    buffer_read_page(table_id, neighbor, page);
    int neighbor_num_keys = get_number_of_key(page);
    /* Coalescence. */

    free(page);
    if (neighbor_num_keys + num_keys <= 1)
        return coalesce_nodes(table_id, root, n, neighbor, my_index, neighbor_index);
    else {
        root = redistribute_nodes(table_id, root, n, neighbor, my_index, neighbor_index);
//        change_key(root, n, pointer);
        return root;
    }
}



/* Master deletion function.
 */
pagenum_t delete(int table_id, pagenum_t root, int64_t key) {

    pagenum_t key_leaf;
    record * key_record = malloc(sizeof(record));

    int ck = find(table_id, root, key, false, key_record);
    key_leaf = find_leaf(table_id, root, key, false);
    if (!ck && key_leaf != 0) {
        root = delete_entry(table_id, root, key_leaf, key_record);
        free(key_record);
        return root;
    }
    free(key_record);
    return -1;
}


//void destroy_tree_nodes(node * root) {
//    int i;
//    if (root->is_leaf)
//        for (i = 0; i < root->num_keys; i++)
//            free(root->pointers[i]);
//    else
//        for (i = 0; i < root->num_keys + 1; i++)
//            destroy_tree_nodes(root->pointers[i]);
//    free(root->pointers);
//    free(root->keys);
//    free(root);
//}
//
//
//node * destroy_tree(node * root) {
//    destroy_tree_nodes(root);
//    return NULL;
//}