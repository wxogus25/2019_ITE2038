#include "lock_manager.h"

typedef struct queue_node{
    pagenum_t now_page;
    int rank, is_first_pointer;
    struct queue_node* next;
} queue_node;

extern int order;
extern queue_node * queue;
extern bool verbose_output;

void change(int64_t num, char* buf);
int print_leaves( int table_id );
void enqueue( queue_node * new_node );
queue_node * dequeue( void );
void print_tree( int table_id );
pagenum_t find_leaf( int table_id, buffer *root, int64_t key, bool verbose );
int find( int table_id, buffer *root, int64_t key, bool verbose, record* record1 );
int cut( int length );
record * make_record(char* value);
pagenum_t make_node( int table_id );
pagenum_t make_leaf( int table_id );
int get_left_index(buffer *parent, buffer *left);
pagenum_t insert_into_leaf(buffer *leaf, record * pointer );
pagenum_t insert_into_leaf_after_splitting( int table_id, buffer *root, buffer *leaf, record * pointer);
pagenum_t insert_into_node(buffer *root, buffer *n,
                           int left_index, int64_t key, buffer *right);
pagenum_t insert_into_node_after_splitting( int table_id, buffer *root, buffer *ld_node, int left_index,
                                           int64_t key, buffer *right);
pagenum_t insert_into_parent( int table_id, buffer *root, buffer *left, int64_t key, buffer *right);
pagenum_t insert_into_new_root( int table_id, buffer *left, int64_t key, buffer *right);
pagenum_t start_new_tree( int table_id, record * pointer);
pagenum_t insert( int table_id, buffer *root, int64_t key, char* value );

int get_my_index( int table_id, buffer *n );
pagenum_t adjust_root( int table_id, buffer *root);
pagenum_t coalesce_nodes( int table_id, buffer *root, buffer *n,buffer *neighbor, int n_index, int neighbor_index);
pagenum_t redistribute_nodes( int table_id, buffer *root, buffer *n,buffer *neighbor, int my_index, int neighbor_index);
pagenum_t remove_entry_from_node( buffer *n, record* pointer);
pagenum_t delete_entry( int table_id, buffer *root, buffer *n, record * pointer );
pagenum_t _delete( int table_id, buffer *root, int64_t key);

int open_table( char* pathname);
int db_insert( int table_id, int64_t key, char* value);
int db_find( int table_id, int64_t key, char* ret_val);
int db_delete( int table_id, int64_t key);

int join_table(int table_id_1, int table_id_2, char* pathname);