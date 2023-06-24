#pragma once

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS

#include "file_manager.h"

// Default order is 4.
#define DEFAULT_ORDER 4

// Minimum order is necessarily 3.  We set the maximum
// order arbitrarily.  You may change the maximum order.
#define MIN_ORDER 3
#define MAX_ORDER 20

// Constants for printing part or all of the GPL license.
#define LICENSE_FILE "LICENSE.txt"
#define LICENSE_WARRANTEE 0
#define LICENSE_WARRANTEE_START 592
#define LICENSE_WARRANTEE_END 624
#define LICENSE_CONDITIONS 1
#define LICENSE_CONDITIONS_START 70
#define LICENSE_CONDITIONS_END 625

// TYPES.

/* Type representing the record
 * to which a given key refers.
 * In a real B+ tree system, the
 * record would hold data (in a database)
 * or a file (in an operating system)
 * or some other information.
 * Users can rewrite this part of the code
 * to change the type and content
 * of the value field.
 */

typedef struct queue_node{
    pagenum_t now_page;
    int rank, is_first_pointer;
    struct queue_node* next;
} queue_node;

/* Type representing a node in the B+ tree.
 * This type is general enough to serve for both
 * the leaf and the internal node.
 * The heart of the node is the array
 * of keys and the array of corresponding
 * pointers.  The relation between keys
 * and pointers differs between leaves and
 * internal nodes.  In a leaf, the index
 * of each key equals the index of its corresponding
 * pointer, with a maximum of order - 1 key-pointer
 * pairs.  The last pointer points to the
 * leaf to the right (or NULL in the case
 * of the rightmost leaf).
 * In an internal node, the first pointer
 * refers to lower nodes with keys less than
 * the smallest key in the keys array.  Then,
 * with indices i starting at 0, the pointer
 * at i + 1 points to the subtree with keys
 * greater than or equal to the key in this
 * node at index i.
 * The num_keys field is used to keep
 * track of the number of valid keys.
 * In an internal node, the number of valid
 * pointers is always num_keys + 1.
 * In a leaf, the number of valid pointers
 * to data is always num_keys.  The
 * last leaf pointer points to the next leaf.
 */

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
extern int order;

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */

extern queue_node * queue;

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
extern bool verbose_output;

// FUNCTION PROTOTYPES.

// Output and utility.
void change(int64_t num, char* buf);
//void change_key(int table_id, pagenum_t root, pagenum_t n, record* pointer);
int print_leaves( int table_id );
//void license_notice( void );
//void print_license( int licence_part );
//void usage_1( void );
//void usage_2( void );
//void usage_3( void );
void enqueue( queue_node * new_node );
queue_node * dequeue( void );
void print_tree( int table_id );
pagenum_t find_leaf( int table_id, pagenum_t root, int64_t key, bool verbose );
int find( int table_id, pagenum_t root, int64_t key, bool verbose, record* record1 );
int cut( int length );
record * make_record(char* value);
pagenum_t make_node( int table_id );
pagenum_t make_leaf( int table_id );
int get_left_index( int table_id, pagenum_t parent, pagenum_t left);
pagenum_t insert_into_leaf( int table_id,  pagenum_t leaf, record * pointer );
pagenum_t insert_into_leaf_after_splitting( int table_id, pagenum_t root, pagenum_t leaf, record * pointer);
pagenum_t insert_into_node( int table_id, pagenum_t root, pagenum_t n,
                           int left_index, int64_t key, pagenum_t right);
pagenum_t insert_into_node_after_splitting( int table_id, pagenum_t root, pagenum_t old_node, int left_index,
                                           int64_t key, pagenum_t right);
pagenum_t insert_into_parent( int table_id, pagenum_t root, pagenum_t left, int64_t key, pagenum_t right);
pagenum_t insert_into_new_root( int table_id, pagenum_t left, int64_t key, pagenum_t right);
pagenum_t start_new_tree( int table_id, record * pointer);
pagenum_t insert( int table_id,  pagenum_t root, int64_t key, char* value );


// Deletion.

int get_my_index( int table_id, pagenum_t n );
pagenum_t adjust_root( int table_id, pagenum_t root);
pagenum_t coalesce_nodes( int table_id, pagenum_t root, pagenum_t n, pagenum_t neighbor, int n_index, int neighbor_index);
pagenum_t redistribute_nodes( int table_id, pagenum_t root, pagenum_t n, pagenum_t neighbor, int my_index, int neighbor_index);
pagenum_t remove_entry_from_node( int table_id, pagenum_t n, record* pointer);
pagenum_t delete_entry( int table_id, pagenum_t root, pagenum_t n, record * pointer );
pagenum_t delete( int table_id, pagenum_t root, int64_t key);
//
//void destroy_tree_nodes(node * root);
//node * destroy_tree(node * root);

// Milestone 2

int open_table( char* pathname);
int db_insert( int table_id, int64_t key, char* value);
int db_find( int table_id, int64_t key, char* ret_val);
int db_delete( int table_id, int64_t key);
