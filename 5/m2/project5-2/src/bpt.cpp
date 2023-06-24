#include "bpt.h"

queue_node * queue = nullptr;

int open_table(char* pathname)
{
    int ck = 0;
    buffer* buffer1;
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
    if (fd[ck] == nullptr)
    {
        fd[ck] = fopen(pathname, "w");
        fclose(fd[ck]);
        fd[ck] = fopen(pathname, "r+b");
    }else
    {
        buffer1 = buffer_read_page(ck, 0);
        page_size[ck] = get_number_of_page(&buffer1->page);
        --(buffer1->pin);
        used_fd[ck] = 1;
        table_name[ck] = (char *)malloc(120);
        strcpy(table_name[ck], pathname);
        return ck;
    }
    if(fd[ck] == nullptr)
    {
        puts("fail file open");
        return -1;
    }
    used_fd[ck] = 1;
    table_name[ck] = (char *)malloc(120);
    strcpy(table_name[ck], pathname);
    
    buffer_alloc_page(ck);
    buffer1 = buffer_read_page(ck, 0);
    
    set_free_pagenum(0, &buffer1->page);
    set_root_pagenum(0, &buffer1->page);
    set_number_of_page(0, &buffer1->page);
    --(buffer1->pin);
    buffer_write(buffer1);
    return 0;
}

int db_insert(int table_id, int64_t key, char* value)
{
    buffer *buffer1, *root;
    pagenum_t rootnum, ck;
    buffer1 = buffer_read_page(table_id, 0);
    rootnum = get_root_pagenum(&buffer1->page);
    root = buffer_read_page(table_id, rootnum);
    ck = insert(table_id, root, key, value);
    --(root->pin);
    --(buffer1->pin);
    if (ck == -1)
        return 1;
    else if (ck == root->page_num) {
        if (ck == 0)
            return 1;
        return 0;
    }
    set_root_pagenum(ck, &buffer1->page);
    buffer_write(buffer1);
    return 0;
}

int db_find( int table_id, int64_t key, char* ret_val, int trx_id)
{
	while (true) {
		bpool.buffer_pool.lock();
		buffer *buffer1, *root;
		pagenum_t rootnum, c;
		buffer1 = buffer_read_page(table_id, 0);
		rootnum = get_root_pagenum(&buffer1->page);
		root = buffer_read_page(table_id, rootnum);
		c = find_leaf(table_id, root, key, false);

		//buffer page latch
		bool ex = false, ac = false;
		for (auto x : bpage.page_latch_list[{table_id, c}]) {
			if (x->key == key) {
				ex = true;
				ac = x->acquire;
				if (!ac)
					x->acquire = true;
				break;
			}
		}
		b_page_latch* temp = (b_page_latch*)malloc(sizeof(b_page_latch));
		if (!ex) {
			temp->acquire = true;
			temp->key = key;
			bpage.page_latch_list[{table_id, c}].push_back(temp);
		}
		else {
			free(temp);
			if (ac) {
				bpool.buffer_pool.unlock();
				--(root->pin);
				--(buffer1->pin);
				continue;
			}
		}
		bpool.buffer_pool.unlock();
		//
		int tar_tid = 0;
		int check = acquire_record_lock(trx_id, 1, table_id, c, key, tar_tid);
		if (check != 0) {
			bpool.buffer_pool.lock();
			for (auto x : bpage.page_latch_list[{table_id, c}]) {
				if (x->key == key) {
					x->acquire = false;
					break;
				}
			}
			bpool.buffer_pool.unlock();
			if (check == 1) {
				std::unique_lock<std::mutex> ul(trx_list.Transaction_Table[trx_id]->trx_mutex);
				trx_list.Transaction_Table[tar_tid]->trx_cond.wait(ul);
				trx_list.Transaction_Table[trx_id]->wait_lock = NULL;
				ul.unlock();
				--(root->pin);
				--(buffer1->pin);
				continue;
			}
			else if (check == 2) {
				abort_trx(trx_id);
				--(root->pin);
				--(buffer1->pin);
				return 1;
			}
		}
		bpool.buffer_pool.lock();
		record *record1 = (record *)malloc(sizeof(record));
		int ck = find(table_id, c, key, false, record1);
		if (!ck)
			memcpy(ret_val, get_value_from_record(record1), 120);
		free(record1);
		--(root->pin);
		--(buffer1->pin);
		for (auto x : bpage.page_latch_list[{table_id, c}]) {
			if (x->key == key) {
				x->acquire = false;
				break;
			}
		}
		bpool.buffer_pool.unlock();
		return 0;//ck
	}
}

int db_update(int table_id, int64_t key, char* values, int trx_id) {
	while (true) {
		bpool.buffer_pool.lock();
		buffer *buffer1, *root;
		pagenum_t rootnum, c;
		buffer1 = buffer_read_page(table_id, 0);
		rootnum = get_root_pagenum(&buffer1->page);
		root = buffer_read_page(table_id, rootnum);
		c = find_leaf(table_id, root, key, false);

		//buffer page latch
		bool ex = false, ac = false;
		for (auto x : bpage.page_latch_list[{table_id, c}]) {
			if (x->key == key) {
				ex = true;
				ac = x->acquire;
				if (!ac)
					x->acquire = true;
				break;
			}
		}
		b_page_latch* temp = (b_page_latch*)malloc(sizeof(b_page_latch));
		if (!ex) {
			temp->acquire = true;
			temp->key = key;
			bpage.page_latch_list[{table_id, c}].push_back(temp);
		}
		else {
			free(temp);
			if (ac) {
				bpool.buffer_pool.unlock();
				--(root->pin);
				--(buffer1->pin);
				continue;
			}
		}
		bpool.buffer_pool.unlock();
		//
		int tar_tid = 0;
		int check = acquire_record_lock(trx_id, 0, table_id, c, key, tar_tid);
		if (check != 0) {
			bpool.buffer_pool.lock();
			for (auto x : bpage.page_latch_list[{table_id, c}]) {
				if (x->key == key) {
					x->acquire = false;
					break;
				}
			}
			bpool.buffer_pool.unlock();
			if (check == 1) {
				std::unique_lock<std::mutex> ul(trx_list.Transaction_Table[trx_id]->trx_mutex);
				trx_list.Transaction_Table[tar_tid]->trx_cond.wait(ul);
				trx_list.Transaction_Table[trx_id]->wait_lock = NULL;
				ul.unlock();
				--(root->pin);
				--(buffer1->pin);
				continue;
			}
			else if (check == 2) {
				abort_trx(trx_id);
				--(root->pin);
				--(buffer1->pin);
				return 1;
			}
		}
		bpool.buffer_pool.lock();
		buffer* buffer2 = buffer_read_page(table_id, c);
		record* record1 = (record*)malloc(sizeof(record));
		int num_keys = get_number_of_key(&buffer2->page), i = 0;
		for (i = 0; i < num_keys; i++) {
			memcpy(record1, (void*)get_key_value(i, &buffer2->page), sizeof(record));
			if (get_key_from_record(record1) == key) {
				undo *un = (undo*)malloc(sizeof(undo));
				un->key = key;
				un->table_id = table_id;
				un->pagenum = c;
				un->value = (char*)malloc(120);
				un->index = i;
				memcpy(un->value, (void*)(record1 + 8), 120);
				trx_list.Transaction_Table[trx_id]->undo_list.push_back(un);
				set_key_value(i, key, values, &buffer2->page);
			}
		}
		free(record1);
		--(buffer2->pin);
		--(root->pin);
		--(buffer1->pin);
		for (auto x : bpage.page_latch_list[{table_id, c}]) {
			if (x->key == key) {
				x->acquire = false;
				break;
			}
		}
		bpool.buffer_pool.unlock();
		return 0;
	}
}

int db_delete(int table_id, int64_t key)
{
    buffer *buffer1, *root;
    pagenum_t rootnum, ck;
    buffer1 = buffer_read_page(table_id, 0);
    rootnum = get_root_pagenum(&buffer1->page);
    root = buffer_read_page(table_id, rootnum);
    ck = _delete(table_id, root, key);
    --(root->pin);
    --(buffer1->pin);
    if (ck == -1)
        return 1;
    if(ck == root->page_num)
    {
        if (ck == 0)
            return 1;
        return 0;
    }
    set_root_pagenum(ck, &buffer1->page);
    buffer_write(buffer1);
    return 0;
}

void enqueue( queue_node *new_node ) {
    queue_node *c;
    if (queue == nullptr) {
        queue = new_node;
        queue->next = nullptr;
    }
    else {
        c = queue;
        while(c->next != nullptr) {
            c = c->next;
        }
        c->next = new_node;
        new_node->next = nullptr;
    }
}

queue_node *dequeue() {
    queue_node * n = queue;
    queue = queue->next;
    n->next = nullptr;
    return n;
}

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
    buffer* buffer1;
    record *record1;
    record1 = (record *) malloc(sizeof(record));
    buffer1 = buffer_read_page(table_id, 0);
    pagenum_t c = get_root_pagenum(&buffer1->page);
    --(buffer1->pin);
    if (c == 0) {
        free(record1);
        printf("Empty tree.\n");
        return 0;
    }
    char ck[120], value[120];
    buffer1 = buffer_read_page(table_id, c);
    int64_t pre = -1, key;
    while (!get_is_leaf(&buffer1->page)) {
        c = get_one_more_pagenum(&buffer1->page);
        --(buffer1->pin);
        buffer1 = buffer_read_page(table_id, c);
    }
    int sum = 0;
    while (true) {
        int num_keys = get_number_of_key(&buffer1->page);
        for (i = 0; i < num_keys; i++) {
            memcpy(record1, get_key_value(i, &buffer1->page), sizeof(record));
            key = get_key_from_record(record1);
            change(key, ck);
            memcpy(value, get_value_from_record(record1) + 4, 108);
            if (key < pre || strcmp(value, ck) != 0) {
                printf("value : %s, key : %lld, pre : %lld, index : %d\n", value, key, pre, i);
                return -1;
            }
            pre = key;
        }
        sum += num_keys;
        if (get_right_sibling_pagenum(&buffer1->page) != 0) {
            c = get_right_sibling_pagenum(&buffer1->page);
            --(buffer1->pin);
            buffer1=buffer_read_page(table_id, c);
        }
        else
            break;
    }
    free(record1);
    --(buffer1->pin);
    return sum;
}

void print_tree( int table_id ) {
    queue_node *n = nullptr, *root, *x;
    int i = 0;
    int rank = 0;
    int new_rank = 0;
    buffer* buffer1;
    buffer1 = buffer_read_page(table_id, 0);
    pagenum_t rootnum = get_root_pagenum(&buffer1->page);
    printf("table_id : %d, table_name : %s root : %lld\n", table_id, table_name[table_id], rootnum);
    --(buffer1->pin);
    if (rootnum == 0) {
        printf("Empty tree.\n");
        return;
    }
    root = (queue_node *) malloc(sizeof(queue_node));
    root->is_first_pointer = root->rank = 0;
    root->now_page = rootnum;
    root->next = nullptr;
    queue = nullptr;
    record *record1;
    record1 = (record *) malloc(sizeof(record));
    enqueue(root);
    while( queue != nullptr ) {
        n = dequeue();
        buffer1 = buffer_read_page(table_id, n->now_page);
        if (n->rank != 0 && n->is_first_pointer) {
            new_rank = n->rank;
            if (new_rank != rank) {
                rank = new_rank;
                printf("\n");
            }
        }
        int num_keys = get_number_of_key(&buffer1->page);
        int is_leaf = get_is_leaf(&buffer1->page);
        for (i = 0; i < num_keys; i++) {
            if (is_leaf)
                memcpy(record1, (void*)get_key_value(i, &buffer1->page), 128);
            else
                memcpy(record1, (void*)get_key_pagenum(i, &buffer1->page), 16);
            if(is_leaf)
            {
                char d[120];
                memcpy(d, get_value_from_record(record1), 120);
                printf("(%lld, %s) ", get_key_from_record(record1), d);
            } else
                printf("%lld ", get_key_from_record(record1));
        }
        if (!is_leaf)
            for (i = -1; i < num_keys; i++)
            {
                x = (queue_node *) malloc(sizeof(queue_node));
                x->rank = n->rank + 1;
                x->is_first_pointer = (i == -1);
                memcpy(record1, (void*)get_key_pagenum(i, &buffer1->page), 16);
                x->now_page = *(pagenum_t*)(get_value_from_record(record1));
                enqueue(x);
            }
        printf("| ");
        free(n);
        --(buffer1->pin);
    }
    free(record1);
    printf("\n");
}

pagenum_t find_leaf( int table_id, buffer *root, int64_t key, bool verbose ) {
    if (root->page_num == 0) {
        if (verbose)
            printf("Empty tree.\n");
        return 0;
    }
    int i = 0, is_leaf = get_is_leaf(&root->page), num_keys = get_number_of_key(&root->page);
    record* record1 = (record*)malloc(sizeof(record));
    pagenum_t now = root->page_num;
    buffer *buffer1 = root;
    while (!is_leaf) {
        i = 0;
        memcpy(record1,(void*)get_key_pagenum(-1, &buffer1->page),16);
        pagenum_t first_pagenum = *(pagenum_t *)(get_value_from_record(record1));
        while (i < num_keys) {
            memcpy(record1,(void*)get_key_pagenum(i, &buffer1->page), 16);
            if (key >= get_key_from_record(record1))
                i++;
            else
                break;
        }
        if (i == 0)
            now = first_pagenum;
        else
            now = *(pagenum_t*)(get_value_from_record((record*)get_key_pagenum(i - 1, &buffer1->page)));
        if (buffer1->page_num != root->page_num)
            --(buffer1->pin);
        buffer1 = buffer_read_page(table_id, now);
        is_leaf = get_is_leaf(&buffer1->page);
        num_keys = get_number_of_key(&buffer1->page);
    }
    free(record1);
    if (buffer1->page_num != root->page_num)
        --(buffer1->pin);
    return now;
}

int find(int table_id, pagenum_t c, int64_t key, bool verbose, record* record1 ) {
    if (verbose)
        printf("leaf page : %lld\n", c);
    if (c == 0) return 1;
	int i = 0;
    buffer *buffer1;
    buffer1 = buffer_read_page(table_id, c);
    int num_keys = get_number_of_key(&buffer1->page);
    for (i = 0; i < num_keys; i++) {
        memcpy(record1, (void*)get_key_value(i, &buffer1->page),sizeof(record));
        if(verbose)
            printf("%lld ", get_key_from_record(record1));
        if (get_key_from_record(record1) == key) break;
    }
    --(buffer1->pin);
    return i == num_keys;
}

int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}

record *make_record(char* value) {
    record *new_record = (record *)malloc(sizeof(record));
    if (new_record == NULL) {
        free(new_record);
        perror("Record creation.");
        exit(EXIT_FAILURE);
    }
    else
        set_value_to_record(value, new_record);
    return new_record;
}

pagenum_t make_node( int table_id ) {
    pagenum_t new_node;
    buffer *buffer1;
    new_node = buffer_alloc_page(table_id);
    if (new_node < 0) {
        perror("Node creation.");
        exit(EXIT_FAILURE);
    }
    buffer1 = buffer_read_page(table_id, new_node);
    set_parent_pagenum(0, &buffer1->page);
    set_is_leaf(0, &buffer1->page);
    set_number_of_key(0, &buffer1->page);
    set_one_more_pagenum(0, &buffer1->page);
    --(buffer1->pin);
    buffer_write(buffer1);
    return new_node;
}

pagenum_t make_leaf( int table_id ) {
    pagenum_t leaf = make_node(table_id);
    buffer *buffer1;
    buffer1 = buffer_read_page(table_id, leaf);
    set_is_leaf(1, &buffer1->page);
    --(buffer1->pin);
    buffer_write(buffer1);
    return leaf;
}

int get_left_index(buffer *parent, buffer *left) {
    int left_index = 0, num_keys;
    record* record1 = (record*)malloc(sizeof(record));
    num_keys = get_number_of_key(&parent->page);
    
    memcpy(record1, get_key_pagenum(-1, &parent->page), 16);
    if (*(pagenum_t *)(get_value_from_record(record1)) == left->page_num)
    {
        free(record1);
        return 0;
    }
    
    while (left_index < num_keys) {
        memcpy(record1, get_key_pagenum(left_index, &parent->page), 16);
        if (*(pagenum_t*)(get_value_from_record(record1)) != left->page_num)
            left_index++;
        else
            break;
    }
    free(record1);
    return left_index + 1;
}

pagenum_t insert_into_leaf(buffer *leaf, record *pointer ) {
    
    int i, insertion_point;
    record* record1 = (record*)malloc(sizeof(record));
    int num_keys = get_number_of_key(&leaf->page);
    insertion_point = 0;
    while (insertion_point < num_keys) {
        memcpy(record1, get_key_value(insertion_point, &leaf->page), sizeof(record));
        if (get_key_from_record(record1) < get_key_from_record(pointer))
            insertion_point++;
        else
            break;
    }
    for (i = num_keys; i > insertion_point; i--) {
        memcpy(record1, get_key_value(i - 1, &leaf->page), sizeof(record));
        set_key_value(i, get_key_from_record(record1), get_value_from_record(record1), &leaf->page);
    }
    set_key_value(insertion_point, get_key_from_record(pointer), get_value_from_record(pointer), &leaf->page);
    set_number_of_key(num_keys+1, &leaf->page);
    free(record1);
    buffer_write(leaf);
    return leaf->page_num;
}

pagenum_t insert_into_leaf_after_splitting(int table_id, buffer *root, buffer *leaf, record * pointer) {
    
    buffer *new_leaf;
    pagenum_t right_sibling, parent, new_root, new_leafnum;
    int64_t new_key, *temp_keys;
    void **temp_pointers;
    int insertion_index, split, i, j, num_keys;
    
    new_leafnum = make_leaf(table_id);
    new_leaf = buffer_read_page(table_id, new_leafnum);
    
    temp_pointers = (void **)malloc( LEAF_PAGE_ORDER * sizeof(char *) );
    for (int k = 0; k < LEAF_PAGE_ORDER; ++k)
        temp_pointers[k] = malloc(120);
    
    temp_keys = (int64_t*)malloc(LEAF_PAGE_ORDER * sizeof(int64_t));
    record* record1 = (record*)malloc(sizeof(record));
    
    num_keys = get_number_of_key(&leaf->page);
    right_sibling = get_right_sibling_pagenum(&leaf->page);
    parent = get_parent_pagenum(&leaf->page);
    insertion_index = 0;
    
    while (insertion_index < LEAF_PAGE_ORDER - 1) {
        memcpy(record1, get_key_value(insertion_index, &leaf->page), sizeof(record));
        if (get_key_from_record(record1) < get_key_from_record(pointer))
            insertion_index++;
        else
            break;
    }
    
    for (i = 0, j = 0; i < num_keys; i++, j++) {
        if (j == insertion_index) j++;
        memcpy(record1,get_key_value(i, &leaf->page), sizeof(record));
        temp_keys[j] = get_key_from_record(record1);
        memcpy(temp_pointers[j], get_value_from_record(record1), 120);
    }
    
    temp_keys[insertion_index] = get_key_from_record(pointer);
    memcpy(temp_pointers[insertion_index],get_value_from_record(pointer), 120);
    
    num_keys = 0;
    
    split = cut(LEAF_PAGE_ORDER - 1);
    
    for (i = 0; i < split; i++) {
        set_key_value(i, temp_keys[i], (char *)temp_pointers[i], &leaf->page);
        num_keys++;
    }
    
    set_number_of_key(num_keys, &leaf->page);
    set_right_sibling_pagenum(new_leafnum, &leaf->page);
    buffer_write(leaf);

    num_keys = 0;
    for (i = split, j = 0; i < LEAF_PAGE_ORDER; i++, j++) {
        set_key_value(j, temp_keys[i], (char *)temp_pointers[i], &new_leaf->page);
        num_keys++;
    }
    set_number_of_key(num_keys, &new_leaf->page);
    set_right_sibling_pagenum(right_sibling, &new_leaf->page);
    set_parent_pagenum(parent, &new_leaf->page);
    buffer_write(new_leaf);
    
    for (int k = 0; k < LEAF_PAGE_ORDER; ++k)
        free(temp_pointers[k]);
    
    free(temp_pointers);
    free(temp_keys);
    
    memcpy(record1, get_key_value(0, &new_leaf->page), sizeof(record));
    new_key = get_key_from_record(record1);
    free(record1);
    new_root = insert_into_parent(table_id, root, leaf, new_key, new_leaf);
    --(new_leaf->pin);
    return new_root;
}

pagenum_t insert_into_node(buffer *root, buffer *n, int left_index, int64_t key, buffer *right)
{
    int i, num_keys;
    record* record1 = (record*)malloc(sizeof(record));
    num_keys = get_number_of_key(&n->page);
    for (i = num_keys - 1; i >= left_index; i--) {
        memcpy(record1, get_key_pagenum(i, &n->page), 16);
        set_key_pagenum(i + 1, get_key_from_record(record1), *(pagenum_t *)(get_value_from_record(record1)), &n->page);
    }
    set_key_pagenum(left_index, key, right->page_num, &n->page);
    set_number_of_key(num_keys+1, &n->page);
    free(record1);
    buffer_write(n);
    return root->page_num;
}

pagenum_t insert_into_node_after_splitting(int table_id, buffer *root, buffer *old_node, int left_index, int64_t key, buffer *right)
{
    int i, j, split;
    pagenum_t new_nodenum, new_root, parent, *temp_pointers;
    buffer *new_node, *child;
    int64_t *temp_keys, k_prime;
    
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
    
    record* record1 = (record*)malloc(sizeof(record));
    
    parent = get_parent_pagenum(&old_node->page);
    int num_keys = get_number_of_key(&old_node->page);
    
    temp_pointers[0] = get_one_more_pagenum(&old_node->page);
    for (i = 0, j = 1; i < num_keys; i++, j++) {
        if (j == left_index + 1) j++;
        memcpy(record1, get_key_pagenum(i, &old_node->page), 16);
        temp_pointers[j] = *(pagenum_t*)(get_value_from_record(record1));
    }
    
    for (i = 0, j = 0; i < num_keys; i++, j++) {
        if (j == left_index) j++;
        memcpy(record1, get_key_pagenum(i, &old_node->page), 16);
        temp_keys[j] = get_key_from_record(record1);
    }
    
    temp_pointers[left_index + 1] = right->page_num;
    temp_keys[left_index] = key;
    
    split = cut(INTERNAL_PAGE_ORDER);
    new_nodenum = make_node(table_id);
    
    num_keys = 0;
    set_key_pagenum(-1, 0, temp_pointers[0], &old_node->page);
    for (i = 0; i < split - 1; i++) {
        set_key_pagenum(i,temp_keys[i], temp_pointers[i + 1], &old_node->page);
        num_keys++;
    }
    set_number_of_key(num_keys, &old_node->page);
    buffer_write(old_node);
    
    k_prime = temp_keys[split - 1];
    
    new_node = buffer_read_page(table_id, new_nodenum);
    set_key_pagenum(-1,0,temp_pointers[i + 1], &new_node->page);
    temp_pointers[0] = temp_pointers[i + 1];
    num_keys = 0;
    for (++i, j = 0; i < INTERNAL_PAGE_ORDER; i++, j++) {
        set_key_pagenum(j, temp_keys[i], temp_pointers[i + 1], &new_node->page);
        temp_pointers[j + 1] = temp_pointers[i + 1];
        num_keys++;
    }
    set_number_of_key(num_keys, &new_node->page);
    set_parent_pagenum(parent, &new_node->page);
    buffer_write(new_node);
    
    for (i = 0; i <= num_keys; i++) {
        child = buffer_read_page(table_id, temp_pointers[i]);
        set_parent_pagenum(new_nodenum, &child->page);
        --(child->pin);
        buffer_write(child);
    }
    free(temp_pointers);
    free(temp_keys);
    free(record1);
    
    new_root = insert_into_parent(table_id, root, old_node, k_prime, new_node);
    --(new_node->pin);
    return new_root;
}

pagenum_t insert_into_parent(int table_id, buffer *root, buffer *left, int64_t key, buffer *right)
{
    int left_index, num_keys;
    pagenum_t new_root, parentnum;
    buffer *parent;
    parentnum = get_parent_pagenum(&left->page);
    parent = buffer_read_page(table_id, parentnum);
    
    if (parent->page_num == 0) {
        --(parent->pin);
        return insert_into_new_root(table_id, left, key, right);
    }
    
    left_index = get_left_index(parent, left);
    
    num_keys = get_number_of_key(&parent->page);
    
    if (num_keys < INTERNAL_PAGE_ORDER - 1) {
        new_root = insert_into_node(root, parent, left_index, key, right);
        --(parent->pin);
        return new_root;
    }
    new_root = insert_into_node_after_splitting(table_id, root, parent, left_index, key, right);
    --(parent->pin);
    return new_root;
}

pagenum_t insert_into_new_root(int table_id, buffer *left, int64_t key, buffer *right) {
    
    pagenum_t new_root = make_node(table_id);
    buffer *root;
    
    root = buffer_read_page(table_id, new_root);
    set_key_pagenum(-1, 0, left->page_num, &root->page);
    set_key_pagenum(0, key, right->page_num, &root->page);
    set_number_of_key(get_number_of_key(&root->page) + 1, &root->page);
    set_parent_pagenum(new_root, &left->page);
    set_parent_pagenum(new_root, &right->page);
    --(root->pin);
    buffer_write(root);
    buffer_write(left);
    buffer_write(right);
    return new_root;
}

pagenum_t start_new_tree(int table_id, record * pointer) {
    pagenum_t root = make_leaf(table_id);
    buffer *buffer1;
    buffer1 = buffer_read_page(table_id, root);
    set_key_value(0, get_key_from_record(pointer), get_value_from_record(pointer), &buffer1->page);
    set_number_of_key(1, &buffer1->page);
    --(buffer1->pin);
    buffer_write(buffer1);
    return root;
}

pagenum_t insert(int table_id, buffer *root, int64_t key, char* value )
{
    record * pointer = (record*)malloc(sizeof(record));
	pagenum_t c = find_leaf(table_id, root, key, false);
    int ck = find(table_id, c, key, false, pointer);
    free(pointer);
    if (!ck)
        return -1;
    pagenum_t new_root, leafnum;
    pointer = make_record(value);
    set_key_to_record(key, pointer);
    if (root->page_num == 0)
    {
        new_root = start_new_tree(table_id, pointer);
        free(pointer);
        return new_root;
    }
    leafnum = find_leaf(table_id, root, key, false);
    buffer *leaf = buffer_read_page(table_id, leafnum);
    int num_keys = get_number_of_key(&leaf->page);
    if (num_keys < LEAF_PAGE_ORDER - 1)
    {
        insert_into_leaf(leaf, pointer);
        free(pointer);
        --(leaf->pin);
        return root->page_num;
    }
    new_root = insert_into_leaf_after_splitting(table_id, root, leaf, pointer);
    free(pointer);
    --(leaf->pin);
    return new_root;
}

int get_my_index( int table_id, buffer *n ) {
    int i, num_keys;
    pagenum_t parentnum = get_parent_pagenum(&n->page);
    buffer *parent = buffer_read_page(table_id, parentnum);
    num_keys = get_number_of_key(&parent->page);

    for (i = -1; i < num_keys; i++) {
        if (*(pagenum_t *)get_value_from_record((record *) get_key_pagenum(i, &parent->page)) == n->page_num) {
            --(parent->pin);
            return i;
        }
    }
    printf("Search for nonexistent pointer to node in parent.\n");
    printf("Node:  %#lx\n", (unsigned long)n);
    exit(EXIT_FAILURE);
}

pagenum_t remove_entry_from_node( buffer *n, record* pointer) {
    int i = 0, num_keys, is_leaf;
    num_keys = get_number_of_key(&n->page);
    is_leaf = get_is_leaf(&n->page);

    if (is_leaf) {
        while (i < num_keys && get_key_from_record((record *) get_key_value(i, &n->page)) != get_key_from_record(pointer))
            i++;
        for (++i; i < num_keys; i++)
            set_key_value(i - 1, get_key_from_record((record *) get_key_value(i, &n->page)), get_value_from_record((record *) get_key_value(i, &n->page)), &n->page);
    }else {
        while (i < num_keys && get_key_from_record((record *) get_key_pagenum(i, &n->page)) != get_key_from_record(pointer))
            i++;
        for (++i; i < num_keys; i++)
            set_key_pagenum(i - 1, get_key_from_record((record *) get_key_pagenum(i, &n->page)),
                            *(pagenum_t *) get_value_from_record((record *) get_key_pagenum(i - 1, &n->page)), &n->page);
        i = -1;
        while (*(pagenum_t *) get_value_from_record((record *) get_key_pagenum(i, &n->page)) != *(pagenum_t *) get_value_from_record(pointer))
            i++;
        for (++i; i < num_keys; i++)
            set_key_pagenum(i - 1, get_key_from_record((record *) get_key_pagenum(i - 1, &n->page)),
                            *(pagenum_t *) get_value_from_record((record *) get_key_pagenum(i, &n->page)), &n->page);
    }
    set_number_of_key( num_keys - 1, &n->page);
    buffer_write(n);
    return n->page_num;
}

pagenum_t adjust_root(int table_id, buffer *root) {
    int num_keys = get_number_of_key(&root->page), is_leaf = get_is_leaf(&root->page);
    if (num_keys > 0) {
        return root->page_num;
    }
    pagenum_t new_root;
    buffer *buffer1;
    if (!is_leaf) {
        new_root = get_one_more_pagenum(&root->page);
        buffer1 = buffer_read_page(table_id, new_root);
        set_parent_pagenum(0, &buffer1->page);
        --(buffer1->pin);
        buffer_write(buffer1);
    }
    else
        new_root = 0;
    buffer_free_page(table_id, root);
    return new_root;
}

pagenum_t coalesce_nodes(int table_id, buffer *root, buffer *n, buffer *neighbor, int n_index, int neighbor_index) {
    pagenum_t rightsibling, parent_n_pointer, new_root, parentnum, leftmostnum;
    buffer *parent, *leftmost, *tmp;
    record* record1 = (record*)malloc(sizeof(record));
    int is_leaf = get_is_leaf(&n->page);
    parentnum = get_parent_pagenum(&n->page);
    parent = buffer_read_page(table_id, parentnum);
    int64_t parent_key;
    
    if (!is_leaf) {
        leftmostnum = get_one_more_pagenum(&n->page);
        leftmost = buffer_read_page(table_id, leftmostnum);
        if(n_index < neighbor_index)
        {
            parent_key = *(int64_t *)get_key_pagenum(0, &parent->page);
            parent_n_pointer = get_one_more_pagenum(&parent->page);
            for (int k = 1; k >= 0; k--)
                set_key_pagenum(k, *(int64_t *)get_key_pagenum(k - 1, &neighbor->page), *(pagenum_t*)(get_key_pagenum(k - 1, &neighbor->page) + 8), &neighbor->page);
            set_key_pagenum(0, parent_key, *(pagenum_t *)(get_key_pagenum(0, &neighbor->page) + 8), &neighbor->page);
            set_one_more_pagenum(leftmostnum, &neighbor->page);
        }else
        {
            parent_key = *(int64_t*)get_key_pagenum(n_index, &parent->page);
            parent_n_pointer = *(pagenum_t*)(get_key_pagenum(n_index, &parent->page) + 8);
            set_key_pagenum(1, parent_key, leftmostnum, &neighbor->page);
        }
        set_number_of_key(2, &neighbor->page);
        set_parent_pagenum(neighbor->page_num, &leftmost->page);
        --(leftmost->pin);
        buffer_write(leftmost);
        buffer_write(neighbor);
    }
    else {
        rightsibling = get_right_sibling_pagenum(&n->page);
        if(n_index < neighbor_index)
        {
            parent_key = *(int64_t*)get_key_pagenum(0, &parent->page);
            parent_n_pointer = *(pagenum_t *)(get_key_pagenum(0, &parent->page) + 8);
            memcpy(record1, get_key_value(0, &neighbor->page), sizeof(record));
            rightsibling = get_right_sibling_pagenum(&neighbor->page);
            set_key_value(0, get_key_from_record(record1), get_value_from_record(record1), &n->page);
            tmp = n; n = neighbor; neighbor= tmp;
        }else
        {
            parent_key = *(int64_t*)get_key_pagenum(n_index, &parent->page);
            parent_n_pointer = *(pagenum_t*)(get_key_pagenum(n_index, &parent->page) + 8);
        }
        set_right_sibling_pagenum(rightsibling, &neighbor->page);
        set_number_of_key(1, &neighbor->page);
        buffer_write(neighbor);
    }
    set_key_to_record(parent_key, record1);
    set_value_to_record((char *)&parent_n_pointer, record1);
    
    buffer_free_page(table_id, n);
    new_root = delete_entry(table_id, root, parent, record1);
    free(record1);
    --(parent->pin);
    return new_root;
}

pagenum_t redistribute_nodes(int table_id, buffer *root, buffer *n, buffer *neighbor, int my_index, int neighbor_index) {
    int is_leaf, neighbor_num_keys;
    int64_t parent_key, neighbor_key;
    pagenum_t parentnum;
    record* record1 = (record*)malloc(sizeof(record));
    buffer *parent, *neighbor_pointer;
    
    parentnum = get_parent_pagenum(&n->page);
    is_leaf = get_is_leaf(&n->page);
    
    parent = buffer_read_page(table_id, parentnum);
    if (!is_leaf) {
        neighbor_num_keys = get_number_of_key(&neighbor->page);
        if (my_index < neighbor_index) {
            parent_key = *(int64_t *)get_key_pagenum(0, &parent->page);
            neighbor_key = *(int64_t*)get_key_pagenum(0, &neighbor->page);
            neighbor_pointer = buffer_read_page(table_id, get_one_more_pagenum(&neighbor->page));
            set_key_pagenum(0, parent_key, neighbor_pointer->page_num, &n->page);
            set_key_pagenum(0, neighbor_key, neighbor->page_num, &parent->page);
            set_one_more_pagenum(*(pagenum_t *)(get_key_pagenum(0, &neighbor->page) + 8), &neighbor->page);
            for (int i = 0; i < neighbor_num_keys - 1; ++i)
                set_key_pagenum(i, *(int64_t *)get_key_pagenum(i + 1, &neighbor->page), *(pagenum_t*)(get_key_pagenum( i + 1, &neighbor->page) + 8), &neighbor->page);
        } else {
            parent_key = *(int64_t *)get_key_pagenum(my_index, &parent->page);
            neighbor_key = *(int64_t*)get_key_pagenum(neighbor_num_keys - 1, &neighbor->page);
            neighbor_pointer = buffer_read_page(table_id, *(pagenum_t*)(get_key_pagenum(neighbor_num_keys - 1, &neighbor->page) + 8));
            set_key_pagenum(my_index, neighbor_key, n->page_num, &parent->page);
            set_key_pagenum(0, parent_key, get_one_more_pagenum(&n->page), &n->page);
            set_one_more_pagenum(neighbor_pointer->page_num, &n->page);
        }
        set_parent_pagenum(n->page_num, &neighbor_pointer->page);
        set_number_of_key(1, &n->page);
        set_number_of_key(neighbor_num_keys - 1, &neighbor->page);
        --(neighbor_pointer->pin);
        buffer_write(neighbor_pointer);
    } else {
        neighbor_num_keys = get_number_of_key(&neighbor->page);
        if (my_index < neighbor_index) {
            memcpy(record1, get_key_value(0, &neighbor->page), sizeof(record));
            for (int i = 0; i < neighbor_num_keys - 1; ++i)
                set_key_value(i, *(int64_t *)get_key_value(i + 1, &neighbor->page), (get_key_value(i + 1, &neighbor->page) + 8), &neighbor->page);
            neighbor_key = get_key_from_record((record*)get_key_value(0, &neighbor->page));
            set_key_pagenum(0, neighbor_key, neighbor->page_num, &parent->page);
            set_key_value(0, get_key_from_record(record1), get_value_from_record(record1), &n->page);
        } else {
            memcpy(record1, get_key_value(neighbor_num_keys - 1, &neighbor->page), sizeof(record));
            neighbor_key = get_key_from_record(record1);
            set_key_pagenum(my_index, neighbor_key, *(pagenum_t *)(get_key_pagenum(my_index, &parent->page) + 8), &parent->page);
            set_key_value(0, neighbor_key, get_value_from_record(record1), &n->page);
        }
        set_number_of_key(1, &n->page);
        set_number_of_key( neighbor_num_keys - 1, &neighbor->page);
    }
    buffer_write(parent);
    buffer_write(neighbor);
    buffer_write(n);
    free(record1);
    --(parent->pin);
    return root->page_num;
}

pagenum_t delete_entry( int table_id, buffer *root, buffer *n, record *pointer ) {
    int min_keys, my_index, neighbor_index, num_keys;
    pagenum_t paren, naver, new_root;
    buffer *parent, *neighbor;
    
    remove_entry_from_node(n, pointer);
    
    if (n->page_num == root->page_num) {
        new_root = adjust_root(table_id, root);
        return new_root;
    }

    num_keys = get_number_of_key(&n->page);
    paren = get_parent_pagenum(&n->page);

    min_keys = 1; // delayed merge
    if (num_keys >= min_keys)
        return root->page_num;

    my_index = get_my_index( table_id, n );
    parent = buffer_read_page(table_id, paren);
    naver = my_index == -1 ? *(pagenum_t *)get_value_from_record((record*)get_key_pagenum(0, &parent->page)) :
               *(pagenum_t *)get_value_from_record((record*)get_key_pagenum(my_index - 1, &parent->page));
    neighbor_index = my_index == -1 ? 0 : my_index - 1;

    neighbor = buffer_read_page(table_id, naver);
    int neighbor_num_keys = get_number_of_key(&neighbor->page);
    
    --(parent->pin);
    if (neighbor_num_keys + num_keys <= 1)
        new_root = coalesce_nodes(table_id, root, n, neighbor, my_index, neighbor_index);
    else
        new_root = redistribute_nodes(table_id, root, n, neighbor, my_index, neighbor_index);
    --(neighbor->pin);
    return new_root;
}

pagenum_t _delete(int table_id, buffer *root, int64_t key) {
    pagenum_t new_root = -1, c;
    record *key_record = (record*)malloc(sizeof(record));

	c = find_leaf(table_id, root, key, false);
    int ck = find(table_id, c, key, false, key_record);
    buffer *key_leaf = buffer_read_page(table_id, find_leaf(table_id, root, key, false));
    if (!ck && key_leaf->page_num != 0)
        new_root = delete_entry(table_id, root, key_leaf, key_record);
    free(key_record);
    --(key_leaf->pin);
    return new_root;
}

int join_table(int table_id_1, int table_id_2, char* pathname)
{
    FILE* join_tb;
    join_tb = fopen(pathname, "r+");
    if (join_tb == NULL) {
        join_tb = fopen(pathname, "w");
        fclose(join_tb);
        join_tb = fopen(pathname, "r+");
    }
    int64_t table1_key, table2_key;
    pagenum_t table1_now, table2_now;
    int cnt1, cnt2, i = 0, j = 0, cnt = 0;
    buffer *buffer1, *buffer2;
    buffer1 = buffer_read_page(table_id_1, 0);
    table1_now = get_root_pagenum(&buffer1->page);
    --(buffer1->pin);
    if (table1_now == 0) return -1;
    buffer1 = buffer_read_page(table_id_1, table1_now);
    while (get_is_leaf(&buffer1->page) == 0)
    {
        table1_now = get_one_more_pagenum(&buffer1->page);
        --(buffer1->pin);
        buffer1 = buffer_read_page(table_id_1, table1_now);
    }
    cnt1 = get_number_of_key(&buffer1->page);
    
    buffer2 = buffer_read_page(table_id_2, 0);
    table2_now = get_root_pagenum(&buffer2->page);
    --(buffer2->pin);
    if (table2_now == 0)
    {
        --(buffer1->pin);
        return -1;
    }
    buffer2 = buffer_read_page(table_id_2, table2_now);
    while (get_is_leaf(&buffer2->page) == 0)
    {
        table2_now = get_one_more_pagenum(&buffer2->page);
        --(buffer2->pin);
        buffer2 = buffer_read_page(table_id_2, table2_now);
    }
    cnt2 = get_number_of_key(&buffer2->page);
    
    while (1)
    {
        table1_key = get_key_from_record((record*)(get_key_value(i, &buffer1->page)));
        table2_key = get_key_from_record((record*)(get_key_value(j, &buffer2->page)));
        if (table1_key > table2_key)
            j++;
        else if (table1_key < table2_key)
            i++;
        else
        {
            fprintf(join_tb,"%lld,%s,%lld,%s\n",
                    table1_key,get_value_from_record((record*)(get_key_value(i, &buffer1->page))),
                    table2_key,get_value_from_record((record*)(get_key_value(j, &buffer2->page))));
            i++;
            j++;
        }
        
        if (i == cnt1)
        {
            table1_now = get_right_sibling_pagenum(&buffer1->page);
            if (table1_now == 0) break;
            --(buffer1->pin);
            buffer1 = buffer_read_page(table_id_1, table1_now);
            cnt1 = get_number_of_key(&buffer1->page);
            i = 0;
        }
        if (j == cnt2)
        {
            table2_now = get_right_sibling_pagenum(&buffer2->page);
            if (table2_now == 0) break;
            --(buffer2->pin);
            buffer2 = buffer_read_page(table_id_2, table2_now);
            cnt2 = get_number_of_key(&buffer2->page);
            j = 0;
        }
    }
    --(buffer1->pin);
    --(buffer2->pin);
    fclose(join_tb);
    return 0;
}