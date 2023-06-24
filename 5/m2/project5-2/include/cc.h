#include "buffer_manager.h"

struct hash_pair {
	template <class T1, class T2>
	std::size_t operator()(const std::pair<T1, T2>& p) const
	{
		auto hash1 = std::hash<T1>{}(p.first);
		auto hash2 = std::hash<T2>{}(p.second);
		return hash1 ^ hash2;
	}
};

enum lock_mode{
    SHARED, EXCLUSIVE
};

enum trx_state{
    IDLE, RUNNING, WAITING
};

typedef struct undo{
	int table_id, index;
	pagenum_t pagenum;
	int64_t key;
	char* value;
};

typedef struct lock_t {
	int table_id;
	int64_t key;
	pagenum_t pagenum;
	bool acquired;
    enum lock_mode mode;
    struct trx_t *trx;
	struct lock_t *next, *prev;
};

typedef struct lock_manager {
	std::unordered_map<std::pair<int, pagenum_t>, std::pair<lock_t*, lock_t*>, hash_pair> Lock_Hash_Table;
	std::mutex lock_sys_mutex;
};

typedef struct trx_t{
    int trx_id;
    enum trx_state state;
    std::list<lock_t*> trx_locks;
	std::mutex trx_mutex;
	std::condition_variable trx_cond;
    lock_t *wait_lock;
	std::list<undo*> undo_list;
};

typedef struct trx_manager {
	std::unordered_map<int, trx_t*> Transaction_Table;
	std::mutex trx_sys_mutex;
};

typedef struct b_pool_latch {
	std::mutex buffer_pool;
	std::condition_variable cond;
};

typedef struct b_page_latch {
	int64_t key;
	bool acquire;
};

typedef struct b_page_latch_list {
	std::unordered_map<std::pair<int, pagenum_t>, std::list<b_page_latch*>, hash_pair> page_latch_list;
};

extern lock_manager lock_list;
extern trx_manager trx_list;
extern b_pool_latch bpool;
extern b_page_latch_list bpage;


int begin_trx();
int end_trx(int tid);
int abort_trx(int tid);
int acquire_record_lock(int trx_id, int S, int table_id, pagenum_t pagenum, int64_t key, int &tar_tid);