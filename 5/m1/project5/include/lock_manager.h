#include "buffer_manager.h"

enum lock_mode{
    SHARED, EXCLUSIVE
};

enum trx_state{
    IDLE, RUNNING, WAITING
};

typedef struct lock_t {
	int64_t key;
	pagenum_t pagenum;
    int table_id;
    enum lock_mode mode;
    struct trx_t *trx;
	struct lock_t *next, *prev;
};

typedef struct trx_t{
    int trx_id;
    enum trx_state state;
    std::list<lock_t*> trx_locks;
    lock_t *wait_lock;
};

extern std::mutex mtex;
extern std::vector<trx_t*> Transaction_Table;
extern std::unordered_map<pagenum_t, std::pair<lock_t*, lock_t*> > Lock_Hash_Table;

int begin_trx();
int end_trx(int tid);