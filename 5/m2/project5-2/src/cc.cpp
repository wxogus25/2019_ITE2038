#include "cc.h"

//namespace std {
//	template <>
//	struct hash <pair<int, pagenum_t> > {
//		size_t operator()(const pair<int, pagenum_t> &t) const {
//			hash<int> hash1;
//			hash<uint64_t> hash2;
//
//			return hash1(t.first) ^ hash2(t.second);
//		}
//	};
//}

int max_cnt = 0;
lock_manager lock_list;
trx_manager trx_list;
b_pool_latch bpool;
b_page_latch_list bpage;

int begin_trx() {
	trx_list.trx_sys_mutex.lock();
	trx_t* tmp = (trx_t*)malloc(sizeof(trx_t));
	int id = ++max_cnt;
	tmp->trx_id = id;
	tmp->state = IDLE;
	tmp->wait_lock = nullptr;
	trx_list.Transaction_Table[id] = tmp;
	trx_list.trx_sys_mutex.unlock();
	return id;
}

int end_trx(int tid) {
	lock_list.lock_sys_mutex.lock();
	trx_t* temp = trx_list.Transaction_Table[tid];
	for (lock_t* x : temp->trx_locks)
		x->acquired = 0;
	temp->trx_cond.notify_all();
	lock_list.lock_sys_mutex.unlock();
	trx_list.trx_sys_mutex.lock();
	for (lock_t* x : temp->trx_locks) {
		auto tmp = lock_list.Lock_Hash_Table[{x->table_id, x->pagenum}];
		if (x == tmp.first)
			tmp.first = x->next;
		if (x == tmp.second)
			tmp.second = x->prev;
		lock_list.Lock_Hash_Table[{x->table_id, x->pagenum}] = tmp;
		if (x->prev != NULL)
			x->prev->next = x->next;
		if (x->next != NULL)
			x->next->prev = x->prev;
		free(x);
	}
	temp->trx_locks.clear();
	free(temp);
	trx_list.trx_sys_mutex.unlock();
	return tid;
}

int abort_trx(int tid) {
	trx_list.trx_sys_mutex.lock();
	if (trx_list.Transaction_Table.find(tid) == trx_list.Transaction_Table.end()) {
		trx_list.trx_sys_mutex.unlock();
		return 0;
	}
	trx_t *trx = trx_list.Transaction_Table[tid];
	trx->trx_mutex.lock();
	for (auto un : trx->undo_list) {
		while (true) {
			bpool.buffer_pool.lock();
			bool ex = false, ac = false;
			for (auto x : bpage.page_latch_list[{un->table_id, un->pagenum}]) {
				if (x->key == un->key) {
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
				temp->key = un->key;
				bpage.page_latch_list[{un->table_id, un->pagenum}].push_back(temp);
			}
			else {
				free(temp);
				if (ac) {
					bpool.buffer_pool.unlock();
					continue;
				}
			}
			buffer *buffer1 = buffer_read_page(un->table_id, un->pagenum);
			set_key_value(un->index, un->key, un->value, &buffer1->page);
			--(buffer1->pin);
			free(un->value);
			free(un);
			bpool.buffer_pool.unlock();
			break;
		}
	}
	trx->undo_list.clear();
	trx->trx_mutex.unlock();
	trx_list.trx_sys_mutex.unlock();
	end_trx(tid);
	return 0;
}

// 0 : SUCCESS, 1 : CONFLICT, 2 : DEADLOCK
int acquire_record_lock(int tid, int S, int table_id, pagenum_t pagenum, int64_t key, int &tar_tid) {
	lock_list.lock_sys_mutex.lock();
	bool conflict = false, deadlock = false;
	lock_t *pnow = NULL, *pwait = NULL;
	std::pair<lock_t*, lock_t*> newht;
	if (lock_list.Lock_Hash_Table.find({ table_id, pagenum }) != lock_list.Lock_Hash_Table.end()) {
		std::pair<lock_t*, lock_t*> ht = lock_list.Lock_Hash_Table[{table_id, pagenum}];
		lock_t *now = ht.first, *prev = NULL;
		bool ex = false;
		while (now != NULL) {
			if (now->trx->trx_id == tid && now->table_id == table_id && now->pagenum == pagenum && now->key == key) {
				ex = true;
				while (now != NULL && now->trx->trx_id == tid && now->table_id == table_id && now->pagenum == pagenum && now->key == key) {
					prev = now;
					now = now->next;
				}
				break;
			}
			prev = now;
			now = now->next;
		}
		if (ex) {
			lock_t *wait = NULL;
			now = prev;
			pnow = now;
			if (S == 0)
				wait = now;
			else {
				while (now != NULL && now->trx->trx_id == tid && now->table_id == table_id && now->pagenum == pagenum && now->key == key) {
					if (now->mode == EXCLUSIVE) {
						wait = now;
						break;
					}
					now = now->prev;
				}
			}
			pwait = wait;
			if (wait != NULL) {
				conflict = true;
				tar_tid = wait->trx->trx_id;
				if (wait->trx->trx_id == tid)
					deadlock = true;
				while (wait != NULL && wait->trx->wait_lock != NULL) {
					wait = wait->trx->wait_lock;
					if (wait->trx->trx_id == tid) {
						deadlock = true;
						break;
					}
				}
			}
			if (deadlock) {
				lock_list.lock_sys_mutex.unlock();
				return 2;
			}
		}
	}
	else {
		newht.first = NULL;
		newht.second = NULL;
	}
	lock_t* temp = (lock_t*)malloc(sizeof(lock_t));
	temp->acquired = 1;
	temp->key = key;
	temp->mode = S ? SHARED : EXCLUSIVE;
	temp->pagenum = pagenum;
	temp->table_id = tid;
	temp->trx = trx_list.Transaction_Table[tid];
	temp->trx->wait_lock = pwait;
	
	if (newht.second == NULL) {
		newht.first = newht.second = temp;
		temp->next = temp->prev = NULL;
	}
	else {
		if (pnow == NULL) {
			newht.second->next = temp;
			temp->prev = newht.second;
			temp->next = NULL;
			newht.second = temp;
		}
		else {
			temp->prev = pnow;
			temp->next = pnow->next;
			if (pnow->next != NULL) {
				pnow->next->prev = temp;
				pnow->next = temp;
			}
			else
				newht.second = temp;
		}
	}

	lock_list.lock_sys_mutex.unlock();
	if (conflict)
		return 1;
	return 0;
}