#include "lock_manager.h"

std::mutex mtex;
std::vector<trx_t*> Transaction_Table;
std::unordered_map<pagenum_t, std::pair<lock_t*, lock_t*> > Lock_Hash_Table;
int max_cnt = 0;
std::queue<int> q;

int begin_trx() {
	mtex.lock();
	trx_t* tmp = (trx_t*)malloc(sizeof(trx_t));
	int id;
	if (q.empty())
		id = ++max_cnt;
	else {
		id = q.front();
		q.pop();
	}
	tmp->trx_id = id;
	tmp->state = IDLE;
	tmp->wait_lock = nullptr;
	Transaction_Table.push_back(tmp);
	mtex.unlock();
	return id;
}

int end_trx(int tid) {
	mtex.lock();
	std::vector<trx_t*>::iterator iter = Transaction_Table.begin();
	for (; iter != Transaction_Table.end(); iter++) {
		if ((*iter)->trx_id == tid) {
			(*iter)->trx_locks.clear();
			free(*iter);
			Transaction_Table.erase(iter);
			break;
		}
	}
	if (iter == Transaction_Table.end())
	{
		mtex.unlock();
		return 0;
	}
	q.push(tid);
	mtex.unlock();
	return tid;
}