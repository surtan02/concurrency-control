
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  	this->ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
	// mencari pada lock table untuk item dengan key
	unordered_map<Key, deque<LockRequest>*>::iterator lock_deque = lock_table_.find(key);

	// jika tidak ada, membuat deque baru yang menampung lock request thdp item sebagai 
	// value yang akan dimasukkan pada lock table dengan key berupa key item
	if (lock_deque == lock_table_.end()) {
		deque<LockRequest> *new_deque = new deque<LockRequest>();
		// menambahkan lock request dari transaksi pada deque baru
		LockRequest *txn_req = new LockRequest(EXCLUSIVE, txn);
		new_deque->push_back(*txn_req);
		lock_table_.insert(std::pair<Key, deque<LockRequest>*>(key, new_deque));
		// memasukkan transaksi pada txn_waits dengan lock count 0
		txn_waits_.insert(std::pair<Txn*, int>(txn, 0));
		return true;
	} else {
		// jika ada, maka menambahkan lock request transaksi pada deque yg sudah ada
		deque<LockRequest> *request_deque = lock_deque->second;
		request_deque->push_back(LockRequest(EXCLUSIVE, txn));
		// jika deque kosong, memasukkan transaksi pada txn_waits dengan lock count 0
		if (request_deque->size() == 1) {
			txn_waits_.insert(std::pair<Txn*, int>(txn, 0));
			return true;
		} else {
			// jika deque tidak kosong, maka transaksi ditambahkan pada txn_waits dengan
			// lock count sebesar 1 apabila transaksi belum ada di txn_waits. apabila transaksi 
			// sudah ada di txn_waits, maka current lock count transaksi tersebut ditambahkan 1
			unordered_map<Txn*, int>::iterator txn_in_waits = txn_waits_.find(txn);
			if (txn_in_waits == txn_waits_.end()) {
				std::pair<Txn*, int> *txn_wait = new std::pair<Txn*, int>(txn, 1);
				txn_waits_.insert(*txn_wait);
			} else { 
				txn_in_waits->second += 1;
			}
			return false;
		}
	}
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
	// simple locking hanya menerapkan exclusive locks baik untuk 
	// read maupun write sehingga readlock memanggil writelock saja
  	return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
	// mencari pada lock table untuk item dengan key
	unordered_map<Key, deque<LockRequest>*>::iterator lock_deque = lock_table_.find(key);

	// jika sudah ada sebelumnya, maka deque akan diiterasi hingga ditemukan
	// lock request dari transaksi yang ingin direlease
	if (lock_deque != lock_table_.end()) {
		deque<LockRequest>::iterator l = lock_deque->second->begin();
		while (l != lock_deque->second->end()) {
			if (l->txn_ == txn) {
				// jika transaksi ditemukan dalam kondisi sedang tidak memegang lock dan masih ada di
				// txn_waits, maka transaksi akan dihapus dari txn_waits (jadi zombie request)
				if (l != lock_deque->second->begin()) { 
					unordered_map<Txn*, int>::iterator zombie = txn_waits_.find(txn);
					if (zombie != txn_waits_.end()) {
						txn_waits_.erase(txn);
					}
					lock_deque->second->erase(l);
					break;
				}
				// mengambil lock request berikutnya setelah yang dihapus dan dicek apakah ada/tidak
				// jika ada, maka mengurangi lock countnya pada txn_waits
				deque<LockRequest>::iterator next = lock_deque->second->erase(l);
				while (next != lock_deque->second->end()) {
					unordered_map<Txn*, int>::iterator zombie = txn_waits_.find(next->txn_);
					// jika transaksi tidak ada di txn_waits (zombie req), hapus dari deque
					// jika lock count lebih dari 0, kurangi lock count sebesar 1
					if (zombie == txn_waits_.end()) {
						next = lock_deque->second->erase(next);
						continue;
					} else if (zombie->second > 0) {
						zombie->second -= 1;
						// jika lock count 0 setelah didecrement, berikan lock
						if (zombie->second == 0) {
							this->ready_txns_->push_back(next->txn_);
						}
					}
					// break dari loop ketika next lock request berupa exclusive lock
					// pada kasus simple locking ini akan selalu true
					if (next->mode_ == EXCLUSIVE) {
						break;
					}
					next += 1;
				}
				break;
			}
			l += 1;
		}
	}
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
	owners->clear();
	// mencari pada lock table untuk item dengan key. akan mengembalikan status UNLOCKED
	// apabila tidak ditemukan atau jika transaksi sedang memegang lock terhadap item.
	// jika tidak, maka memasukkan transaksi pada owners
	unordered_map<Key, deque<LockRequest>*>::iterator lock_deque = lock_table_.find(key);
	if (lock_deque == lock_table_.end()) {
		return UNLOCKED;
	} else { 
		deque<LockRequest>::iterator l = lock_deque->second->begin();  // holds txn
		if (l == lock_deque->second->end())
			return UNLOCKED;
		do {
			owners->push_back(l->txn_);
			l += 1;
		} while (l != lock_deque->second->end() && l->mode_ != EXCLUSIVE);
	}
	return EXCLUSIVE;
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  	ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
	//
	// Implement this method!
	return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
	//
	// Implement this method!
	return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
	//
	// Implement this method!
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
	//
	// Implement this method!
	return UNLOCKED;
}

