#include "db/trx_mgr.h"

TransactionID TrxMgr::AcqTransactionID() {
  return global_trx_id_.fetch_add(1) + 1;
}

void TrxMgr::AcqReadLock(const std::string &key, TransactionID trx_id) {
  read_lock_[key].insert(trx_id);
}

int32_t TrxMgr::HasWWConflict(const std::string &key) const {
  // 正在并发的事务中有没有冲突
  auto iter = write_lock_.find(key);
  if (iter != write_lock_.end())
    return iter->second;
  // 已经提交但是有过并发的事务中有没有冲突

  return 0;
}

int32_t TrxMgr::AcqWriteLock(const std::string &key, TransactionID trx_id) {
  int32_t ret = HasWWConflict(key);
  if (ret)
    return ret;
  write_lock_[key] = trx_id;
  return 0;
}

int32_t TrxMgr::HasVulner(TransactionID trx_id, OperType op,
                          const std::string &key) {
  if (op == READ) {
    // 在加锁的里面找(加锁的必然TransactionID更靠后)
    auto iter = write_lock_.find(key);
    if (iter != write_lock_.end()) {
      rw_conflict_graph_[iter->second][INSIDE] = true;
      rw_conflict_graph_[trx_id][OUTSIDE] = true;
    }
    // 在已提交的里面找TransactionID更靠后的

  } else if (op == WRITE) {
    auto iter = read_lock_.find(key);
    if (iter == read_lock_.end() || iter->second.empty())
      return 1;
    for (auto &read_trx_id : iter->second) {
      uint64_t read_trx_commit_lsn = trx_2_lsn_[read_trx_id].second;
      uint64_t this_trx_start_lsn = trx_2_lsn_[trx_id].first;
      if (read_trx_commit_lsn != 0 &&
          read_trx_commit_lsn >= this_trx_start_lsn) {
        continue;
      }
      if (rw_conflict_graph_[read_trx_id][INSIDE])
        return 0;
      rw_conflict_graph_[read_trx_id][OUTSIDE] = true;
      rw_conflict_graph_[trx_id][INSIDE] = true;
    }
  } else {
    if (rw_conflict_graph_[trx_id][INSIDE] &&
        rw_conflict_graph_[trx_id][OUTSIDE])
      return 0;
  }
  return 1;
}