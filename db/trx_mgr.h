#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <map>
#include <string>
#include <sys/types.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

typedef uint64_t TransactionID;

enum OperType {
  READ = 0,
  WRITE = 1,
  COMMIT = 2,
};

struct RWOperation {
  RWOperation(OperType type, TransactionID id) : type_(type), id_(id) {}
  OperType type_;
  TransactionID id_;
};

class TrxMgr {
public:
  TransactionID AcqTransactionID();
  // 写事务加锁
  int32_t AcqWriteLock(const std::string &key, TransactionID trx_id);
  // 读事务加标记锁
  void AcqReadLock(const std::string &key, TransactionID trx_id);
  //

private:
  // 检查事务之间的写写冲突
  int32_t HasWWConflict(const std::string &key) const;
  // 检查事务之间有没有进出两个读写冲突
  int32_t HasVulner(TransactionID trx_id, OperType op, const std::string &key);

  // 事务写入的标记锁
  std::unordered_map<std::string, TransactionID> write_lock_;
  // 事务读的标记锁
  std::unordered_map<std::string, std::unordered_set<TransactionID>> read_lock_;
  // 全局事务id
  std::atomic_uint64_t global_trx_id_ = 0;

  // 全局事务rw-conflict冲突表
  static constexpr int INSIDE = 0;  // 入边
  static constexpr int OUTSIDE = 1; // 出边
  std::unordered_map<TransactionID, std::array<bool, 2>> rw_conflict_graph_;

  // 记录全局事务到开始终止lsn
  std::unordered_map<TransactionID, std::pair<uint64_t, uint64_t>> trx_2_lsn_;
  // 记录全局事务,按照start_lsn排序
  std::map<uint64_t, TransactionID> start_lsn_2_trx_;
};

class TrxLogic {
public:
  TrxLogic(TrxMgr &trx_mgr)
      : trx_mgr_(trx_mgr), trx_id_(trx_mgr_.AcqTransactionID()){};

  TrxLogic(const TrxLogic &) = delete;
  TrxLogic &operator=(const TrxLogic &) = delete;

private:
  TrxMgr &trx_mgr_;
  TransactionID trx_id_;
};