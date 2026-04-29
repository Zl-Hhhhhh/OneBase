#include "onebase/concurrency/lock_manager.h"
#include "onebase/common/exception.h"

namespace onebase {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  // TODO(student): Implement shared lock acquisition using 2PL
  // - Check transaction state (must be GROWING)
  // - Add lock request to queue
  // - Wait until granted (no exclusive locks held by others)
  // - Add to txn's shared lock set
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> lock(latch_);
  auto &queue = lock_table_[rid];
  queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);

  queue.cv_.wait(lock, [&]() {
    for (const auto &req : queue.request_queue_) {
      if (req.granted_ && req.lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
    }
    return true;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
      if (it->txn_id_ == txn->GetTransactionId()) {
        queue.request_queue_.erase(it);
        break;
      }
    }
    queue.cv_.notify_all();
    return false;
  }

  for (auto &req : queue.request_queue_) {
    if (req.txn_id_ == txn->GetTransactionId()) {
      req.granted_ = true;
      break;
    }
  }
  txn->GetSharedLockSet()->insert(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  // TODO(student): Implement exclusive lock acquisition using 2PL
  // - Check transaction state (must be GROWING)
  // - Add lock request to queue
  // - Wait until granted (no other locks held)
  // - Add to txn's exclusive lock set
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (txn->IsSharedLocked(rid)) {
    return LockUpgrade(txn, rid);
  }

  std::unique_lock<std::mutex> lock(latch_);
  auto &queue = lock_table_[rid];
  queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  queue.cv_.wait(lock, [&]() {
    for (const auto &req : queue.request_queue_) {
      if (req.granted_ && req.txn_id_ != txn->GetTransactionId()) {
        return false;
      }
    }
    return true;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
      if (it->txn_id_ == txn->GetTransactionId()) {
        queue.request_queue_.erase(it);
        break;
      }
    }
    queue.cv_.notify_all();
    return false;
  }

  for (auto &req : queue.request_queue_) {
    if (req.txn_id_ == txn->GetTransactionId()) {
      req.granted_ = true;
      break;
    }
  }
  txn->GetExclusiveLockSet()->insert(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  // TODO(student): Upgrade shared lock to exclusive
  // - Only one upgrade at a time per queue
  // - Wait until all other shared locks released
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  std::unique_lock<std::mutex> lock(latch_);
  auto &queue = lock_table_[rid];
  if (queue.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  queue.upgrading_ = true;

  bool found = false;
  for (auto &req : queue.request_queue_) {
    if (req.txn_id_ == txn->GetTransactionId()) {
      req.lock_mode_ = LockMode::EXCLUSIVE;
      req.granted_ = true;
      found = true;
      break;
    }
  }
  if (!found) {
    queue.upgrading_ = false;
    return false;
  }

  queue.cv_.wait(lock, [&]() {
    for (const auto &req : queue.request_queue_) {
      if (req.granted_ && req.txn_id_ != txn->GetTransactionId()) {
        return false;
      }
    }
    return true;
  });

  queue.upgrading_ = false;

  if (txn->GetState() == TransactionState::ABORTED) {
    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
      if (it->txn_id_ == txn->GetTransactionId()) {
        queue.request_queue_.erase(it);
        break;
      }
    }
    queue.cv_.notify_all();
    return false;
  }

  for (auto &req : queue.request_queue_) {
    if (req.txn_id_ == txn->GetTransactionId()) {
      req.granted_ = true;
      break;
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->insert(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  // TODO(student): Release a lock
  // - Transition txn to SHRINKING state (2PL)
  // - Remove from request queue
  // - Notify waiting transactions
  std::unique_lock<std::mutex> lock(latch_);
  auto table_it = lock_table_.find(rid);
  if (table_it == lock_table_.end()) {
    return false;
  }

  auto &queue = table_it->second;
  for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      queue.request_queue_.erase(it);
      break;
    }
  }

  if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  queue.cv_.notify_all();
  return true;
}

}  // namespace onebase
