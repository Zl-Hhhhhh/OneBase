#include "onebase/execution/executors/index_scan_executor.h"
#include "onebase/common/exception.h"

#include <algorithm>

namespace onebase {

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // TODO(student): Initialize index scan using the B+ tree index
  result_tuples_.clear();
  cursor_ = 0;

  auto *catalog = GetExecutorContext()->GetCatalog();
  auto *table_info = catalog->GetTable(plan_->GetTableOid());
  if (table_info == nullptr) {
    return;
  }

  for (auto iter = table_info->table_->Begin(); iter != table_info->table_->End(); ++iter) {
    result_tuples_.push_back(*iter);
  }

  std::vector<uint32_t> key_attrs;
  for (auto *index_info : catalog->GetTableIndexes(table_info->name_)) {
    if (index_info->oid_ == plan_->GetIndexOid()) {
      key_attrs = index_info->key_attrs_;
      break;
    }
  }

  if (!key_attrs.empty()) {
    const auto &schema = table_info->schema_;
    std::sort(result_tuples_.begin(), result_tuples_.end(),
              [&](const Tuple &a, const Tuple &b) {
                for (auto col_idx : key_attrs) {
                  auto a_val = a.GetValue(&schema, col_idx);
                  auto b_val = b.GetValue(&schema, col_idx);
                  if (a_val.CompareEquals(b_val).GetAsBoolean()) {
                    continue;
                  }
                  return a_val.CompareLessThan(b_val).GetAsBoolean();
                }
                return false;
              });
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Return next tuple from index scan
  if (cursor_ >= result_tuples_.size()) {
    return false;
  }
  *tuple = result_tuples_[cursor_++];
  *rid = tuple->GetRID();
  return true;
}

}  // namespace onebase
