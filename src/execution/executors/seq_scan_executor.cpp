#include "onebase/execution/executors/seq_scan_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // TODO(student): Initialize the sequential scan
  // - Get the table from catalog using plan_->GetTableOid()
  // - Set up iterator to table_heap->Begin()
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  if (table_info_ == nullptr) {
    return;
  }
  iter_ = table_info_->table_->Begin();
  end_ = table_info_->table_->End();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Return the next tuple from the table
  // - Advance iterator, skip tuples that don't match predicate
  // - Return false when no more tuples
  if (table_info_ == nullptr) {
    return false;
  }

  while (iter_ != end_) {
    auto current = *iter_;
    auto current_rid = iter_.GetRID();
    ++iter_;

    const auto &predicate = plan_->GetPredicate();
    if (predicate != nullptr) {
      auto pred_val = predicate->Evaluate(&current, &table_info_->schema_);
      if (!pred_val.GetAsBoolean()) {
        continue;
      }
    }

    *tuple = current;
    *rid = current_rid;
    return true;
  }

  return false;
}

}  // namespace onebase
