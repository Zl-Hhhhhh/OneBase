#include "onebase/execution/executors/index_scan_executor.h"
#include "onebase/common/exception.h"

#include <algorithm>

namespace onebase {

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // TODO(student): Initialize index scan using the B+ tree index
  matching_rids_.clear();
  cursor_ = 0;

  auto *catalog = GetExecutorContext()->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
  if (table_info_ == nullptr || index_info_ == nullptr) {
    return;
  }
  if (!index_info_->SupportsPointLookup()) {
    return;
  }

  auto key_val = plan_->GetLookupKey()->Evaluate(nullptr, nullptr);
  if (key_val.IsNull() || key_val.GetTypeId() != TypeId::INTEGER) {
    return;
  }

  auto *rids = index_info_->LookupInteger(key_val.GetAsInteger());
  if (rids != nullptr) {
    matching_rids_ = *rids;
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Return next tuple from index scan
  if (table_info_ == nullptr || index_info_ == nullptr) {
    return false;
  }

  const auto &predicate = plan_->GetPredicate();
  while (cursor_ < matching_rids_.size()) {
    auto current_rid = matching_rids_[cursor_++];
    auto current = table_info_->table_->GetTuple(current_rid);

    if (predicate != nullptr) {
      auto pred_val = predicate->Evaluate(&current, &table_info_->schema_);
      if (!pred_val.GetAsBoolean()) {
        continue;
      }
    }

    const auto &output_schema = plan_->GetOutputSchema();
    std::vector<Value> values;
    values.reserve(output_schema.GetColumnCount());
    for (uint32_t i = 0; i < output_schema.GetColumnCount(); ++i) {
      values.push_back(current.GetValue(&table_info_->schema_, i));
    }
    *tuple = Tuple(std::move(values));
    tuple->SetRID(current_rid);
    *rid = current_rid;
    return true;
  }

  return false;
}

}  // namespace onebase
