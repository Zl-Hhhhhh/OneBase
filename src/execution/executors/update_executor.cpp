#include "onebase/execution/executors/update_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  // TODO(student): Initialize child executor
  child_executor_->Init();
  has_updated_ = false;
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Update tuples using update expressions
  // - Get tuples from child, evaluate update expressions, update table_heap
  // - Return count of updated rows
  if (has_updated_) {
    return false;
  }
  has_updated_ = true;

  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  if (table_info == nullptr) {
    return false;
  }

  int updated = 0;
  const auto &child_schema = child_executor_->GetOutputSchema();
  Tuple old_tuple;
  RID old_rid;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    std::vector<Value> new_values;
    new_values.reserve(plan_->GetUpdateExpressions().size());
    for (const auto &expr : plan_->GetUpdateExpressions()) {
      new_values.push_back(expr->Evaluate(&old_tuple, &child_schema));
    }

    Tuple new_tuple(std::move(new_values));
    if (table_info->table_->UpdateTuple(old_rid, new_tuple)) {
      updated++;
    }
  }

  *tuple = Tuple({Value(TypeId::INTEGER, updated)});
  *rid = RID();
  return true;
}

}  // namespace onebase
