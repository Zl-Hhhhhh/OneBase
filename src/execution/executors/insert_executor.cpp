#include "onebase/execution/executors/insert_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // TODO(student): Initialize child executor
  child_executor_->Init();
  has_inserted_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Insert tuples from child into the table
  // - Get tuples from child, insert into table_heap
  // - Update any indexes
  // - Return count of inserted rows as a single integer tuple
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;

  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  if (table_info == nullptr) {
    return false;
  }

  int inserted = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto inserted_rid = table_info->table_->InsertTuple(child_tuple);
    if (inserted_rid.has_value()) {
      inserted++;
    }
  }

  *tuple = Tuple({Value(TypeId::INTEGER, inserted)});
  *rid = RID();
  return true;
}

}  // namespace onebase
