#include "onebase/execution/executors/delete_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // TODO(student): Initialize child executor
  child_executor_->Init();
  has_deleted_ = false;
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Delete tuples identified by child executor
  // - Get tuples from child, delete from table_heap
  // - Update any indexes
  // - Return count of deleted rows
  if (has_deleted_) {
    return false;
  }
  has_deleted_ = true;

  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  if (table_info == nullptr) {
    return false;
  }

  int deleted = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    table_info->table_->DeleteTuple(child_rid);
    deleted++;
  }

  *tuple = Tuple({Value(TypeId::INTEGER, deleted)});
  *rid = RID();
  return true;
}

}  // namespace onebase
