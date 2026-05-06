#include "onebase/execution/executors/limit_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                              std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  // TODO(student): Initialize child executor and reset count
  child_executor_->Init();
  count_ = 0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Return next tuple if count < limit, else false
  if (count_ >= plan_->GetLimit()) {
    return false;
  }
  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }
  count_++;
  return true;
}

}  // namespace onebase
