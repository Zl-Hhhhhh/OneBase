#include "onebase/execution/executors/nested_loop_join_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx,
                                                const NestedLoopJoinPlanNode *plan,
                                                std::unique_ptr<AbstractExecutor> left_executor,
                                                std::unique_ptr<AbstractExecutor> right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  // TODO(student): Initialize both child executors
  result_tuples_.clear();
  cursor_ = 0;

  left_executor_->Init();
  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();

  auto make_join_tuple = [&](const Tuple &left_tuple, const Tuple &right_tuple) {
    std::vector<Value> values;
    values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());
    for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
      values.push_back(left_tuple.GetValue(&left_schema, i));
    }
    for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
      values.push_back(right_tuple.GetValue(&right_schema, i));
    }
    return Tuple(std::move(values));
  };

  Tuple left_tuple;
  RID left_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    right_executor_->Init();

    Tuple right_tuple;
    RID right_rid;
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      bool match = true;
      if (plan_->GetPredicate() != nullptr) {
        match = plan_->GetPredicate()
                    ->EvaluateJoin(&left_tuple, &left_schema, &right_tuple, &right_schema)
                    .GetAsBoolean();
      }
      if (match) {
        result_tuples_.push_back(make_join_tuple(left_tuple, right_tuple));
      }
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Perform nested loop join
  // - For each left tuple, scan all right tuples
  // - Evaluate predicate on (left, right) pairs
  // - Output matching combined tuples
  if (cursor_ >= result_tuples_.size()) {
    return false;
  }
  *tuple = result_tuples_[cursor_++];
  *rid = RID();
  return true;
}

}  // namespace onebase
