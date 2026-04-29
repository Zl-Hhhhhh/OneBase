#include "onebase/execution/executors/hash_join_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                    std::unique_ptr<AbstractExecutor> left_executor,
                                    std::unique_ptr<AbstractExecutor> right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {}

void HashJoinExecutor::Init() {
  // TODO(student): Build hash table from left child, initialize right child
  hash_table_.clear();
  result_tuples_.clear();
  cursor_ = 0;

  left_executor_->Init();
  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();

  Tuple left_tuple;
  RID left_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    auto left_key = plan_->GetLeftKeyExpression()->Evaluate(&left_tuple, &left_schema);
    hash_table_[left_key.ToString()].push_back(left_tuple);
  }

  auto make_join_tuple = [&](const Tuple &l_tuple, const Tuple &r_tuple) {
    std::vector<Value> values;
    values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());
    for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
      values.push_back(l_tuple.GetValue(&left_schema, i));
    }
    for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
      values.push_back(r_tuple.GetValue(&right_schema, i));
    }
    return Tuple(std::move(values));
  };

  right_executor_->Init();
  Tuple right_tuple;
  RID right_rid;
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    auto right_key = plan_->GetRightKeyExpression()->Evaluate(&right_tuple, &right_schema);
    auto it = hash_table_.find(right_key.ToString());
    if (it == hash_table_.end()) {
      continue;
    }

    for (const auto &matched_left : it->second) {
      result_tuples_.push_back(make_join_tuple(matched_left, right_tuple));
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Probe hash table with right child tuples
  // - Phase 1 (in Init): Build hash table from left child on left_key_expr
  // - Phase 2 (in Next): For each right tuple, probe hash table using right_key_expr
  if (cursor_ >= result_tuples_.size()) {
    return false;
  }
  *tuple = result_tuples_[cursor_++];
  *rid = RID();
  return true;
}

}  // namespace onebase
