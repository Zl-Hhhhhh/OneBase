#include "onebase/execution/executors/aggregation_executor.h"
#include "onebase/common/exception.h"

#include <string>
#include <unordered_map>

namespace onebase {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                          std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void AggregationExecutor::Init() {
  // TODO(student): Initialize child and build aggregation hash table
  // - Scan all tuples from child
  // - Group by group_by expressions
  // - Compute aggregates (COUNT, SUM, MIN, MAX) per group
  result_tuples_.clear();
  cursor_ = 0;
  child_executor_->Init();

  struct AggState {
    std::vector<Value> group_values;
    std::vector<Value> aggregate_values;
  };

  std::unordered_map<std::string, AggState> groups;
  const auto &group_bys = plan_->GetGroupBys();
  const auto &aggregates = plan_->GetAggregates();
  const auto &agg_types = plan_->GetAggregateTypes();
  const auto &child_schema = child_executor_->GetOutputSchema();

  auto make_group_key = [](const std::vector<Value> &group_values) {
    std::string key;
    for (const auto &v : group_values) {
      key.append(v.ToString());
      key.push_back('\x1f');
    }
    return key;
  };

  auto init_aggregate_values = [&]() {
    std::vector<Value> init_values;
    init_values.reserve(agg_types.size());
    for (size_t i = 0; i < agg_types.size(); i++) {
      switch (agg_types[i]) {
        case AggregationType::CountStarAggregate:
        case AggregationType::CountAggregate:
          init_values.emplace_back(TypeId::INTEGER, 0);
          break;
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          init_values.emplace_back(aggregates[i]->GetReturnType());
          break;
      }
    }
    return init_values;
  };

  auto update_aggregates = [&](AggState &state, const Tuple &child_tuple) {
    for (size_t i = 0; i < agg_types.size(); i++) {
      switch (agg_types[i]) {
        case AggregationType::CountStarAggregate: {
          const auto cur = state.aggregate_values[i].GetAsInteger();
          state.aggregate_values[i] = Value(TypeId::INTEGER, cur + 1);
          break;
        }
        case AggregationType::CountAggregate: {
          auto input = aggregates[i]->Evaluate(&child_tuple, &child_schema);
          if (!input.IsNull()) {
            const auto cur = state.aggregate_values[i].GetAsInteger();
            state.aggregate_values[i] = Value(TypeId::INTEGER, cur + 1);
          }
          break;
        }
        case AggregationType::SumAggregate: {
          auto input = aggregates[i]->Evaluate(&child_tuple, &child_schema);
          if (!input.IsNull()) {
            if (state.aggregate_values[i].IsNull()) {
              state.aggregate_values[i] = input;
            } else {
              state.aggregate_values[i] = state.aggregate_values[i].Add(input);
            }
          }
          break;
        }
        case AggregationType::MinAggregate: {
          auto input = aggregates[i]->Evaluate(&child_tuple, &child_schema);
          if (!input.IsNull()) {
            if (state.aggregate_values[i].IsNull() ||
                input.CompareLessThan(state.aggregate_values[i]).GetAsBoolean()) {
              state.aggregate_values[i] = input;
            }
          }
          break;
        }
        case AggregationType::MaxAggregate: {
          auto input = aggregates[i]->Evaluate(&child_tuple, &child_schema);
          if (!input.IsNull()) {
            if (state.aggregate_values[i].IsNull() ||
                input.CompareGreaterThan(state.aggregate_values[i]).GetAsBoolean()) {
              state.aggregate_values[i] = input;
            }
          }
          break;
        }
      }
    }
  };

  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> group_values;
    group_values.reserve(group_bys.size());
    for (const auto &group_expr : group_bys) {
      group_values.push_back(group_expr->Evaluate(&child_tuple, &child_schema));
    }

    auto key = make_group_key(group_values);
    auto it = groups.find(key);
    if (it == groups.end()) {
      AggState state;
      state.group_values = group_values;
      state.aggregate_values = init_aggregate_values();
      it = groups.emplace(key, std::move(state)).first;
    }
    update_aggregates(it->second, child_tuple);
  }

  if (groups.empty()) {
    if (group_bys.empty()) {
      auto default_agg_values = init_aggregate_values();
      result_tuples_.emplace_back(std::move(default_agg_values));
    }
    return;
  }

  for (auto &[key, state] : groups) {
    std::vector<Value> output_values;
    output_values.reserve(state.group_values.size() + state.aggregate_values.size());
    output_values.insert(output_values.end(), state.group_values.begin(), state.group_values.end());
    output_values.insert(output_values.end(), state.aggregate_values.begin(), state.aggregate_values.end());
    result_tuples_.emplace_back(std::move(output_values));
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Return next aggregation result
  if (cursor_ >= result_tuples_.size()) {
    return false;
  }
  *tuple = result_tuples_[cursor_++];
  *rid = RID();
  return true;
}

}  // namespace onebase
