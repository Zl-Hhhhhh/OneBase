#pragma once
#include <vector>
#include "onebase/execution/executors/abstract_executor.h"
#include "onebase/execution/plans/plan_nodes.h"

namespace onebase {

class IndexScanExecutor : public AbstractExecutor {
 public:
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);
  void Init() override;
  auto Next(Tuple *tuple, RID *rid) -> bool override;
  auto GetOutputSchema() const -> const Schema & override { return plan_->GetOutputSchema(); }

 private:
  const IndexScanPlanNode *plan_;
  TableInfo *table_info_{nullptr};
  IndexInfo *index_info_{nullptr};
    std::vector<Tuple> result_tuples_;
  std::vector<RID> matching_rids_;
  size_t cursor_{0};
};

}  // namespace onebase
