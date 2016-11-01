/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/

#include "relational_operators/InitializeAggregationStateOperator.hpp"

#include <vector>

#include "query_execution/QueryContext.hpp"
#include "query_execution/WorkOrderProtosContainer.hpp"
#include "query_execution/WorkOrdersContainer.hpp"
#include "relational_operators/WorkOrder.pb.h"
#include "storage/AggregationOperationState.hpp"

#include "tmb/id_typedefs.h"

namespace quickstep {

bool InitializeAggregationStateOperator::getAllWorkOrders(
    WorkOrdersContainer *container,
    QueryContext *query_context,
    StorageManager *storage_manager,
    const tmb::client_id scheduler_client_id,
    tmb::MessageBus *bus) {
  if (!started_) {
    AggregationOperationState *agg_state =
        query_context->getAggregationState(aggr_state_index_);
    DCHECK(agg_state != nullptr);

    for (std::size_t partition_id = 0;
         partition_id < agg_state->getNumInitializePartitions();
         ++partition_id) {
      container->addNormalWorkOrder(
          new InitializeAggregationStateWorkOrder(
              query_id_,
              partition_id,
              agg_state),
          op_index_);
    }
    started_ = true;
  }
  return started_;
}

bool InitializeAggregationStateOperator::getAllWorkOrderProtos(WorkOrderProtosContainer *container) {
  // TODO
  LOG(FATAL) << "Not implemented";
}

void InitializeAggregationStateWorkOrder::execute() {
  state_->initializeState(partition_id_);
}

}  // namespace quickstep
