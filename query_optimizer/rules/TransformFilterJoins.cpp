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

#include "query_optimizer/rules/TransformFilterJoins.hpp"

#include <map>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <utility>

#include "query_optimizer/cost_model/StarSchemaSimpleCostModel.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/physical/LIPFilterConfiguration.hpp"
#include "query_optimizer/physical/Aggregate.hpp"
#include "query_optimizer/physical/HashJoin.hpp"
#include "query_optimizer/physical/PatternMatcher.hpp"
#include "query_optimizer/physical/Physical.hpp"
#include "query_optimizer/physical/PhysicalType.hpp"
#include "query_optimizer/physical/Selection.hpp"
#include "query_optimizer/physical/TopLevelPlan.hpp"
#include "types/TypedValue.hpp"
#include "utility/lip_filter/LIPFilter.hpp"

#include "glog/logging.h"

namespace quickstep {
namespace optimizer {

namespace E = ::quickstep::optimizer::expressions;
namespace P = ::quickstep::optimizer::physical;

P::PhysicalPtr TransformFilterJoins::apply(const P::PhysicalPtr &input) {
  DCHECK(input->getPhysicalType() == P::PhysicalType::kTopLevelPlan);

  const P::TopLevelPlanPtr top_level_plan =
     std::static_pointer_cast<const P::TopLevelPlan>(input);
  cost_model_.reset(
      new cost::StarSchemaSimpleCostModel(
          top_level_plan->shared_subplans()));

  P::PhysicalPtr output = applyTransform(input);
  return output;
}

P::PhysicalPtr TransformFilterJoins::applyTransform(const P::PhysicalPtr &input) {
//  std::vector<P::PhysicalPtr> new_children;
//  bool has_changed_children = false;
//  for (const P::PhysicalPtr &child : input->children()) {
//    P::PhysicalPtr new_child = applyTransform(child);
//    if (child != new_child && !has_changed_children) {
//      has_changed_children = true;
//    }
//    new_children.push_back(new_child);
//  }
//
//  P::HashJoinPtr hash_join;
//  if (P::SomeHashJoin::MatchesWithConditionalCast(input, &hash_join)) {
//    // TODO(jianqiao): check for other cases
//    if (hash_join->join_type() == P::HashJoin::JoinType::kLeftSemiJoin) {
//
//
//    }
//  }
//
//  if (has_changed_children) {
//    return applyToNode(tree->copyWithNewChildren(new_children));
//  } else {
//    return applyToNode(tree);
//  }
  return input;
}

}  // namespace optimizer
}  // namespace quickstep
