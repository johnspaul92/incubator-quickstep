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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_RULES_PUSH_DOWN_LOW_COST_DISJUNCTIVE_PREDICATE_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_RULES_PUSH_DOWN_LOW_COST_DISJUNCTIVE_PREDICATE_HPP_

#include <cstddef>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "query_optimizer/cost_model/StarSchemaSimpleCostModel.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/ExprId.hpp"
#include "query_optimizer/physical/Physical.hpp"
#include "query_optimizer/rules/Rule.hpp"
#include "utility/Macros.hpp"

namespace quickstep {
namespace optimizer {

/** \addtogroup OptimizerRules
 *  @{
 */

class PushDownLowCostDisjunctivePredicate : public Rule<physical::Physical> {
 public:
  /**
   * @brief Constructor.
   */
  PushDownLowCostDisjunctivePredicate() {}

  ~PushDownLowCostDisjunctivePredicate() override {}

  std::string getName() const override {
    return "PushDownLowCostDisjunctivePredicate";
  }

  physical::PhysicalPtr apply(const physical::PhysicalPtr &input) override;

 private:
  DISALLOW_COPY_AND_ASSIGN(PushDownLowCostDisjunctivePredicate);
};

/** @} */

}  // namespace optimizer
}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_OPTIMIZER_RULES_PUSH_DOWN_LOW_COST_DISJUNCTIVE_PREDICATE_HPP_