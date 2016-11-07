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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_RULES_REDUCE_GROUP_BY_ATTRIBUTES_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_RULES_REDUCE_GROUP_BY_ATTRIBUTES_HPP_

#include <memory>
#include <string>
#include <unordered_map>

#include "query_optimizer/cost_model/StarSchemaSimpleCostModel.hpp"
#include "query_optimizer/expressions/ExprId.hpp"
#include "query_optimizer/physical/Physical.hpp"
#include "query_optimizer/physical/TableReference.hpp"
#include "query_optimizer/rules/Rule.hpp"
#include "utility/Macros.hpp"

namespace quickstep {
namespace optimizer {

class OptimizerContext;

class ReduceGroupByAttributes : public Rule<physical::Physical> {
 public:
  ReduceGroupByAttributes(OptimizerContext *optimizer_context)
      : optimizer_context_(optimizer_context) {}

  ~ReduceGroupByAttributes() override {}

  std::string getName() const override {
    return "ReduceGroupByAttributes";
  }

  physical::PhysicalPtr apply(const physical::PhysicalPtr &input) override;

 private:
  struct AttributeInfo {
    AttributeInfo(const expressions::AttributeReferencePtr attribute_in,
                  const bool is_unique_in,
                  const bool is_fixed_length_in,
                  const std::size_t maximum_size_in)
        : attribute(attribute_in),
          is_unique(is_unique_in),
          is_fixed_length(is_fixed_length_in),
          maximum_size(maximum_size_in) {}

    inline static bool IsBetterThan(const AttributeInfo *lhs,
                                    const AttributeInfo *rhs) {
      if (lhs->is_unique != rhs->is_unique) {
        return lhs->is_unique;
      }
      if (lhs->is_fixed_length != rhs->is_fixed_length) {
        return lhs->is_fixed_length;
      }
      if (lhs->maximum_size != rhs->maximum_size) {
        return lhs->maximum_size < rhs->maximum_size;
      }
      return lhs->attribute->id() < rhs->attribute->id();
    }

    const expressions::AttributeReferencePtr attribute;
    const bool is_unique;
    const bool is_fixed_length;
    const std::size_t maximum_size;
  };

  physical::PhysicalPtr applyInternal(const physical::PhysicalPtr &input);
  physical::PhysicalPtr applyToNode(const physical::PhysicalPtr &input);

  OptimizerContext *optimizer_context_;
  std::unique_ptr<cost::StarSchemaSimpleCostModel> cost_model_;

  std::unordered_map<expressions::ExprId,
                     std::pair<physical::TableReferencePtr,
                               expressions::AttributeReferencePtr>> source_;

  DISALLOW_COPY_AND_ASSIGN(ReduceGroupByAttributes);
};

}  // namespace optimizer
}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_OPTIMIZER_RULES_REDUCE_GROUP_BY_ATTRIBUTES_HPP_
