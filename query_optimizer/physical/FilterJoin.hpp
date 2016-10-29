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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_FILTER_JOIN_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_FILTER_JOIN_HPP_

#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "query_optimizer/OptimizerTree.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/ExpressionUtil.hpp"
#include "query_optimizer/expressions/NamedExpression.hpp"
#include "query_optimizer/expressions/Predicate.hpp"
#include "query_optimizer/physical/BinaryJoin.hpp"
#include "query_optimizer/physical/Physical.hpp"
#include "query_optimizer/physical/PhysicalType.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {
namespace optimizer {
namespace physical {

/** \addtogroup OptimizerPhysical
 *  @{
 */

class FilterJoin;
typedef std::shared_ptr<const FilterJoin> FilterJoinPtr;

/**
 * @brief Physical filter join node.
 */
class FilterJoin : public BinaryJoin {
 public:
  PhysicalType getPhysicalType() const override { return PhysicalType::kFilterJoin; }

  std::string getName() const override {
    return "FilterJoin";
  }

  const std::vector<expressions::AttributeReferencePtr>& probe_join_attributes() const {
    return probe_join_attributes_;
  }

  const std::vector<expressions::AttributeReferencePtr>& build_join_attributes() const {
    return build_join_attributes_;
  }

  PhysicalPtr copyWithNewChildren(
      const std::vector<PhysicalPtr> &new_children) const override {
    DCHECK_EQ(children().size(), new_children.size());
    return Create(new_children[0],
                  new_children[1],
                  probe_join_attributes_,
                  build_join_attributes_,
                  project_expressions());
  }

  std::vector<expressions::AttributeReferencePtr> getReferencedAttributes() const override;

  bool maybeCopyWithPrunedExpressions(
      const expressions::UnorderedNamedExpressionSet &referenced_expressions,
      PhysicalPtr *output) const override;

  static FilterJoinPtr Create(
      const PhysicalPtr &probe_child,
      const PhysicalPtr &build_child,
      const std::vector<expressions::AttributeReferencePtr> &probe_join_attributes,
      const std::vector<expressions::AttributeReferencePtr> &build_join_attributes,
      const std::vector<expressions::NamedExpressionPtr> &project_expressions) {
    return FilterJoinPtr(
        new FilterJoin(probe_child,
                       build_child,
                       probe_join_attributes,
                       build_join_attributes,
                       project_expressions));
  }

 protected:
  void getFieldStringItems(
      std::vector<std::string> *inline_field_names,
      std::vector<std::string> *inline_field_values,
      std::vector<std::string> *non_container_child_field_names,
      std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
      std::vector<std::string> *container_child_field_names,
      std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const override;

 private:
  FilterJoin(
      const PhysicalPtr &probe_child,
      const PhysicalPtr &build_child,
      const std::vector<expressions::AttributeReferencePtr> &probe_join_attributes,
      const std::vector<expressions::AttributeReferencePtr> &build_join_attributes,
      const std::vector<expressions::NamedExpressionPtr> &project_expressions)
      : BinaryJoin(probe_child, build_child, project_expressions),
        probe_join_attributes_(probe_join_attributes),
        build_join_attributes_(build_join_attributes) {
  }

  std::vector<expressions::AttributeReferencePtr> probe_join_attributes_;
  std::vector<expressions::AttributeReferencePtr> build_join_attributes_;

  DISALLOW_COPY_AND_ASSIGN(FilterJoin);
};

/** @} */

}  // namespace physical
}  // namespace optimizer
}  // namespace quickstep

#endif /* QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_FILTER_JOIN_HPP_ */
