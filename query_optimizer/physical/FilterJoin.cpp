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

#include "query_optimizer/physical/FilterJoin.hpp"

#include <string>
#include <vector>

#include "query_optimizer/OptimizerTree.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/ExpressionUtil.hpp"
#include "query_optimizer/expressions/NamedExpression.hpp"
#include "query_optimizer/expressions/Predicate.hpp"
#include "utility/Cast.hpp"

namespace quickstep {
namespace optimizer {
namespace physical {

namespace E = ::quickstep::optimizer::expressions;

std::vector<E::AttributeReferencePtr> FilterJoin::getReferencedAttributes() const {
  std::vector<E::AttributeReferencePtr> referenced_attributes;
  for (const auto &project_expression : project_expressions()) {
    const std::vector<E::AttributeReferencePtr> referenced_attributes_in_expression =
        project_expression->getReferencedAttributes();
    referenced_attributes.insert(referenced_attributes.end(),
                                 referenced_attributes_in_expression.begin(),
                                 referenced_attributes_in_expression.end());
  }
  referenced_attributes.insert(referenced_attributes.end(),
                               probe_join_attributes_.begin(),
                               probe_join_attributes_.end());
  referenced_attributes.insert(referenced_attributes.end(),
                               build_join_attributes_.begin(),
                               build_join_attributes_.end());
  return referenced_attributes;
}

bool FilterJoin::maybeCopyWithPrunedExpressions(
    const E::UnorderedNamedExpressionSet &referenced_expressions,
    PhysicalPtr *output) const {
  std::vector<E::NamedExpressionPtr> new_project_expressions;
  const auto &current_project_expressions = project_expressions();
  for (const auto &project_expression : current_project_expressions) {
    if (referenced_expressions.find(project_expression) != referenced_expressions.end()) {
      new_project_expressions.emplace_back(project_expression);
    }
  }
  if (new_project_expressions.size() != current_project_expressions.size()) {
    *output = Create(left(),
                     right(),
                     probe_join_attributes_,
                     build_join_attributes_,
                     new_project_expressions);
    return true;
  }
  return false;
}

void FilterJoin::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  BinaryJoin::getFieldStringItems(inline_field_names,
                                  inline_field_values,
                                  non_container_child_field_names,
                                  non_container_child_fields,
                                  container_child_field_names,
                                  container_child_fields);
  container_child_field_names->push_back("probe_join_attributes");
  container_child_fields->push_back(CastSharedPtrVector<OptimizerTreeBase>(probe_join_attributes_));
  container_child_field_names->push_back("build_join_attributes");
  container_child_fields->push_back(CastSharedPtrVector<OptimizerTreeBase>(build_join_attributes_));
}

}  // namespace physical
}  // namespace optimizer
}  // namespace quickstep
