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

#include "query_optimizer/cost_model/StarSchemaSimpleCostModel.hpp"

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <vector>

#include "catalog/CatalogRelation.hpp"
#include "catalog/CatalogRelationStatistics.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/ComparisonExpression.hpp"
#include "query_optimizer/expressions/ExprId.hpp"
#include "query_optimizer/expressions/ExpressionType.hpp"
#include "query_optimizer/expressions/ExpressionUtil.hpp"
#include "query_optimizer/expressions/LogicalAnd.hpp"
#include "query_optimizer/expressions/LogicalOr.hpp"
#include "query_optimizer/expressions/Predicate.hpp"
#include "query_optimizer/expressions/PatternMatcher.hpp"
#include "query_optimizer/physical/Aggregate.hpp"
#include "query_optimizer/physical/NestedLoopsJoin.hpp"
#include "query_optimizer/physical/FilterInjection.hpp"
#include "query_optimizer/physical/HashJoin.hpp"
#include "query_optimizer/physical/PatternMatcher.hpp"
#include "query_optimizer/physical/Physical.hpp"
#include "query_optimizer/physical/PhysicalType.hpp"
#include "query_optimizer/physical/Selection.hpp"
#include "query_optimizer/physical/SharedSubplanReference.hpp"
#include "query_optimizer/physical/Sort.hpp"
#include "query_optimizer/physical/TableGenerator.hpp"
#include "query_optimizer/physical/TableReference.hpp"
#include "query_optimizer/physical/TopLevelPlan.hpp"
#include "types/TypedValue.hpp"
#include "types/NullType.hpp"

#include "glog/logging.h"

namespace quickstep {
namespace optimizer {
namespace cost {

namespace E = ::quickstep::optimizer::expressions;
namespace P = ::quickstep::optimizer::physical;

std::size_t StarSchemaSimpleCostModel::estimateCardinality(
    const P::PhysicalPtr &physical_plan) {
  switch (physical_plan->getPhysicalType()) {
    case P::PhysicalType::kTopLevelPlan:
      return estimateCardinalityForTopLevelPlan(
          std::static_pointer_cast<const P::TopLevelPlan>(physical_plan));
    case P::PhysicalType::kTableReference:
      return estimateCardinalityForTableReference(
          std::static_pointer_cast<const P::TableReference>(physical_plan));
    case P::PhysicalType::kSelection:
      return estimateCardinalityForSelection(
          std::static_pointer_cast<const P::Selection>(physical_plan));
    case P::PhysicalType::kTableGenerator:
      return estimateCardinalityForTableGenerator(
          std::static_pointer_cast<const P::TableGenerator>(physical_plan));
    case P::PhysicalType::kFilterInjection:
      return estimateCardinalityForFilterInjection(
          std::static_pointer_cast<const P::FilterInjection>(physical_plan));
    case P::PhysicalType::kHashJoin:
      return estimateCardinalityForHashJoin(
          std::static_pointer_cast<const P::HashJoin>(physical_plan));
    case P::PhysicalType::kNestedLoopsJoin:
      return estimateCardinalityForNestedLoopsJoin(
          std::static_pointer_cast<const P::NestedLoopsJoin>(physical_plan));
    case P::PhysicalType::kAggregate:
      return estimateCardinalityForAggregate(
          std::static_pointer_cast<const P::Aggregate>(physical_plan));
    case P::PhysicalType::kSharedSubplanReference: {
      const P::SharedSubplanReferencePtr shared_subplan_reference =
          std::static_pointer_cast<const P::SharedSubplanReference>(physical_plan);
      return estimateCardinality(
          shared_subplans_[shared_subplan_reference->subplan_id()]);
    }
    case P::PhysicalType::kSort:
      return estimateCardinalityForSort(
          std::static_pointer_cast<const P::Sort>(physical_plan));
    case P::PhysicalType::kWindowAggregate:
      return estimateCardinalityForWindowAggregate(
          std::static_pointer_cast<const P::WindowAggregate>(physical_plan));
    default:
      return 1u;
//      LOG(FATAL) << "Unsupported physical plan:" << physical_plan->toString();
  }
}

std::size_t StarSchemaSimpleCostModel::estimateCardinalityForTopLevelPlan(
    const P::TopLevelPlanPtr &physical_plan) {
  return estimateCardinality(physical_plan->plan());
}

std::size_t StarSchemaSimpleCostModel::estimateCardinalityForTableReference(
    const P::TableReferencePtr &physical_plan) {
  const CatalogRelation *relation = physical_plan->relation();
  const CatalogRelationStatistics &stat = relation->getStatistics();
  const std::size_t num_tuples =
      stat.hasNumTuples() ? stat.getNumTuples()
                          : relation->estimateTupleCardinality();
  return num_tuples;
}

std::size_t StarSchemaSimpleCostModel::estimateCardinalityForSelection(
    const P::SelectionPtr &physical_plan) {
  double selectivity =
      estimateSelectivityForPredicate(physical_plan->filter_predicate(), physical_plan);
  return static_cast<std::size_t>(
      estimateCardinality(physical_plan->input()) * selectivity);
}

std::size_t StarSchemaSimpleCostModel::estimateCardinalityForSort(
    const physical::SortPtr &physical_plan) {
  std::size_t cardinality = estimateCardinality(
      std::static_pointer_cast<const P::Sort>(physical_plan)->input());
  return std::min(cardinality, static_cast<std::size_t>(physical_plan->limit()));
}

std::size_t StarSchemaSimpleCostModel::estimateCardinalityForTableGenerator(
    const P::TableGeneratorPtr &physical_plan) {
  return physical_plan->generator_function_handle()->getEstimatedCardinality();
}

std::size_t StarSchemaSimpleCostModel::estimateCardinalityForFilterInjection(
    const P::FilterInjectionPtr &physical_plan) {
  double build_side_filter_selectivity =
      estimateSelectivityForPredicate(physical_plan->build_side_filter_predicate(),
                                      physical_plan->right());
  std::size_t left_cardinality = estimateCardinality(physical_plan->left());
  double right_selectivity = estimateSelectivity(physical_plan->right());
  return static_cast<std::size_t>(
      left_cardinality * build_side_filter_selectivity * right_selectivity + 0.5);
}

std::size_t StarSchemaSimpleCostModel::estimateCardinalityForHashJoin(
    const P::HashJoinPtr &physical_plan) {
  std::size_t left_cardinality = estimateCardinality(physical_plan->left());
  std::size_t right_cardinality = estimateCardinality(physical_plan->right());
  double left_selectivity = estimateSelectivity(physical_plan->left());
  double right_selectivity = estimateSelectivity(physical_plan->right());
  return std::max(static_cast<std::size_t>(left_cardinality * right_selectivity + 0.5),
                  static_cast<std::size_t>(right_cardinality * left_selectivity + 0.5));
}

std::size_t StarSchemaSimpleCostModel::estimateCardinalityForNestedLoopsJoin(
    const P::NestedLoopsJoinPtr &physical_plan) {
  return std::max(estimateCardinality(physical_plan->left()),
                  estimateCardinality(physical_plan->right()));
}

std::size_t StarSchemaSimpleCostModel::estimateCardinalityForAggregate(
    const P::AggregatePtr &physical_plan) {
  double filter_selectivity =
      estimateSelectivityForPredicate(physical_plan->filter_predicate(), physical_plan);
  return static_cast<std::size_t>(
      estimateNumGroupsForAggregate(physical_plan) * filter_selectivity);
}

std::size_t StarSchemaSimpleCostModel::estimateCardinalityForWindowAggregate(
    const P::WindowAggregatePtr &physical_plan) {
  return estimateCardinality(physical_plan->input());
}


std::size_t StarSchemaSimpleCostModel::estimateNumGroupsForAggregate(
    const physical::AggregatePtr &aggregate) {
  if (aggregate->grouping_expressions().empty()) {
    return 1uL;
  }

  std::size_t estimated_child_cardinality = estimateCardinality(aggregate->input());
  std::size_t estimated_num_groups = 1;
  std::size_t max_attr_num_distinct_values = 0;
  for (const auto &expr : aggregate->grouping_expressions()) {
    E::AttributeReferencePtr attr;
    if (E::SomeAttributeReference::MatchesWithConditionalCast(expr, &attr)) {
      std::size_t attr_num_distinct_values =
          estimateNumDistinctValues(attr->id(), aggregate->input());
      estimated_num_groups *= std::max(1uL, attr_num_distinct_values);
      max_attr_num_distinct_values =
          std::max(max_attr_num_distinct_values, attr_num_distinct_values);
    } else {
      // TODO(jianqiao): implement estimateNumDistinctValues() for expressions.
      estimated_num_groups *= 64uL;
    }
  }
  estimated_num_groups = std::max(
      std::min(estimated_num_groups, estimated_child_cardinality / 10),
      max_attr_num_distinct_values);
  return estimated_num_groups;
}


std::size_t StarSchemaSimpleCostModel::estimateNumDistinctValues(
    const expressions::ExprId attribute_id,
    const physical::PhysicalPtr &physical_plan) {
  DCHECK(E::ContainsExprId(physical_plan->getOutputAttributes(), attribute_id));

  P::TableReferencePtr table_reference;
  if (P::SomeTableReference::MatchesWithConditionalCast(physical_plan, &table_reference)) {
    return getNumDistinctValues(attribute_id, table_reference);
  }

  double filter_selectivity = estimateSelectivityForFilterPredicate(physical_plan);
  switch (physical_plan->getPhysicalType()) {
    case P::PhysicalType::kSelection:  // Fall through
    case P::PhysicalType::kAggregate: {
      const P::PhysicalPtr &child = physical_plan->children()[0];
      if (E::ContainsExprId(child->getOutputAttributes(), attribute_id)) {
        std::size_t child_num_distinct_values =
            estimateNumDistinctValues(attribute_id, child);
        return static_cast<std::size_t>(
            child_num_distinct_values * filter_selectivity + 0.5);
      }
      break;
    }
    case P::PhysicalType::kFilterInjection: {
      const P::FilterInjectionPtr &filter_injection =
          std::static_pointer_cast<const P::FilterInjection>(physical_plan);
      if (E::ContainsExprId(filter_injection->left()->getOutputAttributes(), attribute_id)) {
        std::size_t left_child_num_distinct_values =
            estimateNumDistinctValues(attribute_id, filter_injection->left());
        double right_child_selectivity =
            estimateSelectivity(filter_injection->right());
        return static_cast<std::size_t>(
            left_child_num_distinct_values * right_child_selectivity + 0.5);
      }
    }
    case P::PhysicalType::kHashJoin: {
      const P::HashJoinPtr &hash_join =
          std::static_pointer_cast<const P::HashJoin>(physical_plan);
      if (E::ContainsExprId(hash_join->left()->getOutputAttributes(), attribute_id)) {
        std::size_t left_child_num_distinct_values =
            estimateNumDistinctValues(attribute_id, hash_join->left());
        double right_child_selectivity =
            estimateSelectivity(hash_join->right());
        return static_cast<std::size_t>(
            left_child_num_distinct_values * right_child_selectivity * filter_selectivity + 0.5);
      }
      if (E::ContainsExprId(hash_join->right()->getOutputAttributes(), attribute_id)) {
        std::size_t right_child_num_distinct_values =
            estimateNumDistinctValues(attribute_id, hash_join->right());
        double left_child_selectivity =
            estimateSelectivity(hash_join->left());
        return static_cast<std::size_t>(
            right_child_num_distinct_values * left_child_selectivity * filter_selectivity + 0.5);
      }
    }
    default:
      break;
  }

  return 16uL;
}

double StarSchemaSimpleCostModel::estimateSelectivity(
    const physical::PhysicalPtr &physical_plan) {
  switch (physical_plan->getPhysicalType()) {
    case P::PhysicalType::kSelection: {
      const P::SelectionPtr &selection =
          std::static_pointer_cast<const P::Selection>(physical_plan);
      double filter_selectivity =
          estimateSelectivityForPredicate(selection->filter_predicate(), selection);
      double child_selectivity = estimateSelectivity(selection->input());
      return filter_selectivity * child_selectivity;
    }
    case P::PhysicalType::kFilterInjection: {
      const P::FilterInjectionPtr &filter_injection =
          std::static_pointer_cast<const P::FilterInjection>(physical_plan);
      double left_selectivity = estimateSelectivity(filter_injection->left());
      double right_selectivity = estimateSelectivity(filter_injection->right());
      double build_side_filter_selectivity =
          estimateSelectivityForPredicate(filter_injection->build_side_filter_predicate(),
                                          filter_injection->right());
      return left_selectivity * right_selectivity * build_side_filter_selectivity;
    }
    case P::PhysicalType::kHashJoin: {
      const P::HashJoinPtr &hash_join =
          std::static_pointer_cast<const P::HashJoin>(physical_plan);
      double filter_selectivity =
          estimateSelectivityForPredicate(hash_join->residual_predicate(), hash_join);
      double child_selectivity =
          estimateSelectivity(hash_join->left()) * estimateSelectivity(hash_join->right());
      return filter_selectivity * child_selectivity;
    }
    case P::PhysicalType::kNestedLoopsJoin: {
      const P::NestedLoopsJoinPtr &nested_loop_join =
          std::static_pointer_cast<const P::NestedLoopsJoin>(physical_plan);
      double filter_selectivity =
          estimateSelectivityForPredicate(nested_loop_join->join_predicate(), nested_loop_join);
      double child_selectivity = std::min(
          estimateSelectivity(nested_loop_join->left()),
          estimateSelectivity(nested_loop_join->right()));
      return filter_selectivity * child_selectivity;
    }
    case P::PhysicalType::kSharedSubplanReference: {
      const P::SharedSubplanReferencePtr shared_subplan_reference =
          std::static_pointer_cast<const P::SharedSubplanReference>(physical_plan);
      return estimateSelectivity(
          shared_subplans_[shared_subplan_reference->subplan_id()]);
    }
    default:
      break;
  }

  if (physical_plan->getNumChildren() == 1) {
    return estimateSelectivity(physical_plan->children()[0]);
  }

  return 1.0;
}

double StarSchemaSimpleCostModel::estimateSelectivityForFilterPredicate(
    const physical::PhysicalPtr &physical_plan) {
  E::PredicatePtr filter_predicate = nullptr;
  switch (physical_plan->getPhysicalType()) {
    case P::PhysicalType::kSelection:
      filter_predicate =
          std::static_pointer_cast<const P::Selection>(physical_plan)->filter_predicate();
      break;
    case P::PhysicalType::kAggregate:
      filter_predicate =
          std::static_pointer_cast<const P::Aggregate>(physical_plan)->filter_predicate();
      break;
    case P::PhysicalType::kHashJoin:
      filter_predicate =
          std::static_pointer_cast<const P::HashJoin>(physical_plan)->residual_predicate();
      break;
    case P::PhysicalType::kNestedLoopsJoin:
      filter_predicate =
          std::static_pointer_cast<const P::NestedLoopsJoin>(physical_plan)->join_predicate();
      break;
    default:
      break;
  }

  if (filter_predicate == nullptr) {
    return 1.0;
  } else {
    return estimateSelectivityForPredicate(filter_predicate, physical_plan);
  }
}


double StarSchemaSimpleCostModel::estimateSelectivityForPredicate(
    const expressions::PredicatePtr &filter_predicate,
    const P::PhysicalPtr &physical_plan) {
  if (filter_predicate == nullptr) {
    return 1.0;
  }

  switch (filter_predicate->getExpressionType()) {
    case E::ExpressionType::kComparisonExpression: {
      // Case 1 - Number of distinct values statistics available
      //   Case 1.1 - Equality comparison: 1.0 / num_distinct_values
      //   Case 1.2 - Otherwise: 0.1
      // Case 2 - Otherwise: 0.5
      const E::ComparisonExpressionPtr &comparison_expression =
          std::static_pointer_cast<const E::ComparisonExpression>(filter_predicate);
      E::AttributeReferencePtr attr;
      if ((E::SomeAttributeReference::MatchesWithConditionalCast(comparison_expression->left(), &attr) &&
           E::SomeScalarLiteral::Matches(comparison_expression->right())) ||
          (E::SomeAttributeReference::MatchesWithConditionalCast(comparison_expression->right(), &attr) &&
           E::SomeScalarLiteral::Matches(comparison_expression->left()))) {
        for (const auto &child : physical_plan->children()) {
          if (E::ContainsExprId(child->getOutputAttributes(), attr->id())) {
            const std::size_t child_num_distinct_values = estimateNumDistinctValues(attr->id(), child);
            if (comparison_expression->isEqualityComparisonPredicate()) {
              return 1.0 / child_num_distinct_values;
            } else {
              return 1.0 / std::max(std::min(child_num_distinct_values / 100.0, 10.0), 2.0);
            }
          }
        }
        return 0.1;
      }
      return 0.5;
    }
    case E::ExpressionType::kLogicalAnd: {
      const E::LogicalAndPtr &logical_and =
          std::static_pointer_cast<const E::LogicalAnd>(filter_predicate);
      double selectivity = 1.0;
      for (const auto &predicate : logical_and->operands()) {
        selectivity = std::min(selectivity, estimateSelectivityForPredicate(predicate, physical_plan));
      }
      return selectivity;
    }
    case E::ExpressionType::kLogicalOr: {
      const E::LogicalOrPtr &logical_or =
          std::static_pointer_cast<const E::LogicalOr>(filter_predicate);
      double selectivity = 0;
      for (const auto &predicate : logical_or->operands()) {
        selectivity += estimateSelectivityForPredicate(predicate, physical_plan);
      }
      return std::min(selectivity, 1.0);
    }
    default:
      break;
  }
  return 1.0;
}

std::size_t StarSchemaSimpleCostModel::getNumDistinctValues(
    const E::ExprId attribute_id,
    const P::TableReferencePtr &table_reference) {
  const auto rel_attr_id =
      findCatalogRelationAttributeId(table_reference, attribute_id);
  if (rel_attr_id != kInvalidAttributeID) {
    const CatalogRelationStatistics &stat =
        table_reference->relation()->getStatistics();
    if (stat.hasNumDistinctValues(rel_attr_id)) {
      return stat.getNumDistinctValues(rel_attr_id);
    }
  }
  return estimateCardinalityForTableReference(table_reference);
}

bool StarSchemaSimpleCostModel::impliesUniqueAttributes(
    const P::PhysicalPtr &physical_plan,
    const std::vector<E::AttributeReferencePtr> &attributes) {
  switch (physical_plan->getPhysicalType()) {
    case P::PhysicalType::kAggregate: {
      const P::AggregatePtr &aggregate =
          std::static_pointer_cast<const P::Aggregate>(physical_plan);
      return E::SubsetOfExpressions(aggregate->grouping_expressions(), attributes);
    }
    case P::PhysicalType::kHashJoin: {
      const P::HashJoinPtr &hash_join =
          std::static_pointer_cast<const P::HashJoin>(physical_plan);
      bool unique_from_left =
          impliesUniqueAttributes(hash_join->right(), hash_join->right_join_attributes())
              && impliesUniqueAttributes(hash_join->left(), attributes);
      bool unique_from_right =
          impliesUniqueAttributes(hash_join->left(), hash_join->left_join_attributes())
              && impliesUniqueAttributes(hash_join->right(), attributes);
      return unique_from_left || unique_from_right;
    }
    case P::PhysicalType::kTableReference: {
      const P::TableReferencePtr &table_reference =
          std::static_pointer_cast<const P::TableReference>(physical_plan);
      const CatalogRelationStatistics &stat =
          table_reference->relation()->getStatistics();
      if (stat.hasNumTuples()) {
        const std::size_t num_tuples = stat.getNumTuples();
        for (const auto &attr : attributes) {
          const attribute_id rel_attr_id =
              findCatalogRelationAttributeId(table_reference, attr->id());
          if (rel_attr_id != kInvalidAttributeID &&
              stat.hasNumDistinctValues(rel_attr_id) &&
              stat.getNumDistinctValues(rel_attr_id) == num_tuples) {
            return true;
          }
        }
      }
      return false;
    }
    case P::PhysicalType::kSample:  // Fall through
    case P::PhysicalType::kSelection:
    case P::PhysicalType::kSort: {
      DCHECK_EQ(physical_plan->getNumChildren(), 1u);
      return impliesUniqueAttributes(physical_plan->children()[0], attributes);
    }
    default:
      break;
  }
  return false;
}

TypedValue StarSchemaSimpleCostModel::findCatalogRelationStat(
    const P::PhysicalPtr &physical_plan,
    const E::ExprId attr_id,
    const StatType stat_type) {
  P::TableReferencePtr table_reference;
  if (P::SomeTableReference::MatchesWithConditionalCast(physical_plan, &table_reference)) {
    const attribute_id rel_attr_id =
        findCatalogRelationAttributeId(table_reference, attr_id);
    if (rel_attr_id != kInvalidAttributeID) {
      const CatalogRelationStatistics &stat =
          table_reference->relation()->getStatistics();
      switch (stat_type) {
        case StatType::kMin: {
          if (stat.hasMinValue(rel_attr_id)) {
            return stat.getMinValue(rel_attr_id);
          }
          break;
        }
        case StatType::kMax: {
          if (stat.hasMaxValue(rel_attr_id)) {
            return stat.getMaxValue(rel_attr_id);
          }
          break;
        }
        default:
          break;
      }
      return NullType::InstanceNullable().makeNullValue();
    }
  }

  for (const auto &child : physical_plan->children()) {
    if (E::ContainsExprId(child->getOutputAttributes(), attr_id)) {
      return findCatalogRelationStat(child, attr_id, stat_type);
    }
  }
  return NullType::InstanceNullable().makeNullValue();
}

attribute_id StarSchemaSimpleCostModel::findCatalogRelationAttributeId(
    const physical::TableReferencePtr &table_reference,
    const expressions::ExprId expr_id) {
  const auto &attribute_list = table_reference->attribute_list();
  for (std::size_t i = 0; i < attribute_list.size(); ++i) {
    if (attribute_list[i]->id() == expr_id) {
      return i;
    }
  }
  return kInvalidAttributeID;
}

}  // namespace cost
}  // namespace optimizer
}  // namespace quickstep
