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

#include "query_optimizer/rules/EliminateSemiAntiJoinResidualPredicate.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

#include "expressions/aggregation/AggregateFunctionCount.hpp"
#include "query_optimizer/OptimizerContext.hpp"
#include "query_optimizer/expressions/AggregateFunction.hpp"
#include "query_optimizer/expressions/Alias.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/ComparisonExpression.hpp"
#include "query_optimizer/expressions/ExpressionUtil.hpp"
#include "query_optimizer/expressions/Predicate.hpp"
#include "query_optimizer/expressions/ScalarLiteral.hpp"
#include "query_optimizer/logical/Aggregate.hpp"
#include "query_optimizer/logical/Filter.hpp"
#include "query_optimizer/logical/HashJoin.hpp"
#include "query_optimizer/logical/Logical.hpp"
#include "query_optimizer/logical/PatternMatcher.hpp"
#include "types/LongType.hpp"
#include "types/TypedValue.hpp"
#include "types/operations/comparisons/Comparison.hpp"
#include "types/operations/comparisons/ComparisonID.hpp"
#include "types/operations/comparisons/EqualComparison.hpp"
#include "types/operations/comparisons/GreaterComparison.hpp"

namespace quickstep {
namespace optimizer {

namespace E = ::quickstep::optimizer::expressions;
namespace L = ::quickstep::optimizer::logical;

L::LogicalPtr EliminateSemiAntiJoinResidualPredicate::applyToNode(
    const L::LogicalPtr &input) {
  L::HashJoinPtr hash_join;
  if (!L::SomeHashJoin::MatchesWithConditionalCast(input, &hash_join) ||
      hash_join->residual_predicate() == nullptr) {
    return input;
  }

  switch (hash_join->join_type()) {
    case L::HashJoin::JoinType::kLeftSemiJoin:  // Fall through
    case L::HashJoin::JoinType::kLeftAntiJoin:
      break;
    default:
      return input;
  }

  E::ComparisonExpressionPtr comparison_expression;
  const bool is_comparison =
      E::SomeComparisonExpression::MatchesWithConditionalCast(
          hash_join->residual_predicate(),
          &comparison_expression);
  if (!is_comparison ||
      comparison_expression->comparison().getComparisonID() != ComparisonID::kNotEqual) {
    return input;
  }

  E::AttributeReferencePtr lhs_attribute;
  E::AttributeReferencePtr rhs_attribute;
  if (!E::SomeAttributeReference::MatchesWithConditionalCast(
           comparison_expression->left(), &lhs_attribute) ||
      !E::SomeAttributeReference::MatchesWithConditionalCast(
           comparison_expression->right(), &rhs_attribute)) {
    return input;
  }

  const L::LogicalPtr &left_child = hash_join->left();
  const L::LogicalPtr &right_child = hash_join->right();

  // TODO(jianqiao): IMPORTANT: (limited) check query containment: right_child
  // $\supersetequal$ left_child

  E::AttributeReferencePtr probe_side_attribute;
  E::AttributeReferencePtr build_side_attribute;
  if (E::ContainsExprId(left_child->getOutputAttributes(), lhs_attribute->id())) {
    DCHECK(E::ContainsExprId(right_child->getOutputAttributes(), rhs_attribute->id()));
    probe_side_attribute = lhs_attribute;
    build_side_attribute = rhs_attribute;
  } else {
    DCHECK(E::ContainsExprId(left_child->getOutputAttributes(), rhs_attribute->id()));
    DCHECK(E::ContainsExprId(right_child->getOutputAttributes(), lhs_attribute->id()));
    probe_side_attribute = rhs_attribute;
    build_side_attribute = lhs_attribute;
  }

  // TODO(jianqiao): IMPORTANT: count_distinct_equal_one operator

  const E::AggregateFunctionPtr count_function =
      E::AggregateFunction::Create(quickstep::AggregateFunctionCount::Instance(),
                                   {build_side_attribute},
                                   true /* is_vector_aggregate */,
                                   false /* is_distinct */);
  const E::AliasPtr count_alias =
      E::Alias::Create(context_->nextExprId(),
                       count_function,
                       "",
                       "COUNT(" + build_side_attribute->attribute_name() + ")");

  const L::AggregatePtr aggregate =
      L::Aggregate::Create(right_child,
                           E::ToNamedExpressions(hash_join->right_join_attributes()),
                           {count_alias});

  const E::ScalarLiteralPtr literal_one =
      E::ScalarLiteral::Create(TypedValue(static_cast<std::int64_t>(1)),
                               LongType::InstanceNonNullable());

  E::PredicatePtr uniqueness_predicate;
  switch (hash_join->join_type()) {
    case L::HashJoin::JoinType::kLeftSemiJoin: {
      uniqueness_predicate =
          E::ComparisonExpression::Create(quickstep::GreaterComparison::Instance(),
                                          E::ToRef(count_alias),
                                          literal_one);
      break;
    }
    case L::HashJoin::JoinType::kLeftAntiJoin:
      uniqueness_predicate =
          E::ComparisonExpression::Create(quickstep::EqualComparison::Instance(),
                                          E::ToRef(count_alias),
                                          literal_one);
      break;
    default:
      LOG(FATAL) << "Not supported";
  }

  const L::FilterPtr filter =
      L::Filter::Create(aggregate, uniqueness_predicate);

  return L::HashJoin::Create(left_child,
                             filter,
                             hash_join->left_join_attributes(),
                             hash_join->right_join_attributes(),
                             nullptr,
                             L::HashJoin::JoinType::kLeftSemiJoin);
}

}  // namespace optimizer
}  // namespace quickstep
