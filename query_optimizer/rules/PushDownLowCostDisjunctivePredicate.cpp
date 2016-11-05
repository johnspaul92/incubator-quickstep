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

#include "query_optimizer/rules/PushDownLowCostDisjunctivePredicate.hpp"

#include <map>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <utility>

#include "query_optimizer/cost_model/StarSchemaSimpleCostModel.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/ExpressionUtil.hpp"
#include "query_optimizer/expressions/LogicalAnd.hpp"
#include "query_optimizer/expressions/LogicalOr.hpp"
#include "query_optimizer/expressions/PatternMatcher.hpp"
#include "query_optimizer/expressions/Predicate.hpp"
#include "query_optimizer/physical/HashJoin.hpp"
#include "query_optimizer/physical/NestedLoopsJoin.hpp"
#include "query_optimizer/physical/PatternMatcher.hpp"
#include "query_optimizer/physical/Physical.hpp"
#include "query_optimizer/physical/PhysicalType.hpp"
#include "query_optimizer/physical/Selection.hpp"
#include "query_optimizer/physical/TableReference.hpp"

#include "glog/logging.h"

namespace quickstep {
namespace optimizer {

namespace E = ::quickstep::optimizer::expressions;
namespace P = ::quickstep::optimizer::physical;

P::PhysicalPtr PushDownLowCostDisjunctivePredicate::apply(const P::PhysicalPtr &input) {
  DCHECK(input->getPhysicalType() == P::PhysicalType::kTopLevelPlan);

  const P::TopLevelPlanPtr top_level_plan =
     std::static_pointer_cast<const P::TopLevelPlan>(input);
  cost_model_.reset(
      new cost::StarSchemaSimpleCostModel(
          top_level_plan->shared_subplans()));

  collectApplicablePredicates(input);
//  std::cout << "Num nodes = " << applicable_predicates_.size() << "\n";
//  for (const auto &pair : applicable_predicates_) {
//    std::cout << "Num predicates = " << pair.second.predicates.size() << "\n";
//    for (const auto &predicate : pair.second.predicates) {
//      std::cout << predicate->toString() << "\n";
//    }
//  }

  if (!applicable_predicates_.empty()) {
    return attachPredicates(input);
  } else {
    return input;
  }
}

void PushDownLowCostDisjunctivePredicate::collectApplicablePredicates(
    const physical::PhysicalPtr &input) {
  // Currently consider only stored relations with small cardinality as targets.
  P::TableReferencePtr table_reference;
  if (P::SomeTableReference::MatchesWithConditionalCast(input, &table_reference)) {
    if (cost_model_->estimateCardinality(input) <= 100u) {
      applicable_nodes_.emplace(input, table_reference->attribute_list());
    }
    return;
  }

  for (const auto &child : input->children()) {
    collectApplicablePredicates(child);
  }

  E::PredicatePtr filter_predicate = nullptr;
  switch (input->getPhysicalType()) {
    case P::PhysicalType::kHashJoin:
      filter_predicate =
          std::static_pointer_cast<const P::HashJoin>(input)->residual_predicate();
      break;
    case P::PhysicalType::kNestedLoopsJoin:
      filter_predicate =
          std::static_pointer_cast<const P::NestedLoopsJoin>(input)->join_predicate();
      break;
    default:
      break;
  }

  E::LogicalOrPtr disjunctive_predicate;
  if (filter_predicate == nullptr ||
      !E::SomeLogicalOr::MatchesWithConditionalCast(filter_predicate, &disjunctive_predicate)) {
    return;
  }

  // Currently consider only disjunctive normal form. E.g. disjunction of conjunctions.
  std::vector<std::vector<E::PredicatePtr>> candidate_predicates;
  std::vector<std::vector<std::vector<E::AttributeReferencePtr>>> candidate_attributes;
  for (const auto &conjunctive_predicate : disjunctive_predicate->operands()) {
    candidate_predicates.emplace_back();
    candidate_attributes.emplace_back();
    E::LogicalAndPtr logical_and;
    if (E::SomeLogicalAnd::MatchesWithConditionalCast(conjunctive_predicate, &logical_and)) {
      for (const auto &predicate : logical_and->operands()) {
        candidate_predicates.back().emplace_back(predicate);
        candidate_attributes.back().emplace_back(
            predicate->getReferencedAttributes());
      }
    } else {
      candidate_predicates.back().emplace_back(conjunctive_predicate);
      candidate_attributes.back().emplace_back(
          conjunctive_predicate->getReferencedAttributes());
    }
  }

  for (const auto &node_pair : applicable_nodes_) {
    const std::vector<E::AttributeReferencePtr> &target_attributes = node_pair.second;
    std::vector<E::PredicatePtr> selected_disj_preds;
    for (std::size_t i = 0; i < candidate_predicates.size(); ++i) {
      const auto &cand_preds = candidate_predicates[i];
      const auto &cand_attrs = candidate_attributes[i];

      std::vector<E::PredicatePtr> selected_conj_preds;
      for (std::size_t j = 0; j < cand_preds.size(); ++j) {
        if (E::SubsetOfExpressions(cand_attrs[j], target_attributes)) {
          selected_conj_preds.emplace_back(cand_preds[j]);
        }
      }
      if (!selected_conj_preds.empty()) {
        selected_disj_preds.emplace_back(
            CreateConjunctive(selected_conj_preds));
      }
    }
    if (!selected_disj_preds.empty()) {
      applicable_predicates_[node_pair.first].add(
          CreateDisjunctive(selected_disj_preds));
    }
  }
}

P::PhysicalPtr PushDownLowCostDisjunctivePredicate::attachPredicates(
    const P::PhysicalPtr &input) const {
  std::vector<P::PhysicalPtr> new_children;
  for (const P::PhysicalPtr &child : input->children()) {
    const P::PhysicalPtr new_child = attachPredicates(child);
    new_children.push_back(new_child);
  }

  const P::PhysicalPtr output =
      new_children == input->children() ? input
                                        : input->copyWithNewChildren(new_children);

  const auto &node_it = applicable_predicates_.find(input);
  if (node_it != applicable_predicates_.end()) {
    const E::PredicatePtr filter_predicate =
        CreateConjunctive(node_it->second.predicates);
    return P::Selection::Create(output,
                                E::ToNamedExpressions(output->getOutputAttributes()),
                                filter_predicate);
  }

  return output;
}

E::PredicatePtr PushDownLowCostDisjunctivePredicate::CreateConjunctive(
    const std::vector<E::PredicatePtr> predicates) {
  DCHECK_GE(predicates.size(), 1u);
  if (predicates.size() == 1) {
    return predicates.front();
  } else {
    return E::LogicalAnd::Create(predicates);
  }
}

E::PredicatePtr PushDownLowCostDisjunctivePredicate::CreateDisjunctive(
    const std::vector<E::PredicatePtr> predicates) {
  DCHECK_GE(predicates.size(), 1u);
  if (predicates.size() == 1) {
    return predicates.front();
  } else {
    return E::LogicalOr::Create(predicates);
  }
}


}  // namespace optimizer
}  // namespace quickstep
