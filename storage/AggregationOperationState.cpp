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

#include "storage/AggregationOperationState.hpp"

#include <cstddef>
#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/CatalogDatabaseLite.hpp"
#include "catalog/CatalogRelationSchema.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "expressions/ExpressionFactories.hpp"
#include "expressions/Expressions.pb.h"
#include "expressions/aggregation/AggregateFunction.hpp"
#include "expressions/aggregation/AggregateFunctionFactory.hpp"
#include "expressions/aggregation/AggregationHandle.hpp"
#include "expressions/aggregation/AggregationID.hpp"
#include "expressions/predicate/Predicate.hpp"
#include "expressions/scalar/Scalar.hpp"
#include "storage/AggregationOperationState.pb.h"
#include "storage/AggregationStateHashTable.hpp"
#include "storage/HashTableBase.hpp"
#include "storage/HashTableFactory.hpp"
#include "storage/InsertDestination.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageManager.hpp"
#include "types/TypeFunctors.hpp"
#include "types/TypedValue.hpp"
#include "types/containers/ColumnVector.hpp"
#include "types/containers/ColumnVectorsValueAccessor.hpp"
#include "types/containers/Tuple.hpp"

#include "glog/logging.h"

using std::unique_ptr;

namespace quickstep {

AggregationOperationState::AggregationOperationState(
    const CatalogRelationSchema &input_relation,
    const std::vector<const AggregateFunction *> &aggregate_functions,
    std::vector<std::vector<std::unique_ptr<const Scalar>>> &&arguments,
    std::vector<bool> &&is_distinct,
    std::vector<std::unique_ptr<const Scalar>> &&group_by,
    const Predicate *predicate,
    const std::size_t estimated_num_entries,
    const HashTableImplType hash_table_impl_type,
    const std::vector<HashTableImplType> &distinctify_hash_table_impl_types,
    StorageManager *storage_manager)
    : input_relation_(input_relation),
      predicate_(predicate),
      group_by_list_(std::move(group_by)),
      arguments_(std::move(arguments)),
      is_distinct_(std::move(is_distinct)),
      storage_manager_(storage_manager) {
  // Sanity checks: each aggregate has a corresponding list of arguments.
  DCHECK(aggregate_functions.size() == arguments_.size());

  // Get the types of GROUP BY expressions for creating HashTables below.
  std::vector<const Type *> group_by_types;
  for (const std::unique_ptr<const Scalar> &group_by_element : group_by_list_) {
    group_by_types.emplace_back(&group_by_element->getType());
  }

  std::vector<AggregationHandle *> aggregation_handles;
  aggregation_handles.clear();

  // Set up each individual aggregate in this operation.
  for (std::size_t i = 0; i < aggregate_functions.size(); ++i) {
    // Get the Types of this aggregate's arguments so that we can create an
    // AggregationHandle.
    std::vector<const Type *> argument_types;
    for (const std::unique_ptr<const Scalar> &argument : arguments_[i]) {
      argument_types.emplace_back(&argument->getType());
    }

    // Sanity checks: aggregate function exists and can apply to the specified
    // arguments.
    const AggregateFunction *agg_func = aggregate_functions[i];
    DCHECK(agg_func != nullptr);
    DCHECK(agg_func->canApplyToTypes(argument_types));

    // Have the AggregateFunction create an AggregationHandle that we can use
    // to do actual aggregate computation.
    handles_.emplace_back(agg_func->createHandle(argument_types));

    if (!group_by_list_.empty()) {
      // TODO(jianqiao): handle DISTINCT aggregation.
      // if (is_distinct_[i]) {
      // }
      aggregation_handles.emplace_back(handles_.back());
    } else {
      // Aggregation without GROUP BY: create a single global state.
      single_states_.emplace_back(handles_.back()->createInitialState());

#ifdef QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION
      // See if all of this aggregate's arguments are attributes in the input
      // relation. If so, remember the attribute IDs so that we can do copy
      // elision when actually performing the aggregation.
      std::vector<attribute_id> local_arguments_as_attributes;
      local_arguments_as_attributes.reserve(arguments_[i].size());
      for (const std::unique_ptr<const Scalar> &argument : arguments_[i]) {
        const attribute_id argument_id =
            argument->getAttributeIdForValueAccessor();
        if (argument_id == -1) {
          local_arguments_as_attributes.clear();
          break;
        } else {
          DCHECK_EQ(input_relation_.getID(),
                    argument->getRelationIdForValueAccessor());
          local_arguments_as_attributes.push_back(argument_id);
        }
      }

      arguments_as_attributes_.emplace_back(
          std::move(local_arguments_as_attributes));
#endif
    }
  }

  if (!group_by_list_.empty()) {
    // TODO: handle non-fast-path case
    for (const auto &group_by_attribute : group_by_list_) {
      const attribute_id attr_id =
          group_by_attribute->getAttributeIdForValueAccessor();
      CHECK_NE(attr_id, kInvalidAttributeID);

      group_by_attribute_ids_.emplace_back(attr_id);
    }

    // Aggregation with GROUP BY: create a HashTable pool for per-group states.
    group_by_hashtable_pool_.reset(new HashTablePool(estimated_num_entries,
                                                     hash_table_impl_type,
                                                     group_by_types,
                                                     aggregation_handles,
                                                     storage_manager));
  }
}

AggregationOperationState* AggregationOperationState::ReconstructFromProto(
    const serialization::AggregationOperationState &proto,
    const CatalogDatabaseLite &database,
    StorageManager *storage_manager) {
  DCHECK(ProtoIsValid(proto, database));

  // Rebuild contructor arguments from their representation in 'proto'.
  std::vector<const AggregateFunction *> aggregate_functions;
  std::vector<std::vector<std::unique_ptr<const Scalar>>> arguments;
  std::vector<bool> is_distinct;
  std::vector<HashTableImplType> distinctify_hash_table_impl_types;
  std::size_t distinctify_hash_table_impl_type_index = 0;
  for (int agg_idx = 0; agg_idx < proto.aggregates_size(); ++agg_idx) {
    const serialization::Aggregate &agg_proto = proto.aggregates(agg_idx);

    aggregate_functions.emplace_back(
        &AggregateFunctionFactory::ReconstructFromProto(agg_proto.function()));

    arguments.emplace_back();
    arguments.back().reserve(agg_proto.argument_size());
    for (int argument_idx = 0; argument_idx < agg_proto.argument_size();
         ++argument_idx) {
      arguments.back().emplace_back(ScalarFactory::ReconstructFromProto(
          agg_proto.argument(argument_idx), database));
    }

    is_distinct.emplace_back(agg_proto.is_distinct());

    if (agg_proto.is_distinct()) {
      distinctify_hash_table_impl_types.emplace_back(
          HashTableImplTypeFromProto(proto.distinctify_hash_table_impl_types(
              distinctify_hash_table_impl_type_index)));
      ++distinctify_hash_table_impl_type_index;
    }
  }

  std::vector<std::unique_ptr<const Scalar>> group_by_expressions;
  for (int group_by_idx = 0; group_by_idx < proto.group_by_expressions_size();
       ++group_by_idx) {
    group_by_expressions.emplace_back(ScalarFactory::ReconstructFromProto(
        proto.group_by_expressions(group_by_idx), database));
  }

  unique_ptr<Predicate> predicate;
  if (proto.has_predicate()) {
    predicate.reset(
        PredicateFactory::ReconstructFromProto(proto.predicate(), database));
  }

  return new AggregationOperationState(
      database.getRelationSchemaById(proto.relation_id()),
      aggregate_functions,
      std::move(arguments),
      std::move(is_distinct),
      std::move(group_by_expressions),
      predicate.release(),
      proto.estimated_num_entries(),
      HashTableImplTypeFromProto(proto.hash_table_impl_type()),
      distinctify_hash_table_impl_types,
      storage_manager);
}

bool AggregationOperationState::ProtoIsValid(
    const serialization::AggregationOperationState &proto,
    const CatalogDatabaseLite &database) {
  if (!proto.IsInitialized() ||
      !database.hasRelationWithId(proto.relation_id()) ||
      (proto.aggregates_size() < 0)) {
    return false;
  }

  std::size_t num_distinctify_hash_tables =
      proto.distinctify_hash_table_impl_types_size();
  std::size_t distinctify_hash_table_impl_type_index = 0;
  for (int i = 0; i < proto.aggregates_size(); ++i) {
    if (!AggregateFunctionFactory::ProtoIsValid(
            proto.aggregates(i).function())) {
      return false;
    }

    // TODO(chasseur): We may also want to check that the specified
    // AggregateFunction is applicable to the specified arguments, but that
    // requires partial deserialization and may be too heavyweight for this
    // method.
    for (int argument_idx = 0;
         argument_idx < proto.aggregates(i).argument_size();
         ++argument_idx) {
      if (!ScalarFactory::ProtoIsValid(
              proto.aggregates(i).argument(argument_idx), database)) {
        return false;
      }
    }

    if (proto.aggregates(i).is_distinct()) {
      if (distinctify_hash_table_impl_type_index >=
              num_distinctify_hash_tables ||
          !serialization::HashTableImplType_IsValid(
              proto.distinctify_hash_table_impl_types(
                  distinctify_hash_table_impl_type_index))) {
        return false;
      }
    }
  }

  for (int i = 0; i < proto.group_by_expressions_size(); ++i) {
    if (!ScalarFactory::ProtoIsValid(proto.group_by_expressions(i), database)) {
      return false;
    }
  }

  if (proto.group_by_expressions_size() > 0) {
    if (!proto.has_hash_table_impl_type() ||
        !serialization::HashTableImplType_IsValid(
            proto.hash_table_impl_type())) {
      return false;
    }
  }

  if (proto.has_predicate()) {
    if (!PredicateFactory::ProtoIsValid(proto.predicate(), database)) {
      return false;
    }
  }

  return true;
}

void AggregationOperationState::aggregateBlock(const block_id input_block) {
  if (group_by_list_.empty()) {
    aggregateBlockSingleState(input_block);
  } else {
    aggregateBlockHashTable(input_block);
  }
}

void AggregationOperationState::finalizeAggregate(
    InsertDestination *output_destination) {
  if (group_by_list_.empty()) {
    finalizeSingleState(output_destination);
  } else {
    finalizeHashTable(output_destination);
  }
}

void AggregationOperationState::mergeSingleState(
    const std::vector<ScopedBuffer> &local_state) {
  DEBUG_ASSERT(local_state.size() == single_states_.size());
  for (std::size_t agg_idx = 0; agg_idx < handles_.size(); ++agg_idx) {
    if (!is_distinct_[agg_idx]) {
      handles_[agg_idx]->mergeStates(single_states_[agg_idx].get(),
                                     local_state[agg_idx].get());
    }
  }
}

void AggregationOperationState::aggregateBlockSingleState(
    const block_id input_block) {
  // Aggregate per-block state for each aggregate.
  std::vector<ScopedBuffer> local_state;

  BlockReference block(
      storage_manager_->getBlock(input_block, input_relation_));

  // If there is a filter predicate, 'reuse_matches' holds the set of matching
  // tuples so that it can be reused across multiple aggregates (i.e. we only
  // pay the cost of evaluating the predicate once).
  std::unique_ptr<TupleIdSequence> reuse_matches;
  for (std::size_t agg_idx = 0; agg_idx < handles_.size(); ++agg_idx) {
    const std::vector<attribute_id> *local_arguments_as_attributes = nullptr;
#ifdef QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION
    // If all arguments are attributes of the input relation, elide a copy.
    if (!arguments_as_attributes_[agg_idx].empty()) {
      local_arguments_as_attributes = &(arguments_as_attributes_[agg_idx]);
    }
#endif
    if (is_distinct_[agg_idx]) {
      // Call StorageBlock::aggregateDistinct() to put the arguments as keys
      // directly into the (threadsafe) shared global distinctify HashTable
      // for this aggregate.
      // TODO(jianqiao): handle DISTINCT aggregation.
      local_state.emplace_back(nullptr);
    } else {
      // Call StorageBlock::aggregate() to actually do the aggregation.
      local_state.emplace_back(block->aggregate(*handles_[agg_idx],
                                                arguments_[agg_idx],
                                                local_arguments_as_attributes,
                                                predicate_.get(),
                                                &reuse_matches));
    }
  }

  // Merge per-block aggregation states back with global state.
  mergeSingleState(local_state);
}

void AggregationOperationState::aggregateBlockHashTable(
    const block_id input_block) {
  BlockReference block(
      storage_manager_->getBlock(input_block, input_relation_));

//  for (std::size_t agg_idx = 0; agg_idx < handles_.size(); ++agg_idx) {
//    if (is_distinct_[agg_idx]) {
//      // Call StorageBlock::aggregateDistinct() to insert the GROUP BY expression
//      // values and the aggregation arguments together as keys directly into the
//      // (threadsafe) shared global distinctify HashTable for this aggregate.
//      // TODO(jianqiao): handle DISTINCT aggregation.
//    }
//  }

  // Call StorageBlock::aggregateGroupBy() to aggregate this block's values
  // directly into the (threadsafe) shared global HashTable for this
  // aggregate.
  DCHECK(group_by_hashtable_pool_ != nullptr);
  auto *agg_hash_table = group_by_hashtable_pool_->getHashTable();
  DCHECK(agg_hash_table != nullptr);

  block->aggregateGroupBy(arguments_,
                          group_by_attribute_ids_,
                          predicate_.get(),
                          agg_hash_table);

  group_by_hashtable_pool_->returnHashTable(agg_hash_table);
}

void AggregationOperationState::finalizeSingleState(
    InsertDestination *output_destination) {
  // Simply build up a Tuple from the finalized values for each aggregate and
  // insert it in '*output_destination'.
  std::vector<TypedValue> attribute_values;

  for (std::size_t agg_idx = 0; agg_idx < handles_.size(); ++agg_idx) {
    if (is_distinct_[agg_idx]) {
      // TODO(jianqiao): handle DISTINCT aggregation
    }

    attribute_values.emplace_back(
        handles_[agg_idx]->finalize(single_states_[agg_idx].get()));
  }

  output_destination->insertTuple(Tuple(std::move(attribute_values)));
}

void AggregationOperationState::mergeGroupByHashTables(
    AggregationStateHashTableBase *destination_hash_table,
    const AggregationStateHashTableBase *source_hash_table) {
  static_cast<ThreadPrivateAggregationStateHashTable *>(
      destination_hash_table)->mergeHashTable(
          static_cast<const ThreadPrivateAggregationStateHashTable *>(
              source_hash_table));
}

void AggregationOperationState::finalizeHashTable(
    InsertDestination *output_destination) {
  // Each element of 'group_by_keys' is a vector of values for a particular
  // group (which is also the prefix of the finalized Tuple for that group).
  std::vector<std::vector<TypedValue>> group_by_keys;

  // TODO(harshad) - The merge phase may be slower when each hash table contains
  // large number of entries. We should find ways in which we can perform a
  // parallel merge.

  // TODO(harshad) - Find heuristics for faster merge, even in a single thread.
  // e.g. Keep merging entries from smaller hash tables to larger.

  auto *hash_tables = group_by_hashtable_pool_->getAllHashTables();
  if (hash_tables->size() == 0) {
    return;
  }

  std::unique_ptr<AggregationStateHashTableBase> final_hash_table(
      hash_tables->back().release());
  for (std::size_t i = 0; i < hash_tables->size() - 1; ++i) {
    std::unique_ptr<AggregationStateHashTableBase> hash_table(
        hash_tables->at(i).release());
    mergeGroupByHashTables(final_hash_table.get(), hash_table.get());
  }

//  static_cast<ThreadPrivateAggregationStateHashTable *>(
//      final_hash_table.get())->print();

  // Bulk-insert the complete result.
  std::unique_ptr<AggregationResultIterator> results(
      final_hash_table->createResultIterator());
  output_destination->bulkInsertAggregationResults(results.get());
}

}  // namespace quickstep
