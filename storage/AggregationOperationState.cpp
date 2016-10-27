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
#include "expressions/aggregation/AggregationHandleDistinct.hpp"
#include "expressions/aggregation/AggregationID.hpp"
#include "expressions/predicate/Predicate.hpp"
#include "expressions/scalar/Scalar.hpp"
#include "storage/AggregationOperationState.pb.h"
#include "storage/HashTableBase.hpp"
#include "storage/InsertDestination.hpp"
#include "storage/PackedPayloadAggregationStateHashTable.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageManager.hpp"
#include "storage/SubBlocksReference.hpp"
#include "storage/TupleIdSequence.hpp"
#include "storage/ValueAccessor.hpp"
#include "types/TypedValue.hpp"
#include "types/containers/ColumnVector.hpp"
#include "types/containers/ColumnVectorsValueAccessor.hpp"
#include "types/containers/Tuple.hpp"
#include "utility/lip_filter/LIPFilterAdaptiveProber.hpp"

#include "glog/logging.h"

using std::unique_ptr;

namespace quickstep {

DEFINE_int32(num_aggregation_partitions,
             41,
             "The number of partitions used for performing the aggregation");
DEFINE_int32(partition_aggregation_num_groups_threshold,
             500000,
             "The threshold used for deciding whether the aggregation is done "
             "in a partitioned way or not");

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
      is_aggregate_partitioned_(checkAggregatePartitioned(
          estimated_num_entries, is_distinct, group_by, aggregate_functions)),
      predicate_(predicate),
      is_distinct_(std::move(is_distinct)),
      storage_manager_(storage_manager) {
  // Sanity checks: each aggregate has a corresponding list of arguments.
  DCHECK(aggregate_functions.size() == arguments.size());

  // Get the types of GROUP BY expressions for creating HashTables below.
  for (const std::unique_ptr<const Scalar> &group_by_element : group_by) {
    group_by_types_.emplace_back(&group_by_element->getType());
  }

  // Prepare group-by element attribute ids and non-trivial expressions.
  for (std::unique_ptr<const Scalar> &group_by_element : group_by) {
    const attribute_id attr_id =
        group_by_element->getAttributeIdForValueAccessor();
    if (attr_id == kInvalidAttributeID) {
      const attribute_id non_trivial_attr_id =
          -(static_cast<attribute_id>(non_trivial_expressions_.size()) + 2);
      non_trivial_expressions_.emplace_back(group_by_element.release());
      group_by_key_ids_.emplace_back(non_trivial_attr_id);
    } else {
      group_by_key_ids_.emplace_back(attr_id);
    }
  }

  std::vector<AggregationHandle *> group_by_handles;

  if (aggregate_functions.size() == 0) {
    // If there is no aggregation function, then it is a distinctify operation
    // on the group-by expressions.
    DCHECK_GT(group_by_key_ids_.size(), 0u);

    handles_.emplace_back(new AggregationHandleDistinct());
    is_distinct_.emplace_back(false);
    group_by_hashtable_pool_.reset(new HashTablePool(estimated_num_entries,
                                                     hash_table_impl_type,
                                                     group_by_types_,
                                                     handles_,
                                                     storage_manager));
  } else {
    // Set up each individual aggregate in this operation.
    std::vector<const AggregateFunction *>::const_iterator agg_func_it =
        aggregate_functions.begin();
    std::vector<std::vector<std::unique_ptr<const Scalar>>>::iterator
        args_it = arguments.begin();
    std::vector<bool>::const_iterator is_distinct_it = is_distinct_.begin();
    for (; agg_func_it != aggregate_functions.end();
         ++agg_func_it, ++args_it, ++is_distinct_it) {
      // Get the Types of this aggregate's arguments so that we can create an
      // AggregationHandle.
      std::vector<const Type *> argument_types;
      for (const std::unique_ptr<const Scalar> &argument : *args_it) {
        argument_types.emplace_back(&argument->getType());
      }

      // Prepare argument attribute ids and non-trivial expressions.
      std::vector<attribute_id> argument_ids;
      for (std::unique_ptr<const Scalar> &argument : *args_it) {
        const attribute_id attr_id =
            argument->getAttributeIdForValueAccessor();
        if (attr_id == kInvalidAttributeID) {
          const attribute_id non_trivial_attr_id =
              -(static_cast<attribute_id>(non_trivial_expressions_.size()) + 2);
          non_trivial_expressions_.emplace_back(argument.release());
          argument_ids.emplace_back(non_trivial_attr_id);
        } else {
          argument_ids.emplace_back(attr_id);
        }
      }
      argument_ids_.emplace_back(std::move(argument_ids));

      // Sanity checks: aggregate function exists and can apply to the specified
      // arguments.
      DCHECK(*agg_func_it != nullptr);
      DCHECK((*agg_func_it)->canApplyToTypes(argument_types));

      // Have the AggregateFunction create an AggregationHandle that we can use
      // to do actual aggregate computation.
      handles_.emplace_back((*agg_func_it)->createHandle(argument_types));

      if (!group_by_key_ids_.empty()) {
        // Aggregation with GROUP BY: combined payload is partially updated in
        // the presence of DISTINCT.
        if (*is_distinct_it) {
          LOG(FATAL) << "Distinct aggregation not supported";
        }
        group_by_handles.emplace_back(handles_.back());
      } else {
        // Aggregation without GROUP BY: create a single global state.
        single_states_.emplace_back(handles_.back()->createInitialState());
      }
    }

    // Aggregation with GROUP BY: create a HashTable pool.
    if (!group_by_key_ids_.empty()) {
      if (!is_aggregate_partitioned_) {
        group_by_hashtable_pool_.reset(new HashTablePool(estimated_num_entries,
                                                         hash_table_impl_type,
                                                         group_by_types_,
                                                         group_by_handles,
                                                         storage_manager));
      } else {
        partitioned_group_by_hashtable_pool_.reset(
            new PartitionedHashTablePool(estimated_num_entries,
                                         FLAGS_num_aggregation_partitions,
                                         hash_table_impl_type,
                                         group_by_types_,
                                         group_by_handles,
                                         storage_manager));
      }
    }
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

  std::unique_ptr<Predicate> predicate;
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

void AggregationOperationState::aggregateBlock(const block_id input_block,
                                               LIPFilterAdaptiveProber *lip_filter_adaptive_prober) {
  if (group_by_key_ids_.empty()) {
    aggregateBlockSingleState(input_block);
  } else {
    aggregateBlockHashTable(input_block, lip_filter_adaptive_prober);
  }
}

void AggregationOperationState::finalizeAggregate(
    InsertDestination *output_destination) {
  if (group_by_key_ids_.empty()) {
    finalizeSingleState(output_destination);
  } else {
    finalizeHashTable(output_destination);
  }
}

void AggregationOperationState::mergeSingleState(
    const std::vector<std::unique_ptr<AggregationState>> &local_state) {
  DEBUG_ASSERT(local_state.size() == single_states_.size());
  for (std::size_t agg_idx = 0; agg_idx < handles_.size(); ++agg_idx) {
    if (!is_distinct_[agg_idx]) {
      handles_[agg_idx]->mergeStates(*local_state[agg_idx],
                                     single_states_[agg_idx].get());
    }
  }
}

void AggregationOperationState::aggregateBlockSingleState(
    const block_id input_block) {
  // Aggregate per-block state for each aggregate.
  std::vector<std::unique_ptr<AggregationState>> local_state;

  BlockReference block(
      storage_manager_->getBlock(input_block, input_relation_));

  std::unique_ptr<TupleIdSequence> matches;
  if (predicate_ != nullptr) {
    matches.reset(block->getMatchesForPredicate(predicate_.get(), matches.get()));
  }

  const auto &tuple_store = block->getTupleStorageSubBlock();
  std::unique_ptr<ValueAccessor> accessor(
      tuple_store.createValueAccessor(matches.get()));

  ColumnVectorsValueAccessor non_trivial_results;
  if (!non_trivial_expressions_.empty()) {
    SubBlocksReference sub_blocks_ref(tuple_store,
                                      block->getIndices(),
                                      block->getIndicesConsistent());
    for (const auto &expression : non_trivial_expressions_) {
      non_trivial_results.addColumn(
          expression->getAllValues(accessor.get(), &sub_blocks_ref));
    }
  }

  for (std::size_t agg_idx = 0; agg_idx < handles_.size(); ++agg_idx) {
    const auto &argument_ids = argument_ids_[agg_idx];
    const auto *handle = handles_[agg_idx];

    AggregationState *state;
    if (argument_ids.empty()) {
      // Special case. This is a nullary aggregate (i.e. COUNT(*)).
      state = handle->accumulateNullary(matches == nullptr ? tuple_store.numTuples()
                                                           : matches->size());
    } else {
      // Have the AggregationHandle actually do the aggregation.
      state = handle->accumulate(accessor.get(), &non_trivial_results, argument_ids);
    }
    local_state.emplace_back(state);
  }

  // Merge per-block aggregation states back with global state.
  mergeSingleState(local_state);
}

void AggregationOperationState::aggregateBlockHashTable(
    const block_id input_block,
    LIPFilterAdaptiveProber *lip_filter_adaptive_prober) {
  BlockReference block(
      storage_manager_->getBlock(input_block, input_relation_));
  const auto &tuple_store = block->getTupleStorageSubBlock();
  std::unique_ptr<ValueAccessor> base_accessor(tuple_store.createValueAccessor());
  std::unique_ptr<ValueAccessor> shared_accessor;
  ValueAccessor *accessor = base_accessor.get();

  // Apply the predicate first, then the LIPFilters, to generate a TupleIdSequence
  // as the existence map for the tuples.
  std::unique_ptr<TupleIdSequence> matches;
  if (predicate_ != nullptr) {
    matches.reset(block->getMatchesForPredicate(predicate_.get()));
    shared_accessor.reset(
        base_accessor->createSharedTupleIdSequenceAdapterVirtual(*matches));
    accessor = shared_accessor.get();
  }
  if (lip_filter_adaptive_prober != nullptr) {
    matches.reset(lip_filter_adaptive_prober->filterValueAccessor(accessor));
    shared_accessor.reset(
        base_accessor->createSharedTupleIdSequenceAdapterVirtual(*matches));
    accessor = shared_accessor.get();
  }

  std::unique_ptr<ColumnVectorsValueAccessor> non_trivial_results;
  if (!non_trivial_expressions_.empty()) {
    non_trivial_results.reset(new ColumnVectorsValueAccessor());
    SubBlocksReference sub_blocks_ref(tuple_store,
                                      block->getIndices(),
                                      block->getIndicesConsistent());
    for (const auto &expression : non_trivial_expressions_) {
      non_trivial_results->addColumn(
          expression->getAllValues(accessor, &sub_blocks_ref));
    }
  }

  if (!is_aggregate_partitioned_) {
    DCHECK(group_by_hashtable_pool_ != nullptr);

    AggregationStateHashTableBase *agg_hash_table =
        group_by_hashtable_pool_->getHashTable();
    DCHECK(agg_hash_table != nullptr);

    std::cout << "Upsert VA start\n";
    std::flush(std::cout);
    accessor->beginIterationVirtual();
    agg_hash_table->upsertValueAccessor(argument_ids_,
                                        group_by_key_ids_,
                                        accessor,
                                        non_trivial_results.get());
    std::cout << "Aggregate done\n";
    std::flush(std::cout);
    group_by_hashtable_pool_->returnHashTable(agg_hash_table);
  } else {
    LOG(FATAL) << "Partitioned aggregation not supported";
//    const std::size_t num_partitions = partitioned_group_by_hashtable_pool_->getNumPartitions();
//
//    // Compute the partitions for the tuple formed by group by values.
//    std::vector<std::unique_ptr<TupleIdSequence>> partition_membership;
//    partition_membership.resize(num_partitions);
//
//    // Create a tuple-id sequence for each partition.
//    for (std::size_t partition = 0; partition < num_partitions; ++partition) {
//      partition_membership[partition].reset(
//          new TupleIdSequence(temp_result.getEndPosition()));
//    }
//
//    // Iterate over ValueAccessor for each tuple,
//    // set a bit in the appropriate TupleIdSequence.
//    temp_result.beginIteration();
//    while (temp_result.next()) {
//      // We need a unique_ptr because getTupleWithAttributes() uses "new".
//      std::unique_ptr<Tuple> curr_tuple(temp_result.getTupleWithAttributes(key_ids));
//      const std::size_t curr_tuple_partition_id =
//          curr_tuple->getTupleHash() % num_partitions;
//      partition_membership[curr_tuple_partition_id]->set(
//          temp_result.getCurrentPosition(), true);
//    }
//    // For each partition, create an adapter around Value Accessor and
//    // TupleIdSequence.
//    std::vector<std::unique_ptr<
//        TupleIdSequenceAdapterValueAccessor<ColumnVectorsValueAccessor>>> adapter;
//    adapter.resize(num_partitions);
//    for (std::size_t partition = 0; partition < num_partitions; ++partition) {
//      adapter[partition].reset(temp_result.createSharedTupleIdSequenceAdapter(
//          *(partition_membership)[partition]));
//      partitioned_group_by_hashtable_pool_->getHashTable(partition)
//          ->upsertValueAccessorCompositeKeyFast(
//              argument_ids, adapter[partition].get(), key_ids, true);
//    }
  }
}

void AggregationOperationState::finalizeSingleState(
    InsertDestination *output_destination) {
  // Simply build up a Tuple from the finalized values for each aggregate and
  // insert it in '*output_destination'.
  std::vector<TypedValue> attribute_values;

  for (std::size_t agg_idx = 0; agg_idx < handles_.size(); ++agg_idx) {
    attribute_values.emplace_back(
        handles_[agg_idx]->finalize(*single_states_[agg_idx]));
  }

  output_destination->insertTuple(Tuple(std::move(attribute_values)));
}

void AggregationOperationState::mergeGroupByHashTables(
    AggregationStateHashTableBase *src, AggregationStateHashTableBase *dst) {
  HashTableMergerFast merger(dst);
  static_cast<PackedPayloadSeparateChainingAggregationStateHashTable *>(src)
      ->forEach(&merger);
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
  DCHECK(hash_tables != nullptr);
  if (hash_tables->empty()) {
    return;
  }

  std::unique_ptr<AggregationStateHashTableBase> final_hash_table(
      hash_tables->back().release());
  for (std::size_t i = 0; i < hash_tables->size() - 1; ++i) {
    std::unique_ptr<AggregationStateHashTableBase> hash_table(
        hash_tables->at(i).release());
    mergeGroupByHashTables(hash_table.get(), final_hash_table.get());
    hash_table->destroyPayload();
  }

  // Collect per-aggregate finalized values.
  std::vector<std::unique_ptr<ColumnVector>> final_values;
  for (std::size_t agg_idx = 0; agg_idx < handles_.size(); ++agg_idx) {
    ColumnVector *agg_result_col = handles_[agg_idx]->finalizeHashTable(
        *final_hash_table, &group_by_keys, agg_idx);
    if (agg_result_col != nullptr) {
      final_values.emplace_back(agg_result_col);
    }
  }
  final_hash_table->destroyPayload();

  // Reorganize 'group_by_keys' in column-major order so that we can make a
  // ColumnVectorsValueAccessor to bulk-insert results.
  //
  // TODO(chasseur): Shuffling around the GROUP BY keys like this is suboptimal
  // if there is only one aggregate. The need to do this should hopefully go
  // away when we work out storing composite structures for multiple aggregates
  // in a single HashTable.
  std::vector<std::unique_ptr<ColumnVector>> group_by_cvs;
  std::size_t group_by_element_idx = 0;
  for (const Type *group_by_type : group_by_types_) {
    if (NativeColumnVector::UsableForType(*group_by_type)) {
      NativeColumnVector *element_cv =
          new NativeColumnVector(*group_by_type, group_by_keys.size());
      group_by_cvs.emplace_back(element_cv);
      for (std::vector<TypedValue> &group_key : group_by_keys) {
        element_cv->appendTypedValue(
            std::move(group_key[group_by_element_idx]));
      }
    } else {
      IndirectColumnVector *element_cv =
          new IndirectColumnVector(*group_by_type, group_by_keys.size());
      group_by_cvs.emplace_back(element_cv);
      for (std::vector<TypedValue> &group_key : group_by_keys) {
        element_cv->appendTypedValue(
            std::move(group_key[group_by_element_idx]));
      }
    }
    ++group_by_element_idx;
  }

  // Stitch together a ColumnVectorsValueAccessor combining the GROUP BY keys
  // and the finalized aggregates.
  ColumnVectorsValueAccessor complete_result;
  for (std::unique_ptr<ColumnVector> &group_by_cv : group_by_cvs) {
    complete_result.addColumn(group_by_cv.release());
  }
  for (std::unique_ptr<ColumnVector> &final_value_cv : final_values) {
    complete_result.addColumn(final_value_cv.release());
  }

  // Bulk-insert the complete result.
  output_destination->bulkInsertTuples(&complete_result);
}

void AggregationOperationState::finalizeAggregatePartitioned(
    const std::size_t partition_id, InsertDestination *output_destination) {
  // Each element of 'group_by_keys' is a vector of values for a particular
  // group (which is also the prefix of the finalized Tuple for that group).
  std::vector<std::vector<TypedValue>> group_by_keys;

  // Collect per-aggregate finalized values.
  std::vector<std::unique_ptr<ColumnVector>> final_values;
  AggregationStateHashTableBase *hash_table =
      partitioned_group_by_hashtable_pool_->getHashTable(partition_id);
  for (std::size_t agg_idx = 0; agg_idx < handles_.size(); ++agg_idx) {
    ColumnVector *agg_result_col = handles_[agg_idx]->finalizeHashTable(
        *hash_table, &group_by_keys, agg_idx);
    if (agg_result_col != nullptr) {
      final_values.emplace_back(agg_result_col);
    }
  }
  hash_table->destroyPayload();

  // Reorganize 'group_by_keys' in column-major order so that we can make a
  // ColumnVectorsValueAccessor to bulk-insert results.
  //
  // TODO(chasseur): Shuffling around the GROUP BY keys like this is suboptimal
  // if there is only one aggregate. The need to do this should hopefully go
  // away when we work out storing composite structures for multiple aggregates
  // in a single HashTable.
  std::vector<std::unique_ptr<ColumnVector>> group_by_cvs;
  std::size_t group_by_element_idx = 0;
  for (const Type *group_by_type : group_by_types_) {
    if (NativeColumnVector::UsableForType(*group_by_type)) {
      NativeColumnVector *element_cv =
          new NativeColumnVector(*group_by_type, group_by_keys.size());
      group_by_cvs.emplace_back(element_cv);
      for (std::vector<TypedValue> &group_key : group_by_keys) {
        element_cv->appendTypedValue(std::move(group_key[group_by_element_idx]));
      }
    } else {
      IndirectColumnVector *element_cv =
          new IndirectColumnVector(*group_by_type, group_by_keys.size());
      group_by_cvs.emplace_back(element_cv);
      for (std::vector<TypedValue> &group_key : group_by_keys) {
        element_cv->appendTypedValue(std::move(group_key[group_by_element_idx]));
      }
    }
    ++group_by_element_idx;
  }

  // Stitch together a ColumnVectorsValueAccessor combining the GROUP BY keys
  // and the finalized aggregates.
  ColumnVectorsValueAccessor complete_result;
  for (std::unique_ptr<ColumnVector> &group_by_cv : group_by_cvs) {
    complete_result.addColumn(group_by_cv.release());
  }
  for (std::unique_ptr<ColumnVector> &final_value_cv : final_values) {
    complete_result.addColumn(final_value_cv.release());
  }

  // Bulk-insert the complete result.
  output_destination->bulkInsertTuples(&complete_result);
}

}  // namespace quickstep
