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

#ifndef QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_CONCRETE_HANDLE_HPP_
#define QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_CONCRETE_HANDLE_HPP_

#include <cstddef>
#include <utility>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "expressions/aggregation/AggregationHandle.hpp"
#include "expressions/aggregation/AggregationID.hpp"
#include "storage/HashTableBase.hpp"
#include "threading/SpinMutex.hpp"
#include "types/TypedValue.hpp"
#include "types/containers/ColumnVector.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

class StorageManager;
class Type;
class ValueAccessor;

/** \addtogroup Expressions
 *  @{
 */

/**
 * @brief An upserter class for modifying the destination hash table while
 *        merging two group by hash tables.
 **/
template <typename HandleT>
class HashTableStateUpserter {
 public:
  /**
   * @brief Constructor.
   *
   * @param handle The aggregation handle being used.
   * @param source_state The aggregation state in the source aggregation hash
   *        table. The corresponding state (for the same key) in the destination
   *        hash table will be upserted.
   **/
  HashTableStateUpserter(const HandleT &handle,
                             const std::uint8_t *source_state)
      : handle_(handle), source_state_(source_state) {}

  /**
   * @brief The operator for the functor required for the upsert.
   *
   * @param destination_state The aggregation state in the aggregation hash
   *        table that is being upserted.
   **/
  void operator()(std::uint8_t *destination_state) {
    handle_.mergeStates(source_state_, destination_state);
  }

 private:
  const HandleT &handle_;
  const std::uint8_t *source_state_;

  DISALLOW_COPY_AND_ASSIGN(HashTableStateUpserter);
};

/**
 * @brief The helper intermediate subclass of AggregationHandle that provides
 *        virtual method implementations as well as helper methods that are
 *        shared among all its subclasses.
 *
 * @note The reason that we have this intermediate class instead of putting
 *       everything inside AggregationHandle is to avoid cyclic dependency, e.g.
 *       when HashTable has to be used.
 **/
class AggregationConcreteHandle : public AggregationHandle {
 public:
  /**
   * @brief Default implementaion for AggregationHandle::accumulateNullary().
   */
  AggregationState* accumulateNullary(
      const std::size_t num_tuples) const override {
    LOG(FATAL) << "Called accumulateNullary on an AggregationHandle that "
               << "takes at least one argument.";
  }

 protected:
  AggregationConcreteHandle(const AggregationID agg_id)
      : AggregationHandle(agg_id) {}

  template <typename HandleT, typename HashTableT>
  ColumnVector* finalizeHashTableHelper(
      const Type &result_type,
      const AggregationStateHashTableBase &hash_table,
      std::vector<std::vector<TypedValue>> *group_by_keys,
      int index) const;

  template <typename HandleT, typename HashTableT>
  inline TypedValue finalizeGroupInHashTable(
      const AggregationStateHashTableBase &hash_table,
      const std::vector<TypedValue> &group_key,
      int index) const {
    const std::uint8_t *group_state =
        static_cast<const HashTableT &>(hash_table).getSingleCompositeKey(group_key, index);
    DCHECK(group_state != nullptr)
        << "Could not find entry for specified group_key in HashTable";
    return static_cast<const HandleT *>(this)->finalizeHashTableEntry(
        group_state);
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(AggregationConcreteHandle);
};

/**
 * @brief Templated helper class used to implement
 *        AggregationHandle::finalizeHashTable() by visiting each entry (i.e.
 *        GROUP) in a HashTable, finalizing the aggregation for the GROUP, and
 *        collecting the GROUP BY key values and the final aggregate values in
 *        a ColumnVector.
 **/
template <typename HandleT, typename ColumnVectorT>
class HashTableAggregateFinalizer {
 public:
  HashTableAggregateFinalizer(
      const HandleT &handle,
      std::vector<std::vector<TypedValue>> *group_by_keys,
      ColumnVectorT *output_column_vector)
      : handle_(handle),
        group_by_keys_(group_by_keys),
        output_column_vector_(output_column_vector) {}

  inline void operator()(const std::vector<TypedValue> &group_by_key,
                         const AggregationState &group_state) {
    group_by_keys_->emplace_back(group_by_key);
    output_column_vector_->appendTypedValue(
        handle_.finalizeHashTableEntry(group_state));
  }

  inline void operator()(const std::vector<TypedValue> &group_by_key,
                         const unsigned char *byte_ptr) {
    group_by_keys_->emplace_back(group_by_key);
    output_column_vector_->appendTypedValue(
        handle_.finalizeHashTableEntry(byte_ptr));
  }

 private:
  const HandleT &handle_;
  std::vector<std::vector<TypedValue>> *group_by_keys_;
  ColumnVectorT *output_column_vector_;
};

/** @} */

// ----------------------------------------------------------------------------
// Implementations of templated methods follow:

template <typename HandleT, typename HashTableT>
ColumnVector* AggregationConcreteHandle::finalizeHashTableHelper(
    const Type &result_type,
    const AggregationStateHashTableBase &hash_table,
    std::vector<std::vector<TypedValue>> *group_by_keys,
    int index) const {
  const HandleT &handle = static_cast<const HandleT &>(*this);
  const HashTableT &hash_table_concrete =
      static_cast<const HashTableT &>(hash_table);

  if (group_by_keys->empty()) {
    if (NativeColumnVector::UsableForType(result_type)) {
      NativeColumnVector *result =
          new NativeColumnVector(result_type, hash_table_concrete.numEntries());
      HashTableAggregateFinalizer<HandleT, NativeColumnVector> finalizer(
          handle, group_by_keys, result);
      hash_table_concrete.forEach(&finalizer, index);
      return result;
    } else {
      IndirectColumnVector *result = new IndirectColumnVector(
          result_type, hash_table_concrete.numEntries());
      HashTableAggregateFinalizer<HandleT, IndirectColumnVector> finalizer(
          handle, group_by_keys, result);
      hash_table_concrete.forEach(&finalizer, index);
      return result;
    }
  } else {
    if (NativeColumnVector::UsableForType(result_type)) {
      NativeColumnVector *result =
          new NativeColumnVector(result_type, group_by_keys->size());
      for (const std::vector<TypedValue> &group_by_key : *group_by_keys) {
        result->appendTypedValue(
            finalizeGroupInHashTable<HandleT, HashTableT>(
                hash_table, group_by_key, index));
      }
      return result;
    } else {
      IndirectColumnVector *result = new IndirectColumnVector(
          result_type, hash_table_concrete.numEntries());
      for (const std::vector<TypedValue> &group_by_key : *group_by_keys) {
        result->appendTypedValue(
            finalizeGroupInHashTable<HandleT, HashTableT>(
                hash_table, group_by_key, index));
      }
      return result;
    }
  }
}

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_CONCRETE_HANDLE_HPP_
