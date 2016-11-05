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

#ifndef QUICKSTEP_STORAGE_COLLISION_FREE_AGGREGATION_STATE_HASH_TABLE_HPP_
#define QUICKSTEP_STORAGE_COLLISION_FREE_AGGREGATION_STATE_HASH_TABLE_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "expressions/aggregation/AggregationHandle.hpp"
#include "expressions/aggregation/AggregationID.hpp"
#include "storage/HashTableBase.hpp"
#include "storage/StorageBlob.hpp"
#include "storage/StorageConstants.hpp"
#include "storage/ValueAccessor.hpp"
#include "types/Type.hpp"
#include "types/TypeID.hpp"
#include "types/TypedValue.hpp"
#include "types/containers/ColumnVector.hpp"
#include "utility/ConcurrentBitVector.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

class StorageMnager;

/** \addtogroup Storage
 *  @{
 */

class CollisionFreeAggregationStateHashTable
    : public AggregationStateHashTableBase {
 public:
  CollisionFreeAggregationStateHashTable(
      const std::vector<const Type *> &key_types,
      const std::size_t num_entries,
      const std::vector<AggregationHandle *> &handles,
      StorageManager *storage_manager);

  ~CollisionFreeAggregationStateHashTable() override;

  void destroyPayload() override;

  inline std::size_t getNumInitializePartitions() const {
    return num_init_partitions_;
  }

  inline std::size_t getNumFinalizePartitions() const {
    return num_finalize_partitions_;
  }

  inline std::size_t getNumTuplesInPartition(
      const std::size_t partition_id) const {
    const std::size_t start_position =
        calculatePartitionStartPosition(partition_id);
    const std::size_t end_position =
        calculatePartitionEndPosition(partition_id);
    return existence_map_->onesCount(start_position, end_position);
  }

  inline void initialize(const std::size_t partition_id) {
    const std::size_t memory_segment_size =
        (memory_size_ + num_init_partitions_ - 1) / num_init_partitions_;
    const std::size_t memory_start = memory_segment_size * partition_id;
    std::memset(reinterpret_cast<char *>(blob_->getMemoryMutable()) + memory_start,
                0,
                std::min(memory_segment_size, memory_size_ - memory_start));
  }

  bool upsertValueAccessor(
      const std::vector<std::vector<attribute_id>> &argument_ids,
      const std::vector<attribute_id> &key_attr_ids,
      ValueAccessor *base_accessor,
      ColumnVectorsValueAccessor *aux_accessor = nullptr) override;

  void finalizeKey(const std::size_t partition_id,
                   NativeColumnVector *output_cv) const;

  void finalizeState(const std::size_t partition_id,
                     std::size_t handle_id,
                     NativeColumnVector *output_cv) const;

 private:
  inline static std::size_t CacheLineAlignedBytes(const std::size_t actual_bytes) {
    return (actual_bytes + kCacheLineBytes - 1) / kCacheLineBytes * kCacheLineBytes;
  }

  inline std::size_t calculatePartitionLength() const {
    const std::size_t partition_length =
        (num_entries_ + num_finalize_partitions_ - 1) / num_finalize_partitions_;
    DCHECK_GE(partition_length, 0u);
    return partition_length;
  }

  inline std::size_t calculatePartitionStartPosition(
      const std::size_t partition_id) const {
    return calculatePartitionLength() * partition_id;
  }

  inline std::size_t calculatePartitionEndPosition(
      const std::size_t partition_id) const {
    return std::min(calculatePartitionLength() * (partition_id + 1),
                    num_entries_);
  }

  template <bool use_two_accessors, typename ...ArgTypes>
  inline void upsertValueAccessorDispatchHelper(
      const bool is_key_nullable,
      const bool is_argument_nullable,
      ArgTypes &&...args);

  template <bool ...bool_values, typename ...ArgTypes>
  inline void upsertValueAccessorDispatchHelper(
      const Type *key_type,
      ArgTypes &&...args);

  template <bool use_two_accessors, bool is_key_nullable, bool is_argument_nullable,
            typename KeyT, typename ...ArgTypes>
  inline void upsertValueAccessorDispatchHelper(
      const Type *argument_type,
      const AggregationID agg_id,
      ArgTypes &&...args);

  template <bool use_two_accessors, bool is_key_nullable, bool is_argument_nullable,
            typename KeyT, typename KeyValueAccessorT, typename ArgumentValueAccessorT>
  inline void upsertValueAccessorCountHelper(
      const attribute_id key_attr_id,
      const attribute_id argument_id,
      void *vec_table,
      KeyValueAccessorT *key_accessor,
      ArgumentValueAccessorT *argument_accessor);

  template <bool use_two_accessors, bool is_key_nullable, bool is_argument_nullable,
            typename KeyT, typename KeyValueAccessorT, typename ArgumentValueAccessorT>
  inline void upsertValueAccessorSumHelper(
      const Type *argument_type,
      const attribute_id key_attr_id,
      const attribute_id argument_id,
      void *vec_table,
      KeyValueAccessorT *key_accessor,
      ArgumentValueAccessorT *argument_accessor);

  template <bool is_key_nullable, typename KeyT, typename KeyValueAccessorT>
  inline void upsertValueAccessorCountNullary(
      const attribute_id key_attr_id,
      std::atomic<std::size_t> *vec_table,
      KeyValueAccessorT *key_accessor);

  template <bool use_two_accessors, bool is_key_nullable, typename KeyT,
            typename KeyValueAccessorT, typename ArgumentValueAccessorT>
  inline void upsertValueAccessorCountUnary(
      const attribute_id key_attr_id,
      const attribute_id argument_id,
      std::atomic<std::size_t> *vec_table,
      KeyValueAccessorT *key_accessor,
      ArgumentValueAccessorT *argument_accessor);

  template <bool use_two_accessors, bool is_key_nullable, bool is_argument_nullable,
            typename KeyT, typename ArgumentT, typename StateT,
            typename KeyValueAccessorT, typename ArgumentValueAccessorT>
  inline void upsertValueAccessorIntegerSum(
      const attribute_id key_attr_id,
      const attribute_id argument_id,
      std::atomic<StateT> *vec_table,
      KeyValueAccessorT *key_accessor,
      ArgumentValueAccessorT *argument_accessor);

  template <bool use_two_accessors, bool is_key_nullable, bool is_argument_nullable,
            typename KeyT, typename ArgumentT, typename StateT,
            typename KeyValueAccessorT, typename ArgumentValueAccessorT>
  inline void upsertValueAccessorGenericSum(
      const attribute_id key_attr_id,
      const attribute_id argument_id,
      std::atomic<StateT> *vec_table,
      KeyValueAccessorT *key_accessor,
      ArgumentValueAccessorT *argument_accessor);

  template <typename KeyT>
  inline void finalizeKeyInternal(const std::size_t start_position,
                                  const std::size_t end_position,
                                  NativeColumnVector *output_cv) const {
    std::size_t loc = start_position - 1;
    while ((loc = existence_map_->nextOne(loc)) < end_position) {
      *static_cast<KeyT *>(output_cv->getPtrForDirectWrite()) = loc;
    }
  }

  template <typename ...ArgTypes>
  inline void finalizeStateDispatchHelper(
      const AggregationID agg_id,
      const Type *argument_type,
      const void *vec_table,
      ArgTypes &&...args) const {
    switch (agg_id) {
       case AggregationID::kCount:
         finalizeStateCount(static_cast<const std::atomic<std::size_t> *>(vec_table),
                            std::forward<ArgTypes>(args)...);
         return;
       case AggregationID::kSum:
         finalizeStateSumHelper(argument_type,
                                vec_table,
                                std::forward<ArgTypes>(args)...);
         return;
       default:
         LOG(FATAL) << "Not supported";
    }
  }

  template <typename ...ArgTypes>
  inline void finalizeStateSumHelper(
      const Type *argument_type,
      const void *vec_table,
      ArgTypes &&...args) const {
    DCHECK(argument_type != nullptr);

    switch (argument_type->getTypeID()) {
      case TypeID::kInt:    // Fall through
      case TypeID::kLong:
        finalizeStateSum<std::int64_t>(
            static_cast<const std::atomic<std::int64_t> *>(vec_table),
            std::forward<ArgTypes>(args)...);
        return;
      case TypeID::kFloat:  // Fall through
      case TypeID::kDouble:
        finalizeStateSum<double>(
            static_cast<const std::atomic<double> *>(vec_table),
            std::forward<ArgTypes>(args)...);
        return;
      default:
        LOG(FATAL) << "Not supported";
    }
  }

  inline void finalizeStateCount(const std::atomic<std::size_t> *vec_table,
                                 const std::size_t start_position,
                                 const std::size_t end_position,
                                 NativeColumnVector *output_cv) const {
    std::size_t loc = start_position - 1;
    while ((loc = existence_map_->nextOne(loc)) < end_position) {
      *static_cast<std::int64_t *>(output_cv->getPtrForDirectWrite()) =
          vec_table[loc].load(std::memory_order_relaxed);
    }
  }

  template <typename ResultT, typename StateT>
  inline void finalizeStateSum(const std::atomic<StateT> *vec_table,
                               const std::size_t start_position,
                               const std::size_t end_position,
                               NativeColumnVector *output_cv) const {
    std::size_t loc = start_position - 1;
    while ((loc = existence_map_->nextOne(loc)) < end_position) {
      *static_cast<ResultT *>(output_cv->getPtrForDirectWrite()) =
          vec_table[loc].load(std::memory_order_relaxed);
    }
  }

  const Type *key_type_;
  const std::size_t num_entries_;

  const std::size_t num_handles_;
  const std::vector<AggregationHandle *> handles_;

  std::unique_ptr<ConcurrentBitVector> existence_map_;
  std::vector<void *> vec_tables_;

  const std::size_t num_finalize_partitions_;

  StorageManager *storage_manager_;
  MutableBlobReference blob_;

  std::size_t memory_size_;
  std::size_t num_init_partitions_;

  DISALLOW_COPY_AND_ASSIGN(CollisionFreeAggregationStateHashTable);
};

// ----------------------------------------------------------------------------
// Implementations of template methods follow.

template <bool use_two_accessors, typename ...ArgTypes>
inline void CollisionFreeAggregationStateHashTable
    ::upsertValueAccessorDispatchHelper(
        const bool is_key_nullable,
        const bool is_argument_nullable,
        ArgTypes &&...args) {
  if (is_key_nullable) {
    if (is_argument_nullable) {
      upsertValueAccessorDispatchHelper<use_two_accessors, true, true>(
          std::forward<ArgTypes>(args)...);
    } else {
      upsertValueAccessorDispatchHelper<use_two_accessors, true, false>(
          std::forward<ArgTypes>(args)...);
    }
  } else {
    if (is_argument_nullable) {
      upsertValueAccessorDispatchHelper<use_two_accessors, false, true>(
          std::forward<ArgTypes>(args)...);
    } else {
      upsertValueAccessorDispatchHelper<use_two_accessors, false, false>(
          std::forward<ArgTypes>(args)...);
    }
  }
}

template <bool ...bool_values, typename ...ArgTypes>
inline void CollisionFreeAggregationStateHashTable
    ::upsertValueAccessorDispatchHelper(
        const Type *key_type,
        ArgTypes &&...args) {
  switch (key_type->getTypeID()) {
    case TypeID::kInt:
      upsertValueAccessorDispatchHelper<bool_values..., int>(
          std::forward<ArgTypes>(args)...);
      return;
    case TypeID::kLong:
      upsertValueAccessorDispatchHelper<bool_values..., std::int64_t>(
          std::forward<ArgTypes>(args)...);
      return;
    default:
      LOG(FATAL) << "Not supported";
  }
}

template <bool use_two_accessors, bool is_key_nullable, bool is_argument_nullable,
          typename KeyT, typename ...ArgTypes>
inline void CollisionFreeAggregationStateHashTable
    ::upsertValueAccessorDispatchHelper(
        const Type *argument_type,
        const AggregationID agg_id,
        ArgTypes &&...args) {
  switch (agg_id) {
     case AggregationID::kCount:
       upsertValueAccessorCountHelper<
           use_two_accessors, is_key_nullable, is_argument_nullable, KeyT>(
               std::forward<ArgTypes>(args)...);
       return;
     case AggregationID::kSum:
       upsertValueAccessorSumHelper<
           use_two_accessors, is_key_nullable, is_argument_nullable, KeyT>(
               argument_type, std::forward<ArgTypes>(args)...);
       return;
     default:
       LOG(FATAL) << "Not supported";
  }
}

template <bool use_two_accessors, bool is_key_nullable, bool is_argument_nullable,
          typename KeyT, typename KeyValueAccessorT, typename ArgumentValueAccessorT>
inline void CollisionFreeAggregationStateHashTable
    ::upsertValueAccessorCountHelper(
        const attribute_id key_attr_id,
        const attribute_id argument_id,
        void *vec_table,
        KeyValueAccessorT *key_accessor,
        ArgumentValueAccessorT *argument_accessor) {
  DCHECK_GE(key_attr_id, 0u);

  if (is_argument_nullable && argument_id != kInvalidAttributeID) {
    upsertValueAccessorCountUnary<use_two_accessors, is_key_nullable, KeyT>(
        key_attr_id,
        argument_id,
        static_cast<std::atomic<std::size_t> *>(vec_table),
        key_accessor,
        argument_accessor);
    return;
  } else {
    upsertValueAccessorCountNullary<is_key_nullable, KeyT>(
        key_attr_id,
        static_cast<std::atomic<std::size_t> *>(vec_table),
        key_accessor);
    return;
  }
}

template <bool use_two_accessors, bool is_key_nullable, bool is_argument_nullable,
          typename KeyT, typename KeyValueAccessorT, typename ArgumentValueAccessorT>
inline void CollisionFreeAggregationStateHashTable
    ::upsertValueAccessorSumHelper(
        const Type *argument_type,
        const attribute_id key_attr_id,
        const attribute_id argument_id,
        void *vec_table,
        KeyValueAccessorT *key_accessor,
        ArgumentValueAccessorT *argument_accessor) {
  DCHECK_GE(key_attr_id, 0u);
  DCHECK_GE(argument_id, 0u);
  DCHECK(argument_type != nullptr);

  switch (argument_type->getTypeID()) {
    case TypeID::kInt:
      upsertValueAccessorIntegerSum<
          use_two_accessors, is_key_nullable, is_argument_nullable, KeyT, int>(
              key_attr_id,
              argument_id,
              static_cast<std::atomic<std::int64_t> *>(vec_table),
              key_accessor,
              argument_accessor);
      return;
    case TypeID::kLong:
      upsertValueAccessorIntegerSum<
          use_two_accessors, is_key_nullable, is_argument_nullable, KeyT, std::int64_t>(
              key_attr_id,
              argument_id,
              static_cast<std::atomic<std::int64_t> *>(vec_table),
              key_accessor,
              argument_accessor);
      return;
    case TypeID::kFloat:
      upsertValueAccessorGenericSum<
          use_two_accessors, is_key_nullable, is_argument_nullable, KeyT, float>(
              key_attr_id,
              argument_id,
              static_cast<std::atomic<double> *>(vec_table),
              key_accessor,
              argument_accessor);
      return;
    case TypeID::kDouble:
      upsertValueAccessorGenericSum<
          use_two_accessors, is_key_nullable, is_argument_nullable, KeyT, double>(
              key_attr_id,
              argument_id,
              static_cast<std::atomic<double> *>(vec_table),
              key_accessor,
              argument_accessor);
      return;
    default:
      LOG(FATAL) << "Not supported";
  }
}

template <bool is_key_nullable, typename KeyT, typename ValueAccessorT>
inline void CollisionFreeAggregationStateHashTable
    ::upsertValueAccessorCountNullary(
        const attribute_id key_attr_id,
        std::atomic<std::size_t> *vec_table,
        ValueAccessorT *accessor) {
  accessor->beginIteration();
  while (accessor->next()) {
    const KeyT *key = static_cast<const KeyT *>(
        accessor->template getUntypedValue<is_key_nullable>(key_attr_id));
    if (is_key_nullable && key == nullptr) {
      continue;
    }
    const std::size_t loc = *key;
    vec_table[loc].fetch_add(1u, std::memory_order_relaxed);
    existence_map_->setBit(loc);
  }
}

template <bool use_two_accessors, bool is_key_nullable, typename KeyT,
          typename KeyValueAccessorT, typename ArgumentValueAccessorT>
inline void CollisionFreeAggregationStateHashTable
    ::upsertValueAccessorCountUnary(
        const attribute_id key_attr_id,
        const attribute_id argument_id,
        std::atomic<std::size_t> *vec_table,
        KeyValueAccessorT *key_accessor,
        ArgumentValueAccessorT *argument_accessor) {
  key_accessor->beginIteration();
  while (key_accessor->next()) {
    if (use_two_accessors) {
      argument_accessor->next();
    }
    const KeyT *key = static_cast<const KeyT *>(
        key_accessor->template getUntypedValue<is_key_nullable>(key_attr_id));
    if (is_key_nullable && key == nullptr) {
      continue;
    }
    const std::size_t loc = *key;
    existence_map_->setBit(loc);
    if (argument_accessor->getUntypedValue(argument_id) == nullptr) {
      continue;
    }
    vec_table[loc].fetch_add(1u, std::memory_order_relaxed);
  }
}

template <bool use_two_accessors, bool is_key_nullable, bool is_argument_nullable,
          typename KeyT, typename ArgumentT, typename StateT,
          typename KeyValueAccessorT, typename ArgumentValueAccessorT>
inline void CollisionFreeAggregationStateHashTable
    ::upsertValueAccessorIntegerSum(
        const attribute_id key_attr_id,
        const attribute_id argument_id,
        std::atomic<StateT> *vec_table,
        KeyValueAccessorT *key_accessor,
        ArgumentValueAccessorT *argument_accessor) {
  key_accessor->beginIteration();
  while (key_accessor->next()) {
    if (use_two_accessors) {
      argument_accessor->next();
    }
    const KeyT *key = static_cast<const KeyT *>(
        key_accessor->template getUntypedValue<is_key_nullable>(key_attr_id));
    if (is_key_nullable && key == nullptr) {
      continue;
    }
    const std::size_t loc = *key;
    existence_map_->setBit(loc);
    const ArgumentT *argument = static_cast<const ArgumentT *>(
        argument_accessor->template getUntypedValue<is_argument_nullable>(argument_id));
    if (is_argument_nullable && argument == nullptr) {
      continue;
    }
    vec_table[loc].fetch_add(*argument, std::memory_order_relaxed);
  }
}

template <bool use_two_accessors, bool is_key_nullable, bool is_argument_nullable,
          typename KeyT, typename ArgumentT, typename StateT,
          typename KeyValueAccessorT, typename ArgumentValueAccessorT>
inline void CollisionFreeAggregationStateHashTable
    ::upsertValueAccessorGenericSum(
        const attribute_id key_attr_id,
        const attribute_id argument_id,
        std::atomic<StateT> *vec_table,
        KeyValueAccessorT *key_accessor,
        ArgumentValueAccessorT *argument_accessor) {
  key_accessor->beginIteration();
  while (key_accessor->next()) {
    if (use_two_accessors) {
      argument_accessor->next();
    }
    const KeyT *key = static_cast<const KeyT *>(
        key_accessor->template getUntypedValue<is_key_nullable>(key_attr_id));
    if (is_key_nullable && key == nullptr) {
      continue;
    }
    const std::size_t loc = *key;
    existence_map_->setBit(loc);
    const ArgumentT *argument = static_cast<const ArgumentT *>(
        argument_accessor->template getUntypedValue<is_argument_nullable>(argument_id));
    if (is_argument_nullable && argument == nullptr) {
      continue;
    }
    const ArgumentT arg_val = *argument;
    std::atomic<StateT> &state = vec_table[loc];
    StateT state_val = state.load(std::memory_order_relaxed);
    while(!state.compare_exchange_weak(state_val, state_val + arg_val)) {}
  }
}

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_COLLISION_FREE_AGGREGATION_STATE_HASH_TABLE_HPP_
