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

#include "storage/CollisionFreeAggregationStateHashTable.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <vector>

#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageManager.hpp"
#include "storage/ValueAccessor.hpp"
#include "storage/ValueAccessorUtil.hpp"
#include "types/containers/ColumnVectorsValueAccessor.hpp"

namespace quickstep {

CollisionFreeAggregationStateHashTable::CollisionFreeAggregationStateHashTable(
    const std::vector<const Type *> &key_types,
    const std::size_t num_entries,
    const std::vector<AggregationHandle *> &handles,
    StorageManager *storage_manager)
    : key_type_(key_types.front()),
      num_entries_(num_entries),
      num_handles_(handles.size()),
      handles_(handles),
      num_finalize_partitions_(std::min((num_entries_ >> 12u) + 1u, 80uL)),
      storage_manager_(storage_manager) {
  CHECK_EQ(1u, key_types.size());
  DCHECK_GT(num_entries, 0u);

  std::map<std::string, std::size_t> memory_offsets;
  std::size_t required_memory = 0;

  memory_offsets.emplace("existence_map", required_memory);
  required_memory +=
      CacheLineAlignedBytes(ConcurrentBitVector::BytesNeeded(num_entries));

  for (std::size_t i = 0; i < num_handles_; ++i) {
    const AggregationHandle *handle = handles_[i];
    const std::vector<const Type *> argument_types = handle->getArgumentTypes();

    std::size_t state_size = 0;
    switch (handle->getAggregationID()) {
      case AggregationID::kCount: {
        state_size = sizeof(std::atomic<std::size_t>);
        break;
      }
      case AggregationID::kSum: {
        CHECK_EQ(1u, argument_types.size());
        switch (argument_types.front()->getTypeID()) {
          case TypeID::kInt:  // Fall through
          case TypeID::kLong:
            state_size = sizeof(std::atomic<std::int64_t>);
            break;
          case TypeID::kFloat:  // Fall through
          case TypeID::kDouble:
            state_size = sizeof(std::atomic<double>);
            break;
          default:
            LOG(FATAL) << "Not implemented";
        }
        break;
      }
      default:
        LOG(FATAL) << "Not implemented";
    }

    memory_offsets.emplace(std::string("state") + std::to_string(i),
                           required_memory);
    required_memory += CacheLineAlignedBytes(state_size * num_entries);
  }

  const std::size_t num_storage_slots =
      storage_manager_->SlotsNeededForBytes(required_memory);

  const block_id blob_id = storage_manager_->createBlob(num_storage_slots);
  blob_ = storage_manager_->getBlobMutable(blob_id);

  void *memory_start = blob_->getMemoryMutable();
  existence_map_.reset(new ConcurrentBitVector(
      reinterpret_cast<char *>(memory_start) + memory_offsets.at("existence_map"),
      num_entries));

  for (std::size_t i = 0; i < num_handles_; ++i) {
    vec_tables_.emplace_back(
        reinterpret_cast<char *>(memory_start) +
            memory_offsets.at(std::string("state") + std::to_string(i)));
  }

  memory_size_ = required_memory;
  num_init_partitions_ = std::min(memory_size_ / (4uL * 1024 * 1024), 80uL);
}

CollisionFreeAggregationStateHashTable::~CollisionFreeAggregationStateHashTable() {
  const block_id blob_id = blob_->getID();
  blob_.release();
  storage_manager_->deleteBlockOrBlobFile(blob_id);
}

void CollisionFreeAggregationStateHashTable::destroyPayload() {
}

bool CollisionFreeAggregationStateHashTable::upsertValueAccessor(
    const std::vector<std::vector<attribute_id>> &argument_ids,
    const std::vector<attribute_id> &key_attr_ids,
    ValueAccessor *base_accessor,
    ColumnVectorsValueAccessor *aux_accessor) {
  DCHECK_EQ(1u, key_attr_ids.size());

  const attribute_id key_attr_id = key_attr_ids.front();
  const bool is_key_nullable = key_type_->isNullable();

  // TODO: aux_accessor
  CHECK_GE(key_attr_id, 0);

  for (std::size_t i = 0; i < num_handles_; ++i) {
    DCHECK_LE(argument_ids[i].size(), 1u);

    const attribute_id argument_id =
        argument_ids[i].empty() ? kInvalidAttributeID : argument_ids[i].front();

    // TODO: aux_accessor
    CHECK_GE(argument_id, 0u);

    const AggregationHandle *handle = handles_[i];
    const auto &argument_types = handle->getArgumentTypes();

    const Type *argument_type;
    bool is_argument_nullable;
    if (argument_types.empty()) {
      argument_type = nullptr;
      is_argument_nullable = false;
    } else {
      argument_type = argument_types.front();
      is_argument_nullable = argument_type->isNullable();
    }

    // TODO: aux_accessor
    InvokeOnValueAccessorMaybeTupleIdSequenceAdapter(
        base_accessor,
        [&](auto *accessor) -> void {  // NOLINT(build/c++11)
      upsertValueAccessorDispatchHelper(is_key_nullable,
                                        is_argument_nullable,
                                        key_type_,
                                        argument_type,
                                        handle->getAggregationID(),
                                        key_attr_id,
                                        argument_id,
                                        vec_tables_[i],
                                        accessor);
    });
  }
  return true;
}

void CollisionFreeAggregationStateHashTable::finalizeKey(
    const std::size_t partition_id,
    NativeColumnVector *output_cv) const {
  const std::size_t start_position =
      calculatePartitionStartPosition(partition_id);
  const std::size_t end_position =
      calculatePartitionEndPosition(partition_id);

  switch (key_type_->getTypeID()) {
    case TypeID::kInt:
      finalizeKeyInternal<int>(start_position, end_position, output_cv);
      return;
    case TypeID::kLong:
      finalizeKeyInternal<std::int64_t>(start_position, end_position, output_cv);
      return;
    default:
      LOG(FATAL) << "Not supported";
  }
}

void CollisionFreeAggregationStateHashTable::finalizeState(
    const std::size_t partition_id,
    std::size_t handle_id,
    NativeColumnVector *output_cv) const {
  const std::size_t start_position =
      calculatePartitionStartPosition(partition_id);
  const std::size_t end_position =
      calculatePartitionEndPosition(partition_id);

  const AggregationHandle *handle = handles_[handle_id];
  const auto &argument_types = handle->getArgumentTypes();
  const Type *argument_type =
      argument_types.empty() ? nullptr : argument_types.front();

  finalizeStateDispatchHelper(handle->getAggregationID(),
                              argument_type,
                              vec_tables_[handle_id],
                              start_position,
                              end_position,
                              output_cv);
}

}  // namespace quickstep
