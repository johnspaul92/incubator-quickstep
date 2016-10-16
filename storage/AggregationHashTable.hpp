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

#ifndef QUICKSTEP_STORAGE_AGGREGATION_HASH_TABLE_HPP_
#define QUICKSTEP_STORAGE_AGGREGATION_HASH_TABLE_HPP_

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "expressions/aggregation/AggregationHandle.hpp"
#include "storage/HashTableBase.hpp"
#include "storage/HashTableUntypedKeyManager.hpp"
#include "storage/StorageBlob.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageConstants.hpp"
#include "storage/StorageManager.hpp"
#include "storage/ValueAccessor.hpp"
#include "storage/ValueAccessorUtil.hpp"
#include "threading/SpinMutex.hpp"
#include "threading/SpinSharedMutex.hpp"
#include "types/Type.hpp"
#include "types/TypeFunctors.hpp"
#include "utility/Alignment.hpp"
#include "utility/InlineMemcpy.hpp"
#include "utility/Macros.hpp"
#include "utility/PrimeNumber.hpp"

namespace quickstep {

/** \addtogroup Storage
 *  @{
 */

template <bool use_mutex>
class AggregationHashTablePayloadManager {
 public:
  AggregationHashTablePayloadManager(const std::vector<AggregationHandle *> &handles)
      : handles_(handles),
        payload_size_in_bytes_(0) {
    if (use_mutex) {
      payload_size_in_bytes_ += sizeof(SpinMutex);
    }
    for (const AggregationHandle *handle : handles) {
      const std::size_t state_size = handle->getStateSize();
      agg_state_sizes_.emplace_back(state_size);
      agg_state_offsets_.emplace_back(payload_size_in_bytes_);
      payload_size_in_bytes_ += state_size;
    }

    initial_payload_ = std::malloc(payload_size_in_bytes_);
    if (use_mutex) {
      new(initial_payload_) Mutex;
    }
//    for (std::size_t i = 0; i < handles_.size(); ++i) {
//      handles_[i]->initPayload(
//          static_cast<std::uint8_t *>(initial_payload_) + agg_state_offsets_[i]);
//    }
  }

  ~AggregationHashTablePayloadManager() {
    std::free(initial_payload_);
  }

  inline std::size_t getPayloadSizeInBytes() const {
    return payload_size_in_bytes_;
  }

  inline void updatePayload(void *payload) const {
  }

  inline void initPayload(void *payload) const {
  }

 private:
  std::vector<AggregationHandle *> handles_;

  std::vector<std::size_t> agg_state_sizes_;
  std::vector<std::size_t> agg_state_offsets_;
  std::size_t payload_size_in_bytes_;

  void *initial_payload_;

  DISALLOW_COPY_AND_ASSIGN(AggregationHashTablePayloadManager);
};

class ThreadPrivateAggregationHashTable : public AggregationHashTableBase {
 public:
  ThreadPrivateAggregationHashTable(const std::vector<const Type *> &key_types,
                                    const std::size_t num_entries,
                                    const std::vector<AggregationHandle *> &handles,
                                    StorageManager *storage_manager)
    : payload_manager_(handles),
      key_types_(key_types),
      key_manager_(this->key_types_, payload_manager_.getPayloadSizeInBytes()),
      slots_(num_entries * kHashTableLoadFactor,
             key_manager_.getUntypedKeyHashFunctor(),
             key_manager_.getUntypedKeyEqualityFunctor()),
      bucket_size_(ComputeBucketSize(key_manager_.getFixedKeySize(),
                                     payload_manager_.getPayloadSizeInBytes())),
      buckets_allocated_(0),
      storage_manager_(storage_manager) {
    std::size_t num_storage_slots =
        this->storage_manager_->SlotsNeededForBytes(num_entries);

    // Get a StorageBlob to hold the hash table.
    const block_id blob_id = this->storage_manager_->createBlob(num_storage_slots);
    this->blob_ = this->storage_manager_->getBlobMutable(blob_id);

    buckets_ = this->blob_->getMemoryMutable();
    num_buckets_ = num_storage_slots * kSlotSizeBytes / bucket_size_;
  }

  void resize() {
    const std::size_t resized_memory_required = num_buckets_ * bucket_size_ * 2;
    const std::size_t resized_storage_slots =
        this->storage_manager_->SlotsNeededForBytes(resized_memory_required);
    const block_id resized_blob_id =
        this->storage_manager_->createBlob(resized_storage_slots);
    MutableBlobReference resized_blob =
        this->storage_manager_->getBlobMutable(resized_blob_id);

    void *resized_buckets = resized_blob->getMemoryMutable();
    std::memcpy(resized_buckets, buckets_, buckets_allocated_ * bucket_size_);

    for (auto &pair : slots_) {
      pair.second =
           (static_cast<const char *>(pair.first) - static_cast<char *>(buckets_))
           + static_cast<char *>(resized_buckets);
    }

    buckets_ = resized_buckets;
    num_buckets_ = resized_storage_slots * kSlotSizeBytes / bucket_size_;
    std::swap(this->blob_, resized_blob);
  }

  bool upsertValueAccessor(ValueAccessor *accessor,
                           const attribute_id key_attr_id,
                           const std::vector<attribute_id> &argument_ids,
                           const bool check_for_null_keys) override {
    if (check_for_null_keys) {
      return upsertValueAccessorInternal<true>(
          accessor, key_attr_id, argument_ids);
    } else {
      return upsertValueAccessorInternal<false>(
          accessor, key_attr_id, argument_ids);
    }
  }

  template <bool check_for_null_keys>
  bool upsertValueAccessorInternal(ValueAccessor *accessor,
                                   const attribute_id key_attr_id,
                                   const std::vector<attribute_id> &argument_ids) {
    return InvokeOnAnyValueAccessor(
        accessor,
        [&](auto *accessor) -> bool {  // NOLINT(build/c++11)
      accessor->beginIteration();
      while (accessor->next()) {
        const void *key = accessor->template getUntypedValue<check_for_null_keys>(key_attr_id);
        if (check_for_null_keys && key == nullptr) {
          continue;
        }
        bool is_empty;
        void *bucket = locateBucket(key, &is_empty);
        if (is_empty) {
          payload_manager_.initPayload(bucket);
        } else {
          payload_manager_.updatePayload(bucket);
        }
      }
      return true;
    });
  }

  bool upsertValueAccessorCompositeKey(ValueAccessor *accessor,
                                       const std::vector<attribute_id> &key_attr_ids,
                                       const std::vector<attribute_id> &argument_ids,
                                       const bool check_for_null_keys) override {
    if (check_for_null_keys) {
      return upsertValueAccessorCompositeKeyInternal<true>(
          accessor, key_attr_ids, argument_ids);
    } else {
      return upsertValueAccessorCompositeKeyInternal<false>(
          accessor, key_attr_ids, argument_ids);
    }
  }

  template <bool check_for_null_keys>
  bool upsertValueAccessorCompositeKeyInternal(ValueAccessor *accessor,
                                               const std::vector<attribute_id> &key_attr_ids,
                                               const std::vector<attribute_id> &argument_ids) {
    return InvokeOnAnyValueAccessor(
        accessor,
        [&](auto *accessor) -> bool {  // NOLINT(build/c++11)
      accessor->beginIteration();
      void *prealloc_bucket = allocateBucket();
      while (accessor->next()) {
        if (check_for_null_keys) {
          const bool is_null =
              key_manager_.writeNullableUntypedKeyFromValueAccessorToBucket(
                  accessor,
                  key_attr_ids,
                  prealloc_bucket);
          if (is_null) {
            continue;
          }
        } else {
          key_manager_.writeUntypedKeyFromValueAccessorToBucket(
              accessor,
              key_attr_ids,
              prealloc_bucket);
        }
        void *bucket = locateBucketWithPrealloc(prealloc_bucket);
        if (bucket != prealloc_bucket) {
          payload_manager_.initPayload(bucket);
          prealloc_bucket = allocateBucket();
        } else {
          payload_manager_.updatePayload(bucket);
        }
      }
      // Reclaim the last unused bucket
      --buckets_allocated_;
      return true;
    });
  }

  inline void* locateBucket(const void *key, bool *is_empty) {
    auto slot_it = slots_.find(key);
    if (slot_it == slots_.end()) {
      void *bucket = allocateBucket();
      key_manager_.writeUntypedKeyToBucket(key, bucket);
      slots_.emplace(key_manager_.getUntypedKeyComponent(bucket), bucket);
      *is_empty = true;
      return bucket;
    } else {
      *is_empty = false;
      return slot_it->second;
    }
  }

  inline void* locateBucketWithPrealloc(void *prealloc_bucket) {
    const void *key = key_manager_.getUntypedKeyComponent(prealloc_bucket);
    auto slot_it = slots_.find(key);
    if (slot_it == slots_.end()) {
      slots_.emplace(key, prealloc_bucket);
      return prealloc_bucket;
    } else {
      return slot_it->second;
    }
  }

  inline void* allocateBucket() {
    if (buckets_allocated_ >= num_buckets_) {
      resize();
    }
    void *bucket = static_cast<char *>(buckets_) + buckets_allocated_ * bucket_size_;
    ++buckets_allocated_;
    return bucket;
  }

  void print() const override {
    std::cerr << "Bucket size = " << bucket_size_ << "\n";
    std::cerr << "Buckets: \n";
    for (const auto &pair : slots_) {
      std::cerr << pair.first << " -- " << pair.second << "\n";
      std::cerr << *static_cast<const int *>(pair.second) << "\n";
    }
  }

 private:
  // Helper object to manage hash table payloads (i.e. aggregation states).
  AggregationHashTablePayloadManager<false> payload_manager_;

  // Type(s) of keys.
  const std::vector<const Type*> key_types_;

  // Helper object to manage key storage.
  HashTableUntypedKeyManager key_manager_;

  // Round bucket size up to a multiple of kBucketAlignment.
  static std::size_t ComputeBucketSize(const std::size_t fixed_key_size,
                                       const std::size_t total_payload_size) {
    constexpr std::size_t kBucketAlignment = 4;
    return (((fixed_key_size + total_payload_size - 1)
               / kBucketAlignment) + 1) * kBucketAlignment;
  }

  std::unordered_map<const void *, void *,
                     UntypedKeyHashFunctor,
                     UntypedKeyEqualityFunctor> slots_;

  void *buckets_;
  const std::size_t bucket_size_;
  std::size_t num_buckets_;
  std::size_t buckets_allocated_;

  StorageManager *storage_manager_;
  MutableBlobReference blob_;

  DISALLOW_COPY_AND_ASSIGN(ThreadPrivateAggregationHashTable);
};


}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_AGGREGATION_HASH_TABLE_HPP_

