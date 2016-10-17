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

#ifndef QUICKSTEP_STORAGE_AGGREGATION_RESULT_ITERATOR_HPP_
#define QUICKSTEP_STORAGE_AGGREGATION_RESULT_ITERATOR_HPP_

#include <cstddef>
#include <vector>

#include "storage/AggregationStateManager.hpp"
#include "storage/HashTableUntypedKeyManager.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Storage
 *  @{
 */

class AggregationResultIterator {
 public:
  AggregationResultIterator(const void *buckets,
                            const std::size_t bucket_size,
                            const std::size_t num_entries,
                            const HashTableUntypedKeyManager &key_manager,
                            const AggregationStateManager<false> &state_manager)
      : buckets_(buckets),
        bucket_size_(bucket_size),
        num_entries_(num_entries),
        key_manager_(key_manager),
        state_manager_(state_manager) {}

  inline std::size_t getKeySize() const {
    return key_manager_.getFixedKeySize();
  }

  inline std::size_t getResultsSize() const {
    return state_manager_.getResultsSizeInBytes();
  }

  inline void beginIteration() {
    current_position_ = std::numeric_limits<std::size_t>::max();
  }

  inline bool iterationFinished() const {
    return current_position_ + 1 >= num_entries_;
  }

  inline bool next() {
    ++current_position_;
    return current_position_ < num_entries_;
  }

  inline void previous() {
    --current_position_;
  }

  inline void writeKeyTo(void *destination) const {
    key_manager_.copyUntypedKey(
        destination,
        key_manager_.getUntypedKeyComponent(getCurrentBucket()));
  }

  inline void writeResultsTo(void *destination) const {
    state_manager_.finalizeStates(destination, getCurrentBucket());
  }

 private:
  inline const void* getCurrentBucket() const {
    return static_cast<const char *>(buckets_) + current_position_ * bucket_size_;
  }

  friend class ThreadPrivateAggregationStateHashTable;

  std::size_t current_position_;

  const void *buckets_;
  const std::size_t bucket_size_;
  const std::size_t num_entries_;
  const HashTableUntypedKeyManager &key_manager_;
  const AggregationStateManager<false> &state_manager_;

  DISALLOW_COPY_AND_ASSIGN(AggregationResultIterator);
};

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_AGGREGATION_RESULT_ITERATOR_HPP_
