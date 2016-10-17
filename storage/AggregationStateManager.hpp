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

#ifndef QUICKSTEP_STORAGE_AGGREGATION_STATE_MANAGER_HPP_
#define QUICKSTEP_STORAGE_AGGREGATION_STATE_MANAGER_HPP_

#include <cstddef>
#include <cstring>
#include <vector>

#include "expressions/aggregation/AggregationHandle.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "threading/SpinMutex.hpp"
#include "threading/SpinSharedMutex.hpp"
#include "utility/InlineMemcpy.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedBuffer.hpp"

#include "glog/logging.h"

namespace quickstep {

/** \addtogroup Storage
 *  @{
 */

template <bool use_mutex>
class AggregationStateManager {
 public:
  AggregationStateManager(const std::vector<AggregationHandle *> &handles)
      : handles_(handles),
        states_size_in_bytes_(0),
        results_size_in_bytes_(0) {
    if (use_mutex) {
      states_size_in_bytes_ += sizeof(SpinMutex);
    }
    for (const AggregationHandle *handle : handles) {
      const std::size_t state_size = handle->getStateSize();
      state_sizes_.emplace_back(state_size);
      state_offsets_.emplace_back(states_size_in_bytes_);
      states_size_in_bytes_ += state_size;

      const std::size_t result_size = handle->getResultSize();
      result_sizes_.emplace_back(result_size);
      result_offsets_.emplace_back(results_size_in_bytes_);
      results_size_in_bytes_ += result_size;

      accumulate_functors_.emplace_back(handle->getStateAccumulateFunctor());
      merge_functors_.emplace_back(handle->getStateMergeFunctor());
      finalize_functors_.emplace_back(handle->getStateFinalizeFunctor());
    }

    initial_states_.reset(states_size_in_bytes_, false);
    if (use_mutex) {
      new(initial_states_.get()) Mutex;
    }
    for (std::size_t i = 0; i < handles_.size(); ++i) {
      handles_[i]->initializeState(
          static_cast<char *>(initial_states_.get()) + state_offsets_[i]);
    }
  }

  inline std::size_t getStatesSizeInBytes() const {
    return states_size_in_bytes_;
  }

  inline std::size_t getResultsSizeInBytes() const {
    return results_size_in_bytes_;
  }

  inline void initializeStates(void *states) const {
    copyStates(states, initial_states_.get());
  }

  template <bool check_for_null_keys, typename ValueAccessorT>
  inline void updateState(void *states,
                          ValueAccessorT *accessor,
                          const attribute_id argument_id) const {
    // TODO: templates on whether to check invalid attribute id
    DCHECK_NE(argument_id, kInvalidAttributeID);

    const void *value =
        accessor->template getUntypedValue<check_for_null_keys>(argument_id);
    if (check_for_null_keys && value == nullptr) {
      return;
    }
    accumulate_functors_.front()(states, value);
  }

  template <bool check_for_null_keys, typename ValueAccessorT>
  inline void updateStates(void *states,
                           ValueAccessorT *accessor,
                           const std::vector<attribute_id> &argument_ids) const {
    for (std::size_t i = 0; i < argument_ids.size(); ++i) {
      // TODO: templates on whether to check invalid attribute id
      DCHECK_NE(argument_ids[i], kInvalidAttributeID);

      const void *value =
          accessor->template getUntypedValue<check_for_null_keys>(argument_ids[i]);
      if (check_for_null_keys && value == nullptr) {
        return;
      }
      accumulate_functors_[i](getStateComponent(states, i), value);
    }
  }

  inline void copyStates(void *destination_states,
                         const void *source_states) const {
    InlineMemcpy(destination_states, source_states, states_size_in_bytes_);
  }

  inline void mergeStates(void *destination_states,
                          const void *source_states) const {
    for (std::size_t i = 0; i < merge_functors_.size(); ++i) {
      merge_functors_[i](getStateComponent(destination_states, i),
                         getStateComponent(source_states, i));
    }
  }

  inline void finalizeStates(void *results, const void *states) const {
    for (std::size_t i = 0; i < merge_functors_.size(); ++i) {
      finalize_functors_[i](getResultComponent(results, i),
                            getStateComponent(states, i));
    }
  }

  inline const void* getStateComponent(const void *states,
                                       const std::size_t component_id) const {
    return static_cast<const char *>(states) + state_offsets_[component_id];
  }

  inline void* getStateComponent(void *states,
                                 const std::size_t component_id) const {
    return static_cast<char *>(states) + state_offsets_[component_id];
  }

  inline void* getResultComponent(void *results,
                                  const std::size_t component_id) const {
    return static_cast<char *>(results) + result_offsets_[component_id];
  }

 private:
  std::vector<AggregationHandle *> handles_;

  std::vector<std::size_t> state_sizes_;
  std::vector<std::size_t> state_offsets_;
  std::size_t states_size_in_bytes_;

  std::vector<std::size_t> result_sizes_;
  std::vector<std::size_t> result_offsets_;
  std::size_t results_size_in_bytes_;

  std::vector<AggregationStateAccumulateFunctor> accumulate_functors_;
  std::vector<AggregationStateMergeFunctor> merge_functors_;
  std::vector<AggregationStateFinalizeFunctor> finalize_functors_;

  ScopedBuffer initial_states_;

  DISALLOW_COPY_AND_ASSIGN(AggregationStateManager);
};

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_AGGREGATION_STATE_MANAGER_HPP_

