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

#ifndef QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_HPP_
#define QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_HPP_

#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "types/Type.hpp"
#include "types/TypedValue.hpp"
#include "utility/ScopedBuffer.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

class ColumnVector;
class StorageManager;
class Type;
class ValueAccessor;

/** \addtogroup Expressions
 *  @{
 */

typedef std::function<void (void *, const void *)> AggregationStateAccumulateFunctor;
typedef std::function<void (void *, const void *)> AggregationStateMergeFunctor;
typedef std::function<void (void *, const void *)> AggregationStateFinalizeFunctor;

class AggregationHandle {
 public:
  /**
   * @brief Virtual destructor.
   *
   **/
  virtual ~AggregationHandle() {}

  /**
   * @brief Accumulate over tuples for a nullary aggregate function (one that
   *        has zero arguments, i.e. COUNT(*)).
   **/
  virtual void accumulateNullary(void *state,
                                 const std::size_t num_tuples) const {
    LOG(FATAL) << "Called accumulateNullary on an AggregationHandle that "
               << "takes at least one argument.";
  }

  /**
   * @brief Accumulate (iterate over) all values in one or more ColumnVectors
   *        and return a new AggregationState which can be merged with other
   *        states or finalized.
   **/
  virtual void accumulateColumnVectors(
      void *state,
      const std::vector<std::unique_ptr<ColumnVector>> &column_vectors) const {
    LOG(FATAL) << "Not implemented\n";
  }

#ifdef QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION
  /**
   * @brief Accumulate (iterate over) all values in columns accessible through
   *        a ValueAccessor and return a new AggregationState which can be
   *        merged with other states or finalized.
   *
   * @param accessor A ValueAccessor that the columns to be aggregated can be
   *        accessed through.
   * @param accessor_ids The attribute_ids that correspond to the columns in
   *        accessor to aggeregate. These correspond to the aggregate
   *        function's arguments, in order.
   * @return A new AggregationState which contains the accumulated results from
   *         applying the aggregate to the specified columns in accessor.
   *         Caller is responsible for deleting the returned AggregationState.
   **/
  virtual void accumulateValueAccessor(
      void *state,
      ValueAccessor *accessor,
      const std::vector<attribute_id> &accessor_ids) const {
    LOG(FATAL) << "Not implemented\n";
  }
#endif

  /**
   * @brief Get the number of bytes needed to store the aggregation handle's
   *        state.
   **/
  inline std::size_t getStateSize() const {
    return state_size_;
  }

  inline void initializeState(void *state) const {
    std::memcpy(state, blank_state_.get(), state_size_);
  }

  inline ScopedBuffer createInitialState() const {
    ScopedBuffer state(state_size_, false);
    initializeState(state.get());
    return state;
  }

  inline void mergeStates(void *destination_state,
                          const void *source_state) const {
    merge_functor_(destination_state, source_state);
  }

  inline std::size_t getResultSize() const {
    return result_type_->maximumByteLength();
  }

  inline void finalize(void *value, const void *state) const {
    finalize_functor_(value, state);
  }

  inline TypedValue finalize(const void *state) const {
    ScopedBuffer value(state_size_, false);
    finalize(value.get(), state);
    TypedValue result = result_type_->makeValue(value.get());
    result.ensureNotReference();
    return result;
  }

  inline const AggregationStateMergeFunctor& getStateAccumulateFunctor() const {
    DCHECK(accumulate_functor_);
    return accumulate_functor_;
  }

  inline const AggregationStateMergeFunctor& getStateMergeFunctor() const {
    DCHECK(merge_functor_);
    return merge_functor_;
  }

  inline const AggregationStateFinalizeFunctor& getStateFinalizeFunctor() const {
    DCHECK(finalize_functor_);
    return finalize_functor_;
  }

 protected:
  AggregationHandle() {}

  std::size_t state_size_;
  ScopedBuffer blank_state_;

  AggregationStateAccumulateFunctor accumulate_functor_;
  AggregationStateMergeFunctor merge_functor_;

  const Type *result_type_;
  AggregationStateFinalizeFunctor finalize_functor_;

 private:
  DISALLOW_COPY_AND_ASSIGN(AggregationHandle);
};

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_HPP_
