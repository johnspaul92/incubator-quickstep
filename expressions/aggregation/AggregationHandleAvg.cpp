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

#include "expressions/aggregation/AggregationHandleAvg.hpp"

#include <cstddef>
#include <memory>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "types/Type.hpp"
#include "types/TypeFactory.hpp"
#include "types/TypeID.hpp"
#include "types/TypedValue.hpp"
#include "types/operations/binary_operations/BinaryOperation.hpp"
#include "types/operations/binary_operations/BinaryOperationFactory.hpp"
#include "types/operations/binary_operations/BinaryOperationID.hpp"

#include "glog/logging.h"

namespace quickstep {

class StorageManager;

AggregationHandleAvg::AggregationHandleAvg(const Type &argument_type) {
  // We sum Int as Long and Float as Double so that we have more headroom when
  // adding many values.
  TypeID type_precision_id;
  switch (argument_type.getTypeID()) {
    case kInt:
    case kLong:
      type_precision_id = kLong;
      break;
    case kFloat:
    case kDouble:
      type_precision_id = kDouble;
      break;
    default:
      type_precision_id = argument_type.getTypeID();
      break;
  }

  const Type &sum_type = TypeFactory::GetType(type_precision_id);
  const Type &count_type = TypeFactory::GetType(CountType::kStaticTypeID);

  const std::size_t sum_state_size = sum_type.maximumByteLength();
  count_offset_ = sum_state_size;
  state_size_ = sum_state_size + sizeof(CountCppType);

  blank_state_.reset(state_size_, false);
  sum_type.makeZeroValue(blank_state_.get());
  count_type.makeZeroValue(static_cast<char *>(blank_state_.get()) + sum_state_size);

  tv_blank_sum_ = sum_type.makeZeroValue();

  // Make operators to do arithmetic:
  // Add operator for summing argument values.
  accumulate_add_operator_.reset(
      BinaryOperationFactory::GetBinaryOperation(BinaryOperationID::kAdd)
          .makeUncheckedBinaryOperatorForTypes(sum_type, argument_type));
  const auto accumulate_add_functor = accumulate_add_operator_->getMergeFunctor();
  accumulate_functor_ = [sum_state_size, accumulate_add_functor](
      void *state, const void *value) {
    accumulate_add_functor(state, value);
    void *count_ptr = static_cast<char *>(state) + sum_state_size;
    ++(*static_cast<CountCppType *>(count_ptr));
  };

  // Add operator for merging states.
  merge_add_operator_.reset(
      BinaryOperationFactory::GetBinaryOperation(BinaryOperationID::kAdd)
          .makeUncheckedBinaryOperatorForTypes(sum_type, sum_type));
  const auto merge_add_functor = merge_add_operator_->getMergeFunctor();
  merge_functor_ = [sum_state_size, merge_add_functor](
      void *destination_state, const void *source_state) {
    merge_add_functor(destination_state, source_state);
    void *destination_count_ptr =
        static_cast<char *>(destination_state) + sum_state_size;
    const void *source_count_ptr =
        static_cast<const char *>(source_state) + sum_state_size;
    *static_cast<CountCppType *>(destination_count_ptr) +=
        *static_cast<const CountCppType *>(source_count_ptr);
  };

  // Divide operator for dividing sum by count to get final average.
  divide_operator_.reset(
      BinaryOperationFactory::GetBinaryOperation(BinaryOperationID::kDivide)
          .makeUncheckedBinaryOperatorForTypes(sum_type,
                                               TypeFactory::GetType(kDouble)));
  const auto divide_functor = divide_operator_->getFunctor();
  finalize_functor_ = [sum_state_size, divide_functor](
      void *result, const void *state) {
    const void *count_ptr = static_cast<const char *>(state) + sum_state_size;
    const double count = *static_cast<const CountCppType *>(count_ptr);
    divide_functor(result, state, &count);
  };

  result_type_ =
      BinaryOperationFactory::GetBinaryOperation(BinaryOperationID::kDivide)
          .resultTypeForArgumentTypes(sum_type, TypeFactory::GetType(kDouble));
}

void AggregationHandleAvg::accumulateColumnVectors(
    void *state,
    const std::vector<std::unique_ptr<ColumnVector>> &column_vectors) const {
  DCHECK_EQ(1u, column_vectors.size())
      << "Got wrong number of ColumnVectors for AVG: " << column_vectors.size();

  std::size_t count = 0;
  TypedValue cv_sum = accumulate_add_operator_->accumulateColumnVector(
      tv_blank_sum_, *column_vectors.front(), &count);
  cv_sum.copyInto(state);
  void *count_ptr = static_cast<char *>(state) + count_offset_;
  *static_cast<CountCppType *>(count_ptr) = count;
}

#ifdef QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION
void AggregationHandleAvg::accumulateValueAccessor(
    void *state,
    ValueAccessor *accessor,
    const std::vector<attribute_id> &accessor_ids) const {
  DCHECK_EQ(1u, accessor_ids.size())
      << "Got wrong number of attributes for AVG: " << accessor_ids.size();

  std::size_t count = 0;
  TypedValue cv_sum = accumulate_add_operator_->accumulateValueAccessor(
      tv_blank_sum_, accessor, accessor_ids.front(), &count);
  cv_sum.copyInto(state);
  void *count_ptr = static_cast<char *>(state) + count_offset_;
  *static_cast<CountCppType *>(count_ptr) = count;
}
#endif

}  // namespace quickstep
