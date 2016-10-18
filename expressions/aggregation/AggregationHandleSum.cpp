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

#include "expressions/aggregation/AggregationHandleSum.hpp"

#include <cstddef>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/HashTable.hpp"
#include "storage/HashTableFactory.hpp"
#include "threading/SpinMutex.hpp"
#include "types/Type.hpp"
#include "types/TypeFactory.hpp"
#include "types/TypeID.hpp"
#include "types/TypedValue.hpp"
#include "types/operations/binary_operations/BinaryOperation.hpp"
#include "types/operations/binary_operations/BinaryOperationFactory.hpp"
#include "types/operations/binary_operations/BinaryOperationID.hpp"
#include "types/TypeFunctors.hpp"

#include "glog/logging.h"

namespace quickstep {

class StorageManager;

AggregationHandleSum::AggregationHandleSum(const Type &argument_type) {
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
  state_size_ = sum_type.maximumByteLength();
  blank_state_.reset(state_size_, false);

  sum_type.makeZeroValue(blank_state_.get());
  tv_blank_state_ = sum_type.makeZeroValue();

  // Make operators to do arithmetic:
  // Add operator for summing argument values.
  accumulate_operator_.reset(
      BinaryOperationFactory::GetBinaryOperation(BinaryOperationID::kAdd)
          .makeUncheckedBinaryOperatorForTypes(sum_type, argument_type));
  accumulate_functor_ = accumulate_operator_->getMergeFunctor();

  // Add operator for merging states.
  merge_operator_.reset(
      BinaryOperationFactory::GetBinaryOperation(BinaryOperationID::kAdd)
          .makeUncheckedBinaryOperatorForTypes(sum_type, sum_type));
  merge_functor_ = merge_operator_->getMergeFunctor();

  finalize_functor_ = MakeUntypedCopyFunctor(&sum_type);
  result_type_ = &sum_type;
}

void AggregationHandleSum::accumulateColumnVectors(
    void *state,
    const std::vector<std::unique_ptr<ColumnVector>> &column_vectors) const {
  DCHECK_EQ(1u, column_vectors.size())
      << "Got wrong number of ColumnVectors for SUM: " << column_vectors.size();
  std::size_t num_tuples = 0;
  TypedValue cv_sum = accumulate_operator_->accumulateColumnVector(
      tv_blank_state_, *column_vectors.front(), &num_tuples);
  cv_sum.copyInto(state);
}

#ifdef QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION
void AggregationHandleSum::accumulateValueAccessor(
    void *state,
    ValueAccessor *accessor,
    const std::vector<attribute_id> &accessor_ids) const {
  DCHECK_EQ(1u, accessor_ids.size())
      << "Got wrong number of attributes for SUM: " << accessor_ids.size();

  std::size_t num_tuples = 0;
  TypedValue va_sum = accumulate_operator_->accumulateValueAccessor(
      tv_blank_state_, accessor, accessor_ids.front(), &num_tuples);
  va_sum.copyInto(state);
}
#endif

}  // namespace quickstep
