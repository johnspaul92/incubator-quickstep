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

#include "expressions/aggregation/AggregationHandleCount.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"

#ifdef QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION
#include "storage/ValueAccessor.hpp"
#include "storage/ValueAccessorUtil.hpp"
#endif

#include "types/TypeFactory.hpp"
#include "types/TypeID.hpp"
#include "types/TypedValue.hpp"
#include "types/containers/ColumnVector.hpp"
#include "types/containers/ColumnVectorUtil.hpp"

#include "glog/logging.h"

namespace quickstep {

class StorageManager;
class Type;
class ValueAccessor;

template <bool count_star, bool nullable_type>
AggregationHandleCount<count_star, nullable_type>::AggregationHandleCount() {
  state_size_ = sizeof(ResultCppType);
  blank_state_.reset(state_size_, false);

  result_type_ = &TypeFactory::GetType(ResultType::kStaticTypeID);
  result_type_->makeZeroValue(blank_state_.get());

  accumulate_functor_ = [](void *state, const void *value) {
    *static_cast<ResultCppType *>(state) += 1;
  };

  merge_functor_ = [](void *state, const void *value) {
    *static_cast<ResultCppType *>(state) +=
        *static_cast<const ResultCppType *>(value);
  };

  finalize_functor_ = [](void *result, const void *state) {
    *static_cast<ResultCppType *>(result) =
        *static_cast<const ResultCppType *>(state);
  };

}

template <bool count_star, bool nullable_type>
void AggregationHandleCount<count_star, nullable_type>::accumulateNullary(
      void *state,
      const std::size_t num_tuples) const {
  *static_cast<ResultCppType *>(state) = num_tuples;
}

template <bool count_star, bool nullable_type>
void AggregationHandleCount<count_star, nullable_type>::accumulateColumnVectors(
    void *state,
    const std::vector<std::unique_ptr<ColumnVector>> &column_vectors) const {
  DCHECK(!count_star)
      << "Called non-nullary accumulation method on an AggregationHandleCount "
      << "set up for nullary COUNT(*)";

  DCHECK_EQ(1u, column_vectors.size())
      << "Got wrong number of ColumnVectors for COUNT: "
      << column_vectors.size();

  std::size_t count = 0;
  InvokeOnColumnVector(
      *column_vectors.front(),
      [&](const auto &column_vector) -> void {  // NOLINT(build/c++11)
        if (nullable_type) {
          // TODO(shoban): Iterating over the ColumnVector is a rather slow way
          // to do this. We should look at extending the ColumnVector interface
          // to do a quick count of the non-null values (i.e. the length minus
          // the population count of the null bitmap). We should do something
          // similar for ValueAccessor too.
          for (std::size_t pos = 0; pos < column_vector.size(); ++pos) {
            if (column_vector.getUntypedValue(pos) != nullptr) {
              ++count;
            }
          }
        } else {
          count = column_vector.size();
        }
      });

  *static_cast<ResultCppType *>(state) = count;
}

#ifdef QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION
template <bool count_star, bool nullable_type>
void AggregationHandleCount<count_star, nullable_type>::accumulateValueAccessor(
    void *state,
    ValueAccessor *accessor,
    const std::vector<attribute_id> &accessor_ids) const {
  DCHECK(!count_star)
      << "Called non-nullary accumulation method on an AggregationHandleCount "
      << "set up for nullary COUNT(*)";

  DCHECK_EQ(1u, accessor_ids.size())
      << "Got wrong number of attributes for COUNT: " << accessor_ids.size();

  const attribute_id accessor_id = accessor_ids.front();
  std::size_t count = 0;
  InvokeOnValueAccessorMaybeTupleIdSequenceAdapter(
      accessor,
      [&accessor_id, &count](auto *accessor) -> void {  // NOLINT(build/c++11)
        if (nullable_type) {
          while (accessor->next()) {
            if (accessor->getUntypedValue(accessor_id) != nullptr) {
              ++count;
            }
          }
        } else {
          count = accessor->getNumTuples();
        }
      });

  *static_cast<ResultCppType *>(state) = count;
}
#endif

// Explicitly instantiate and compile in the different versions of
// AggregationHandleCount we need. Note that we do not compile a version with
// 'count_star == true' and 'nullable_type == true', as that combination is
// semantically impossible.
template class AggregationHandleCount<false, false>;
template class AggregationHandleCount<false, true>;
template class AggregationHandleCount<true, false>;

}  // namespace quickstep
