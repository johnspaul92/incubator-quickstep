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
#include <memory>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/PackedPayloadAggregationStateHashTable.hpp"
#include "storage/ValueAccessor.hpp"
#include "storage/ValueAccessorUtil.hpp"
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
AggregationState* AggregationHandleCount<count_star, nullable_type>::accumulate(
    ValueAccessor *accessor,
    ColumnVectorsValueAccessor *aux_accessor,
    const std::vector<attribute_id> &argument_ids) const {
  DCHECK(!count_star)
      << "Called non-nullary accumulation method on an AggregationHandleCount "
      << "set up for nullary COUNT(*)";

  DCHECK_EQ(1u, argument_ids.size())
      << "Got wrong number of attributes for COUNT: " << argument_ids.size();

  const attribute_id argument_id = argument_ids.front();
  DCHECK_NE(argument_id, kInvalidAttributeID);

  ValueAccessor *target_accessor =
      argument_id >= 0 ? accessor : aux_accessor;
  const attribute_id target_argument_id =
      argument_id >= 0 ? argument_id : -(argument_id+2);

  std::size_t count = 0;
  InvokeOnValueAccessorMaybeTupleIdSequenceAdapter(
      target_accessor,
      [&target_argument_id, &count](auto *accessor) -> void {  // NOLINT(build/c++11)
        if (nullable_type) {
          while (accessor->next()) {
            count += !accessor->getTypedValue(target_argument_id).isNull();
          }
        } else {
          count = accessor->getNumTuples();
        }
      });

  return new AggregationStateCount(count);
}

template <bool count_star, bool nullable_type>
void AggregationHandleCount<count_star, nullable_type>::mergeStates(
    const AggregationState &source, AggregationState *destination) const {
  const AggregationStateCount &count_source =
      static_cast<const AggregationStateCount &>(source);
  AggregationStateCount *count_destination =
      static_cast<AggregationStateCount *>(destination);

  count_destination->count_.fetch_add(
      count_source.count_.load(std::memory_order_relaxed),
      std::memory_order_relaxed);
}

template <bool count_star, bool nullable_type>
void AggregationHandleCount<count_star, nullable_type>::mergeStates(
    const std::uint8_t *source, std::uint8_t *destination) const {
  const std::int64_t *src_count_ptr =
      reinterpret_cast<const std::int64_t *>(source);
  std::int64_t *dst_count_ptr = reinterpret_cast<std::int64_t *>(destination);
  (*dst_count_ptr) += (*src_count_ptr);
}

template <bool count_star, bool nullable_type>
ColumnVector*
AggregationHandleCount<count_star, nullable_type>::finalizeHashTable(
    const AggregationStateHashTableBase &hash_table,
    std::vector<std::vector<TypedValue>> *group_by_keys,
    int index) const {
  return finalizeHashTableHelper<
      AggregationHandleCount<count_star, nullable_type>,
      PackedPayloadSeparateChainingAggregationStateHashTable>(
          TypeFactory::GetType(kLong), hash_table, group_by_keys, index);
}

// Explicitly instantiate and compile in the different versions of
// AggregationHandleCount we need. Note that we do not compile a version with
// 'count_star == true' and 'nullable_type == true', as that combination is
// semantically impossible.
template class AggregationHandleCount<false, false>;
template class AggregationHandleCount<false, true>;
template class AggregationHandleCount<true, false>;

}  // namespace quickstep
