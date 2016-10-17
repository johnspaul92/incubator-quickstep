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

#ifndef QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_COUNT_HPP_
#define QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_COUNT_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "expressions/aggregation/AggregationHandle.hpp"
#include "types/LongType.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class ColumnVector;
class StorageManager;
class Type;
class ValueAccessor;

template <bool, bool>
class AggregationHandleCount;

/** \addtogroup Expressions
 *  @{
 */

/**
 * @brief An aggregationhandle for count.
 *
 * @param count_star If true, this AggregationHandleCount is for nullary
 *        COUNT(*). If false, it is a COUNT over some scalar expression.
 * @param nullable_type If true, the argument to COUNT() is nullable, and NULL
 *        values should not be included in the count. If false, the argument is
 *        not nullable and NULL-checks can safely be skipped.
 **/
template <bool count_star, bool nullable_type>
class AggregationHandleCount : public AggregationHandle {
 public:
  ~AggregationHandleCount() override {}

  void accumulateNullary(
      void *state,
      const std::size_t num_tuples) const override;

  void accumulateColumnVectors(
      void *state,
      const std::vector<std::unique_ptr<ColumnVector>> &column_vectors) const override;

#ifdef QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION
  void accumulateValueAccessor(
      void *state,
      ValueAccessor *accessor,
      const std::vector<attribute_id> &accessor_id) const override;
#endif

 private:
  friend class AggregateFunctionCount;

  typedef LongType ResultType;
  typedef ResultType::cpptype ResultCppType;

  /**
   * @brief Constructor.
   **/
  AggregationHandleCount();

  DISALLOW_COPY_AND_ASSIGN(AggregationHandleCount);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_COUNT__
