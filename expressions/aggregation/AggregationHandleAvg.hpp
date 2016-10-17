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

#ifndef QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_AVG_HPP_
#define QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_AVG_HPP_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "expressions/aggregation/AggregationHandle.hpp"
#include "storage/HashTableBase.hpp"
#include "types/Type.hpp"
#include "types/TypedValue.hpp"
#include "types/operations/binary_operations/BinaryOperation.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

class ColumnVector;
class StorageManager;
class ValueAccessor;

/** \addtogroup Expressions
 *  @{
 */

/**
 * @brief An aggregationhandle for avg.
 **/
class AggregationHandleAvg : public AggregationHandle {
 public:
  ~AggregationHandleAvg() override {}

 private:
  friend class AggregateFunctionAvg;

  /**
   * @brief Constructor.
   *
   * @param type Type of the avg value.
   **/
  explicit AggregationHandleAvg(const Type &type);

//  const Type &argument_type_;
//  const Type *result_type_;
//  AggregationStateAvg blank_state_;
//  std::unique_ptr<UncheckedBinaryOperator> fast_add_operator_;
//  std::unique_ptr<UncheckedBinaryOperator> merge_add_operator_;
//  std::unique_ptr<UncheckedBinaryOperator> divide_operator_;

  DISALLOW_COPY_AND_ASSIGN(AggregationHandleAvg);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_AVG_HPP_
