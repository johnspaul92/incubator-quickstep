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

#include "types/TypeFunctors.hpp"

#include <cstdlib>
#include <cstring>
#include <functional>
#include <vector>

#include "types/CharType.hpp"
#include "types/DateType.hpp"
#include "types/DatetimeIntervalType.hpp"
#include "types/DatetimeType.hpp"
#include "types/DoubleType.hpp"
#include "types/FloatType.hpp"
#include "types/IntType.hpp"
#include "types/LongType.hpp"
#include "types/NullType.hpp"
#include "types/Type.hpp"
#include "types/TypeID.hpp"
#include "types/VarCharType.hpp"
#include "types/YearMonthIntervalType.hpp"
#include "types/operations/comparisons/ComparisonUtil.hpp"
#include "types/port/strnlen.hpp"
#include "utility/HashPair.hpp"

#include "glog/logging.h"

namespace quickstep {

template <typename Maker>
typename Maker::FunctorType MakeHelper(const Type *type) {
  switch (type->getTypeID()) {
    case kInt:
      return Maker::MakeFunctor(static_cast<const IntType *>(type));
    case kLong:
      return Maker::MakeFunctor(static_cast<const LongType *>(type));
    case kFloat:
      return Maker::MakeFunctor(static_cast<const FloatType *>(type));
    case kDouble:
      return Maker::MakeFunctor(static_cast<const DoubleType *>(type));
    case kDate:
      return Maker::MakeFunctor(static_cast<const DateType *>(type));
    case kDatetime:
      return Maker::MakeFunctor(static_cast<const DatetimeType *>(type));
    case kDatetimeInterval:
      return Maker::MakeFunctor(static_cast<const DatetimeIntervalType *>(type));
    case kYearMonthInterval:
      return Maker::MakeFunctor(static_cast<const YearMonthIntervalType *>(type));
    case kChar:
      return Maker::MakeFunctor(static_cast<const CharType *>(type));
    case kVarChar:
      return Maker::MakeFunctor(static_cast<const VarCharType *>(type));
    default:
      LOG(FATAL) << "Unrecognized type: " << type->getName();
  }
}

struct UntypedKeyHashFunctorMaker {
  typedef UntypedKeyHashFunctor FunctorType;

  template <typename TypeName>
  static FunctorType MakeFunctor(const TypeName *type) {
    return [type](const void *value_ptr) -> std::size_t {
      return type->getHash(value_ptr);
    };
  }
};

UntypedKeyHashFunctor MakeUntypedKeyHashFunctor(const Type *type) {
  return MakeHelper<UntypedKeyHashFunctorMaker>(type);
}

UntypedKeyHashFunctor MakeUntypedKeyHashFunctor(const std::vector<const Type *> &types) {
  DCHECK_GE(types.size(), 1u);

  if (types.size() == 1u) {
    return MakeUntypedKeyHashFunctor(types.front());
  }

  std::vector<UntypedKeyHashFunctor> hashers;
  std::vector<std::size_t> offsets;
  std::size_t accum_offset = 0;
  for (const Type *type : types) {
    hashers.emplace_back(MakeUntypedKeyHashFunctor(type));
    offsets.emplace_back(accum_offset);
    accum_offset += type->isVariableLength() ? sizeof(void *)
                                             : type->maximumByteLength();
  }
  return [offsets, hashers](const void *value_ptr) -> std::size_t {
    std::size_t hash = hashers[0](value_ptr);
    for (std::size_t i = 1; i < hashers.size(); ++i) {
      hash = CombineHashes(
          hash,
          hashers[i](static_cast<const char *>(value_ptr) + offsets[i]));
    }
    return hash;
  };
}

struct UntypedKeyEqualityFunctorMaker {
  typedef UntypedKeyEqualityFunctor FunctorType;

  template <typename TypeName>
  static UntypedKeyEqualityFunctor MakeFunctor(const TypeName *type) {
    return STLLiteralEqual<typename TypeName::cpptype>();
  }
};

template <>
UntypedKeyEqualityFunctor UntypedKeyEqualityFunctorMaker::MakeFunctor(
    const CharType *type) {
  return STLCharEqual(type->getStringLength());
}

template <>
UntypedKeyEqualityFunctor UntypedKeyEqualityFunctorMaker::MakeFunctor(
    const VarCharType *type) {
  return STLVarCharEqual();
}

UntypedKeyEqualityFunctor MakeUntypedKeyEqualityFunctor(const Type *type) {
  return MakeHelper<UntypedKeyEqualityFunctorMaker>(type);
}

UntypedKeyEqualityFunctor MakeUntypedKeyEqualityFunctor(const std::vector<const Type *> &types) {
  DCHECK_GE(types.size(), 1u);

  if (types.size() == 1u) {
    return MakeUntypedKeyEqualityFunctor(types.front());
  }

  std::vector<UntypedKeyEqualityFunctor> equality_checkers;
  std::vector<std::size_t> offsets;
  std::size_t accum_offset = 0;
  bool can_check_equality_with_memcmp = true;
  for (const Type *type : types) {
    equality_checkers.emplace_back(MakeUntypedKeyEqualityFunctor(type));
    offsets.emplace_back(accum_offset);
    accum_offset += type->isVariableLength() ? sizeof(void *)
                                             : type->maximumByteLength();
    can_check_equality_with_memcmp &= type->canCheckEqualityWithMemcmp();
  }
  if (can_check_equality_with_memcmp) {
    return [accum_offset](const void *left, const void *right) -> bool {
      return memcmp(left, right, accum_offset);
    };
  } else {
    return [offsets, equality_checkers](const void *left, const void *right) -> bool {
      for (std::size_t i = 0; i < equality_checkers.size(); ++i) {
        if (!equality_checkers[i](static_cast<const char *>(left) + offsets[i],
                                  static_cast<const char *>(right) + offsets[i])) {
          return false;
        }
      }
      return true;
    };
  }
}


}  // namespace quickstep
