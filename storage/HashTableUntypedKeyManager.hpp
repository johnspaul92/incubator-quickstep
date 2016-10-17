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

#ifndef QUICKSTEP_STORAGE_HASH_TABLE_UNTYPED_KEY_MANAGER_HPP_
#define QUICKSTEP_STORAGE_HASH_TABLE_UNTYPED_KEY_MANAGER_HPP_

#include <atomic>
#include <functional>
#include <cstddef>
#include <cstring>
#include <vector>

#include "storage/StorageConstants.hpp"
#include "types/Type.hpp"
#include "types/TypeFunctors.hpp"
#include "types/TypedValue.hpp"
#include "utility/InlineMemcpy.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

/** \addtogroup Storage
 *  @{
 */

class HashTableUntypedKeyManager {
 public:
  HashTableUntypedKeyManager(const std::vector<const Type *> &key_types,
                             const std::size_t key_start_in_bucket)
      : key_types_(key_types),
        key_hasher_(MakeUntypedHashFunctor(key_types)),
        key_equality_checker_(MakeUntypedEqualityFunctor(key_types)),
        key_start_in_bucket_(key_start_in_bucket),
        fixed_key_size_(0),
        is_key_nullable_(false) {

    for (const Type *key_type : key_types) {
      key_sizes_.emplace_back(key_type->maximumByteLength());
      key_offsets_.emplace_back(fixed_key_size_);
      key_offsets_in_bucket_.emplace_back(key_start_in_bucket + fixed_key_size_);

      DCHECK(!key_type->isVariableLength());
      fixed_key_size_ += key_type->maximumByteLength();
      is_key_nullable_ |= key_type->isNullable();
    }
  }

  inline std::size_t getNumKeyComponents() const {
    return key_types_.size();
  }

  inline std::size_t getFixedKeySize() const  {
    return fixed_key_size_;
  }

  inline bool isKeyNullable() const {
    return is_key_nullable_;
  }

  inline std::size_t hashUntypedKey(const void *key) const {
    return key_hasher_(key);
  }

  inline bool checkUntypedKeyCollision(const void *key,
                                       const void *bucket) const {
    return key_equality_checker_(key, getUntypedKeyComponent(bucket));
  }

  inline void writeUntypedKeyToBucket(const void *key,
                                      void *bucket) {
    copyUntypedKey(getUntypedKeyComponent(bucket), key);
  }

  inline void copyUntypedKey(void *destination,
                             const void *key) const {
    InlineMemcpy(destination, key, fixed_key_size_);
  }

  template <typename ValueAccessorT>
  inline bool writeNullableUntypedKeyFromValueAccessorToBucket(
      ValueAccessorT *accessor,
      const std::vector<attribute_id> &key_attr_ids,
      void *bucket) {
    for (std::size_t i = 0; i < key_attr_ids.size(); ++i) {
      const void *key_component = accessor->getUntypedValue(key_attr_ids[i]);
      if (key_component == nullptr) {
        return true;
      }
      InlineMemcpy(getUntypedKeyComponent(bucket, i), key_component, key_sizes_[i]);
    }
    return false;
  }

  template <typename ValueAccessorT>
  inline void writeUntypedKeyFromValueAccessorToBucket(
      ValueAccessorT *accessor,
      const std::vector<attribute_id> &key_attr_ids,
      void *bucket) {
    for (std::size_t i = 0; i < key_attr_ids.size(); ++i) {
      const void *key_component = accessor->template getUntypedValue<false>(key_attr_ids[i]);
      InlineMemcpy(getUntypedKeyComponent(bucket, i), key_component, key_sizes_[i]);
    }
  }

  inline const void* getUntypedKeyComponent(const void *bucket) const {
    return static_cast<const char *>(bucket) + key_start_in_bucket_;
  }

  inline void* getUntypedKeyComponent(void *bucket) const {
    return static_cast<char *>(bucket) + key_start_in_bucket_;
  }

  inline const void* getUntypedKeyComponent(const void *bucket,
                                            const std::size_t component_id) const {
    return static_cast<const char *>(bucket) + key_offsets_in_bucket_[component_id];
  }

  inline void* getUntypedKeyComponent(void *bucket,
                                      const std::size_t component_id) const {
    return static_cast<char *>(bucket) + key_offsets_in_bucket_[component_id];
  }

  inline const UntypedHashFunctor& getUntypedKeyHashFunctor() const {
    return key_hasher_;
  }

  inline const UntypedEqualityFunctor& getUntypedKeyEqualityFunctor() const {
    return key_equality_checker_;
  }

 private:
  const std::vector<const Type*> &key_types_;
  const UntypedHashFunctor key_hasher_;
  const UntypedEqualityFunctor key_equality_checker_;


  std::size_t key_start_in_bucket_;
  std::size_t fixed_key_size_;
  bool is_key_nullable_;

  std::vector<std::size_t> key_sizes_;
  std::vector<std::size_t> key_offsets_;
  std::vector<std::size_t> key_offsets_in_bucket_;

  DISALLOW_COPY_AND_ASSIGN(HashTableUntypedKeyManager);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_HASH_TABLE_UNTYPED_KEY_MANAGER_HPP_
