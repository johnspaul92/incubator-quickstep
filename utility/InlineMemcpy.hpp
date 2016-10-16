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

#ifndef QUICKSTEP_UTILITY_INLINE_MEMCPY_HPP_
#define QUICKSTEP_UTILITY_INLINE_MEMCPY_HPP_

#include <cstring>

namespace quickstep {

/** \addtogroup Utility
 *  @{
 */

inline void InlineMemcpy(void *dst, const void *src, const std::size_t size) {
#define INLINE_ENTRY_(len) \
  case len: std::memcpy(dst, src, len); break

  switch (size) {
    INLINE_ENTRY_(1);
    INLINE_ENTRY_(2);
    INLINE_ENTRY_(3);
    INLINE_ENTRY_(4);
    INLINE_ENTRY_(5);
    INLINE_ENTRY_(6);
    INLINE_ENTRY_(7);
    INLINE_ENTRY_(8);
    INLINE_ENTRY_(9);
    INLINE_ENTRY_(10);
    INLINE_ENTRY_(11);
    INLINE_ENTRY_(12);
    INLINE_ENTRY_(13);
    INLINE_ENTRY_(14);
    INLINE_ENTRY_(15);
    INLINE_ENTRY_(16);
    INLINE_ENTRY_(17);
    INLINE_ENTRY_(18);
    INLINE_ENTRY_(19);
    INLINE_ENTRY_(20);
    INLINE_ENTRY_(21);
    INLINE_ENTRY_(22);
    INLINE_ENTRY_(23);
    INLINE_ENTRY_(24);
    INLINE_ENTRY_(25);
    INLINE_ENTRY_(26);
    INLINE_ENTRY_(27);
    INLINE_ENTRY_(28);
    INLINE_ENTRY_(29);
    INLINE_ENTRY_(30);
    INLINE_ENTRY_(31);
    INLINE_ENTRY_(32);

    default:
      std::memcpy(dst, src, size);
  }

#undef INLINE_ENTRY_
}

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_INLINE_MEMCPY_HPP_
