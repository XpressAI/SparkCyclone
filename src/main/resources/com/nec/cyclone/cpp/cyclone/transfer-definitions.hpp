/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#pragma once

#ifndef VE_TD_DEFS

#include <stdint.h>
#include <type_traits>

template<typename T>
struct NullableScalarVec {
  // Allow instantiations of this template only for primiitive types T
  static_assert(std::is_fundamental_v<T>, "NullableScalarVec<T> can only be instantiated with T where T is a primitive type!");

  // NOTE: Field declaration order must be maintained to match existing JNA bindings

  T         *data;              // The raw data
  uint64_t  *validityBuffer;    // Bit vector to denote null values
  int32_t   count;              // Row count (synonymous with size of data array)

  // Returns a deep copy of this NullableScalarVec<T>
  NullableScalarVec<T> * clone();

  // Value equality check against another NullableScalarVec<T>
  bool equals(const NullableScalarVec<T> * const other);
};

// Explicitly instantiate struct template for int32_t
// struct nullable_int_vector : NullableScalarVec<int32_t> {};
typedef NullableScalarVec<int32_t>  nullable_int_vector;

// Explicitly instantiate struct template for double
// struct nullable_double_vector : NullableScalarVec<double> {};
typedef NullableScalarVec<double>  nullable_double_vector;

// Explicitly instantiate struct template for int64_t
// struct nullable_bigint_vector : NullableScalarVec<int64_t> {};
typedef NullableScalarVec<int64_t>  nullable_bigint_vector;

struct nullable_varchar_vector {
  // NOTE: Field declaration order must be maintained to match existing JNA bindings

  char      *data;              // The raw data containing all the varchars concatenated together
  int32_t   *offsets;           // Offsets to denote varchar start and end positions
  uint64_t  *validityBuffer;    // Bit vector to denote null values
  int32_t   dataSize;           // Size of data array
  int32_t   count;              // The row count

  // Returns a deep copy of this nullable_varchar_vector
  nullable_varchar_vector * clone();

  // Value equality check against another nullable_varchar_vector
  bool equals(const nullable_varchar_vector * const other);
};

struct non_null_c_bounded_string {
  char *data;
  int32_t length;
};

#define VE_TD_DEFS 1
#endif
