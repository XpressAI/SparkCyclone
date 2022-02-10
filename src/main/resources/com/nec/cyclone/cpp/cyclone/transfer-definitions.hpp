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

#include <stddef.h>
#include <stdint.h>
#include <type_traits>
#include <vector>

template<typename T>
struct NullableScalarVec {
  // Allow instantiations of this template only for primiitive types T
  static_assert(std::is_fundamental_v<T>, "NullableScalarVec<T> can only be instantiated with T where T is a primitive type!");

  // NOTE: Field declaration order must be maintained to match existing JNA bindings

  // Initialize fields with defaults
  T         *data             = nullptr;  // The raw data
  uint64_t  *validityBuffer   = nullptr;  // Bit vector to denote null values
  int32_t   count             = 0;        // Row count (synonymous with size of data array)

  // Print the data structure out for debugging
  void print() const;

  // Value equality check against another NullableScalarVec<T>
  bool equals(const NullableScalarVec<T> * const other) const;

  // Returns a deep copy of this NullableScalarVec<T>
  NullableScalarVec<T> * clone() const;

  // Returns a filtered deep copy of the NullableScalarVec<T> that contains only
  // elements whose original index value is in the matching_ids
  NullableScalarVec<T> * filter(const std::vector<size_t> &matching_ids) const;

  // Create N new NullableScalarVec<T>'s and copy values over to them based on
  // the bucket_assignments
  NullableScalarVec<T> ** bucket(const std::vector<size_t> &bucket_counts,
                                 const std::vector<size_t> &bucket_assignments) const;
};

// Explicitly instantiate struct template for int32_t
typedef NullableScalarVec<int32_t> nullable_int_vector;

// Explicitly instantiate struct template for int64_t
typedef NullableScalarVec<int64_t> nullable_bigint_vector;

// Explicitly instantiate struct template for float
typedef NullableScalarVec<float> nullable_float_vector;

// Explicitly instantiate struct template for double
typedef NullableScalarVec<double> nullable_double_vector;

struct nullable_varchar_vector {
  // NOTE: Field declaration order must be maintained to match existing JNA bindings

  // Initialize fields with defaults
  char      *data             = nullptr;  // The raw data containing all the varchars concatenated together
  int32_t   *offsets          = nullptr;  // Offsets to denote varchar start and end positions
  uint64_t  *validityBuffer   = nullptr;  // Bit vector to denote null values
  int32_t   dataSize          = 0;        // Size of data array
  int32_t   count             = 0;        // The row count

  // Print the data structure out for debugging
  void print() const;

  // Value equality check against another nullable_varchar_vector
  bool equals(const nullable_varchar_vector * const other) const;

  // Returns a deep copy of this nullable_varchar_vector
  nullable_varchar_vector * clone() const;

  // Returns a filtered deep copy of the NullableScalarVec<T> that contains only
  // elements whose original index value is in the matching_ids
  nullable_varchar_vector * filter(const std::vector<size_t> &matching_ids) const;

  // Create N new nullable_varchar_vector's and copy values over to them based
  // on the bucket_assignments
  nullable_varchar_vector ** bucket(const std::vector<size_t> &bucket_counts,
                                    const std::vector<size_t> &bucket_assignments) const;
};

struct non_null_c_bounded_string {
  char *data;
  int32_t length;
};

#define VE_TD_DEFS 1
#endif
