/*
 * Copyright (c) 2022 Xpress AI.
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

#include "cyclone/cyclone_function_view.hpp"
#include "frovedis/text/words.hpp"
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <type_traits>
#include <vector>

inline void set_valid_bit(uint64_t * buffer,
                          const size_t idx,
                          const uint32_t validity) {
  auto byte = idx / 64;
  auto bit_index = idx % 64;
  if (validity) {
    buffer[byte] |= (1UL << bit_index);
  } else {
    buffer[byte] &= ~(1UL << bit_index);
  }
}

inline uint32_t get_valid_bit(const uint64_t * const buffer,
                              const size_t idx) {
  auto byte = idx / 64;
  auto bit_index = idx % 64;
  return (buffer[byte] >> bit_index) & 1;
}

template<typename T>
struct NullableScalarVec {
  // Allow instantiations of this template only for primiitive types T
  static_assert(std::is_fundamental_v<T>, "NullableScalarVec<T> can only be instantiated with T where T is a primitive type!");

  // NOTE: Field declaration order must be maintained to match existing JNA bindings

  // Initialize fields with defaults
  T         *data             = nullptr;  // The raw data
  uint64_t  *validityBuffer   = nullptr;  // Bit vector to denote null values
  int32_t   count             = 0;        // Row count (synonymous with size of data array)

  // malloc() a NullableScalarVec<T> with C++ initialization
  static NullableScalarVec<T> * allocate();

  // Merge N NullableScalarVec<T>s into 1 NullableScalarVec<T> (order is preserved)
  static NullableScalarVec<T> * merge(const NullableScalarVec<T> * const * const inputs,
                                      const size_t batches);

  // Explicitly force the generation of a default constructor
  NullableScalarVec() = default;

  // Construct from a given std::vector<T>
  NullableScalarVec(const std::vector<T> &src);

  // C++ destructor (to be called for object instances created by `new`)
  ~NullableScalarVec() {
    reset();
  }

  // C pseudo-destructor (to be called before `free()` for object instances created by `malloc()`)
  void reset();

  // Set the count to size, and allocate memory for data and validityBuffer
  void resize(const size_t size);

  // C implementation of a C++ move assignment
  void move_assign_from(NullableScalarVec<T> * other);

  // Returns true if the struct fields have default values
  bool is_default() const;

  // Print the data structure out for debugging
  void print() const;

  // Compute the hash of the value at a given index, starting with a given seed
  inline int64_t hash_at(const size_t idx,
                         const int64_t seed) const {
    return 31 * seed + data[idx];
  }

  // Set the validity value of the vector at the given index
  inline void set_validity(const size_t idx,
                           const int32_t validity) {
    set_valid_bit(validityBuffer, idx, validity);
  }

  // Fetch the validity value of the vector at the given index
  inline uint32_t get_validity(const size_t idx) const {
    return get_valid_bit(validityBuffer, idx);
  }

  // Return the validity buffer as a std::vector
  const std::vector<int32_t> validity_vec() const;

  // Return the data buffer as a std::vector<size_t> (values will be casted to size_t)
  const std::vector<size_t> size_t_data_vec() const;

  // Value equality check against another NullableScalarVec<T>
  bool equals(const NullableScalarVec<T> * const other) const;

  // Returns a deep copy of this NullableScalarVec<T>
  NullableScalarVec<T> * clone() const;

  // Returns a deep copy of the NullableScalarVec<T> that contains only elements
  // whose original index value is in the selected_ids.  This method can be used
  // to either filter out and/or re-order elements of the original NullableScalarVec<T>.
  NullableScalarVec<T> * select(const std::vector<size_t> &selected_ids) const;

  // Create N new NullableScalarVec<T>'s and copy values over to them based on
  // the bucket_assignments
  NullableScalarVec<T> ** bucket(const std::vector<size_t> &bucket_counts,
                                 const std::vector<size_t> &bucket_assignments) const;

  // Return a bitmask that is the value of evaluating an IN expression
  const std::vector<size_t> eval_in(const std::vector<T> &elements) const;
};

// Explicitly instantiate struct template for int32_t
typedef NullableScalarVec<int32_t> nullable_int_vector;

// Explicitly instantiate struct template for short
// int32_t is used because VE is not optimized for shorts
typedef NullableScalarVec<int32_t> nullable_short_vector;

// Explicitly instantiate struct template for int64_t
typedef NullableScalarVec<int64_t> nullable_bigint_vector;

// Explicitly instantiate struct template for float
typedef NullableScalarVec<float> nullable_float_vector;

// Explicitly instantiate struct template for double
typedef NullableScalarVec<double> nullable_double_vector;

struct nullable_varchar_vector {
  // NOTE: Field declaration order must be maintained to match existing JNA bindings

  // Initialize fields with defaults
  int32_t   *data             = nullptr;  // The raw data containing all the varchars concatenated together
  int32_t   *offsets          = nullptr;  // Offsets to denote varchar start and end positions
  int32_t   *lengths          = nullptr;  // Lengths of the words
  uint64_t  *validityBuffer   = nullptr;  // Bit vector to denote null values
  int32_t   dataSize          = 0;        // Size of data array
  int32_t   count             = 0;        // The row count

  // malloc() a nullable_varchar_vector with C++ initialization
  static nullable_varchar_vector * allocate();

  // Construct a C-allocated nullable_varchar_vector from frovedis::words (order is preserved)
  static nullable_varchar_vector * from_words(const frovedis::words &src);

  // Merge N nullable_varchar_vectors into 1 nullable_varchar_vector
  static nullable_varchar_vector * merge(const nullable_varchar_vector * const * const inputs,
                                         const size_t batches);

  static nullable_varchar_vector * from_binary_choice(const size_t count,
                                                      const cyclone::function_view<bool(size_t)> &condition,
                                                      const std::string &trueval,
                                                      const std::string &falseval);

  // Explicitly force the generation of a default constructor
  nullable_varchar_vector() = default;

  // Construct from a given std::vector<std::string>
  nullable_varchar_vector(const std::vector<std::string> &src);

  // Construct from a given frovedis::words
  nullable_varchar_vector(const frovedis::words &src);

  // C++ destructor (to be called for object instances created by `new`)
  ~nullable_varchar_vector() {
    reset();
  }

  // C pseudo-destructor (to be called before `free()` for object instances created by `malloc()`)
  void reset();

  // C implementation of a C++ move assignment
  void move_assign_from(nullable_varchar_vector * other);

  // Returns true if the struct fields have default values
  bool is_default() const;

  // Print the data structure out for debugging
  frovedis::words to_words() const;

  // Print the data structure out for debugging
  void print() const;

  // Compute the hash of the value at a given index, starting with a given seed
  inline int64_t hash_at(const size_t idx,
                         int64_t seed) const {
    for (int x = offsets[idx]; x < offsets[idx] + lengths[idx]; x++) {
      seed = 31 * seed + data[x];
    }
    return seed;
  }

  // Compute a vector of hashes corresponding to the values of the nullable_varchar_vector
  const std::vector<int64_t> hash_vec() const;

  // Set the validity value of the vector at the given index
  inline void set_validity(const size_t idx,
                           const int32_t validity) {
    set_valid_bit(validityBuffer, idx, validity);
  }

  // Fetch the validity value of the vector at the given index
  inline uint32_t get_validity(const size_t idx) const {
    return get_valid_bit(validityBuffer, idx);
  }

  // Return the validity buffer as a std::vector
  const std::vector<int32_t> validity_vec() const;

  // Value equality check against another nullable_varchar_vector
  bool equals(const nullable_varchar_vector * const other) const;

  bool equivalent_to(const nullable_varchar_vector * const other) const;

  // Returns a deep copy of this nullable_varchar_vector
  nullable_varchar_vector * clone() const;

  // Returns a deep copy of the nullable_varchar_vector that contains only
  // elements whose original index value is in the selected_ids.  This method
  // can be used to either filter out and/or re-order elements of the original
  // nullable_varchar_vector.
  nullable_varchar_vector * select(const std::vector<size_t> &selected_ids) const;

  // Create N new nullable_varchar_vector's and copy values over to them based
  // on the bucket_assignments
  nullable_varchar_vector ** bucket(const std::vector<size_t> &bucket_counts,
                                    const std::vector<size_t> &bucket_assignments) const;

  // Convert a vector of string dates into int32_t's (number of days since 1970-01-01)
  const std::vector<int32_t> date_cast() const;

  // Return a bitmask that is the value of evaluating a LIKE expression
  const std::vector<size_t> eval_like(const std::string &pattern) const;

  // Return a bitmask that is the value of evaluating an IN expression
  const std::vector<size_t> eval_in(const frovedis::words &elements) const;

  // Return a bitmask that is the value of evaluating an IN expression
  const std::vector<size_t> eval_in(const std::vector<std::string> &elements) const;
};

struct non_null_c_bounded_string {
  char      *data   = nullptr;
  int32_t   length  = 0;
};

#define VE_TD_DEFS 1
#endif
