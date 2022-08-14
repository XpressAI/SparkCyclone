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

#include "cyclone/util/func.hpp"
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

  // Construct a C-allocated NullableScalarVec<T> to represent a vector of N copies of the same value
  static NullableScalarVec<T> * constant(const size_t size, const T value);

  // Merge N NullableScalarVec<T>s into 1 NullableScalarVec<T> (order is preserved)
  static NullableScalarVec<T> * merge(const NullableScalarVec<T> * const * const inputs,
                                      const size_t batches);

  // Explicitly force the generation of a default constructor
  NullableScalarVec() = default;

  // Construct from a given std::vector<T>
  NullableScalarVec(const std::vector<T> &src);

  // Construct a vector of N copies of the same value
  NullableScalarVec(const size_t size, const T value);

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

  // Return the validity buffer as a std::vector<size_t>
  const std::vector<size_t> size_t_validity_vec() const;

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

  inline int32_t max_len() const { return -1; };
  inline int32_t min_len() const { return -1; };
  inline int32_t avg_len() const { return -1; };

  // Return groups of indexes for elements of the same value
  const std::vector<std::vector<size_t>> group_indexes() const;

  /*
    Perform sort + grouping on multiple contiguous ranges.

    Given a single range in the nullablr_scalar_vector, the algorithm
    steps for sort + group are as follows:

      1.  Sort and group by elements marked as valid vs invalid.
      1.  For the valid elements group, run a stable sort on the array of elements
          using [[frovedis::radix_sort]].
      2.  With the elements now in order, compute the indexes on the array where
          the values change using [[frovedis::set_separate]].  For example,
          in the sorted array [ 0, 0, 3, 5 ], the values change at array indices
          0, 2, 3, and 4.  These indices form the bounds of the groups.

    Using the following example (`#` indicates invalid element):
      [ 3, 1, 2, 2, 3, 3, 4, #11, 3, 9, 9, #4, 1, #9, 10, 11, 12, 42, -8, ]

    After step 1, the invalid elements are grouped to the right (`|` indicates grouping):
      [ 3, 1, 2, 2, 3, 3, 4, 3, 9, 9, 1, 10, 11, 12, 42, -8, | #9, #4, #11, ]

    After step 2, the valid elements are sorted:
        [ -8, 1, 1, 2, 2, 3, 3, 3, 3, 4, 9, 9, 10, 11, 12, 42, | #9, #4, #11, ]

    After step 3, sorted elements are now grouped:
      [ -8, | 1, 1, | 2, 2, | 3, 3, 3, 3, | 4, | 9, 9, | 10, | 11, | 12, | 42, | #9, #4, #11, ]

    The delimiters of the groups are given by the indices relative to the input array:
      [ 0, 1, 3, 5, 9, 10, 12, 13, 14, 15, 16, 19 ]

    group_indexes_on_subset() applies the sort + grouping algorithm onto on
    multiple contiguous ranges of the nullable_varchar_vector.

    Function Arguments:
      input_index_arr0        : An array of indices of the elements (sort values).
                                If set to nullptr, the regular iteration order is used.
      input_group_delims_arr  : Indices that denote the subset ranges to be sorted.
                                Index values are relative to `this->data`.
      input_group_delims_len  : Length of the input indices.
      output_index_arr        : An array of indices that reflect input_index_arr0
                                after sort + grouping (pre-allocated and to be written).
      output_group_delims_arr : Combined indices where the values change after
                                sort + grouping (pre-allocated and to be written).
                                Index values are relative to `this->data`.  The
                                pre-allocated size should be at least `range_size + 1`
                                where `range_size` refers to the distance from the
                                0th to last delimiter in input_group_delims_arr.
      output_group_delims_len : Length of output_group_delims_arr (to be written).
                                Index values are relative to `this->data`.
  */
  void group_indexes_on_subset(const size_t * input_index_arr0,
                               const size_t * input_group_delims_arr,
                               const size_t   input_group_delims_len,
                               size_t       * output_index_arr,
                               size_t       * output_group_delims_arr,
                               size_t       & output_group_delims_len) const;
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

  // Construct a C-allocated nullable_varchar_vector to represent a vector of N copies of the same value
  static nullable_varchar_vector * constant(const size_t size, const std::string_view &value);

  // Construct a C-allocated nullable_varchar_vector from frovedis::words (order is preserved)
  static nullable_varchar_vector * from_words(const frovedis::words &src);

  // Merge N nullable_varchar_vectors into 1 nullable_varchar_vector
  static nullable_varchar_vector * merge(const nullable_varchar_vector * const * const inputs,
                                         const size_t batches);

  static nullable_varchar_vector * from_binary_choice(const size_t count,
                                                      const cyclone::func::function_view<bool(size_t)> &condition,
                                                      const std::string &trueval,
                                                      const std::string &falseval);

  // Explicitly force the generation of a default constructor
  nullable_varchar_vector() = default;

  // Construct from a given std::vector<std::string>
  nullable_varchar_vector(const std::vector<std::string> &src);

  // Construct a vector of N copies of the same value
  nullable_varchar_vector(const size_t size, const std::string_view &value);

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
    int64_t out = seed;
    auto start = offsets[idx];
    auto end = offsets[idx] + lengths[idx];
    #pragma _NEC vector
    #pragma _NEC ivdep
    for (int x = start; x < end; x++) {
      out = 31 * out + data[x];
    }
    return out;
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

  int32_t max_len() const;
  int32_t min_len() const;
  int32_t avg_len() const;

  // Return groups of indexes for elements of the same value
  const std::vector<std::vector<size_t>> group_indexes() const;

  /*
    Perform sort + grouping on multiple contiguous ranges.

    The sort + group algorithm for varchars is slightly different from that for
    scalars.  Given a single range in the nullablr_varchar_vector, the algorithm
    steps are as follows:

      1.  Sort and group by elements marked as valid vs invalid.
      2.  For the valid-elements group, sort and group by string length.
      3.  For each of the same-length subgroups, sort and group by the ith
          character, for i from 0 to N, where N is the string length of all
          elements in the subgroup.

    Using the following example (`#` indicates invalid element):
      [ JAN, JANU, FEBU, FEB, #FOO, MARCH, MARCG, APR, APR, #BAR, JANU, SEP, OCT, NOV, DEC2, DEC1, DEC0, ]

    After step 1, the invalid elements are grouped to the right (`|` indicates grouping):
      [ JAN, JANU, FEBU, FEB, MARCH, MARCG, APR, APR, JANU, SEP, OCT, NOV, DEC2, DEC1, DEC0, | #FOO, #BAR, ]

    After step 2, the elements are grouped by string length:
      [ JAN, FEB, APR, APR, SEP, OCT, NOV, | JANU, FEBU, JANU, DEC2, DEC1, DEC0, | MARCH, MARCG, | #FOO, #BAR, ]

    After step 3a, elements of length 3 are sorted and grouped:
      [ APR, APR, | FEB, | JAN, | NOV, | OCT, | SEP, | JANU, FEBU, JANU, DEC2, DEC1, DEC0, | MARCH, MARCG, | #FOO, #BAR, ]

    After step 3b, elements of length 4 are sorted and grouped:
      [ APR, APR, | FEB, | JAN, | NOV, | OCT, | SEP, | DEC0, | DEC1, | DEC2, | FEBU, | JANU, JANU, | MARCH, MARCG, | #FOO, #BAR, ]

    After step 3c, elements of length 5 are sorted and grouped:
      [ APR, APR, | FEB, | JAN, | NOV, | OCT, | SEP, | DEC0, | DEC1, | DEC2, | FEBU, | JANU, JANU, | MARCG, | MARCH, | #FOO, #BAR, ]

    The delimiters of the groups are given by the indices relative to the input array:
      [ 0, 2, 3, 4, 5, 6, 7, 8, 9, 19, 12, 13, 14, 15, 17 ]

    group_indexes_on_subset() applies the sort + grouping algorithm onto on
    multiple contiguous ranges of the nullable_varchar_vector.

    Function Arguments:
      input_index_arr0        : An array of indices of the elements (sort values).
                                If set to nullptr, the regular iteration order is used.
      input_group_delims_arr  : Indices that denote the subset ranges to be sorted.
                                Index values are relative to `this->data`.
      input_group_delims_len  : Length of the input indices.
      output_index_arr        : An array of indices that reflect input_index_arr0
                                after sort + grouping (pre-allocated and to be written).
      output_group_delims_arr : Combined indices where the values change after
                                sort + grouping (pre-allocated and to be written).
                                Index values are relative to `this->data`.  The
                                pre-allocated size should be at least `range_size + 1`
                                where `range_size` refers to the distance from the
                                0th to last delimiter in input_group_delims_arr.
      output_group_delims_len : Length of output_group_delims_arr (to be written).
                                Index values are relative to `this->data`.
  */
  void group_indexes_on_subset(const size_t * input_index_arr0,
                               const size_t * input_group_delims_arr,
                               const size_t   input_group_delims_len,
                               size_t       * output_index_arr,
                               size_t       * output_group_delims_arr,
                               size_t       & output_group_delims_len) const;

  void group_indexes_on_subset0(const size_t  * input_index_arr0,
                                const size_t  * input_group_delims_arr,
                                const size_t    input_group_delims_len,
                                size_t        * output_index_arr,
                                size_t        * output_group_delims_arr,
                                size_t        & output_group_delims_len) const;

  const std::vector<std::vector<size_t>> group_indexes0() const;
};
