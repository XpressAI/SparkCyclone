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

#include "frovedis/core/radix_sort.hpp"
#include "frovedis/core/set_operations.hpp"
#include <vector>

namespace cyclone::grouping {
  /*
    Perform sort + group on multiple contiguous ranges.

    Sort + group is an easy and vectorizable algorithm for grouping elements.
    For a given array range, the steps are as follows:

      1.  Run a stable sort on the array of elements using
          [[frovedis::radix_sort]].
      2.  With the elements now in order, compute the indexes on the array where
          the values change using [[frovedis::set_separate]].  For example,
          in the sorted array [ 0, 0, 3, 5 ], the values change at array indices
          0, 2, 3, and 4.  These indices form the bounds of the groups.

    Multiple sort + group is simply an application of this algorithm over to
    multiple contiguous ranges of an array, and concatenates the indices of the
    groups together.

    Consider this example input:

      ARRAY     : [ 23, 0, 1, 4, 3, -2, 1, 5, 3, 0, 1, 6, 9, 6, 42, -100 ]
      INDEX     : [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 ]
      GROUPING  : [ 1, 6, 11, 14 ]

    The full input array consists of 16 elements, but we are asked to perform
    sort + group specificaly on 3 ranges: [1, 6), [6, 11), and [11, 14).

    After running multiple sort + group, we will get the following output:

      ARRAY     : [ 23, -2, 0, 1, 3, 4, 0, 1, 1, 3, 5, 6, 6, 9, 42, -100 ]
      INDEX     : [ 0, 5, 1, 2, 4, 3, 9, 6, 10, 8, 7, 11, 13, 12, 14, 15 ]
      GROUPING  : [ 0, 2, 3, 4, 5, 6, 7, 9, 10, 11, 13, 14 ]

    The elements of [1, 6) in the original array are now sorted in the output
    array, but only within that range.  Likewise for ranges [6, 11), and
    [11, 14).  Elements at indices 0, 14, and 15 remain un-touched.  The
    output groups reflect the indices where the values change, and are a
    concatenation of the group indices found in each range denoted by the
    input group indices.

    If this function is called with sort_ascending = false in the template
    parameter, then the elements in each specified range will be sorted in DESC
    order.  Using the above example input, the output will be as follows:

      ARRAY     : [ 23, 4, 3, 1, 0, -2, 5, 3, 1, 1, 0, 9, 6, 6, 42, -100 ]
      INDEX     : [ 0, 5, 1, 2, 4, 3, 9, 6, 10, 8, 7, 11, 13, 12, 14, 15 ]
      GROUPING  : [ 0, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 14 ]

    Function Arguments:
      sort_data_arr           : The data array whose elements are to be sorted (sort keys).
      sort_data_len           : The length of the data array.
      index_arr               : An array of indices of the elements (sort values).  This is used for multi-stage sorting.
      input_group_delims_arr  : Indices that denote the ranges to be sorted.  Index values are relative to &sort_data_arr[0].
      input_group_delims_len  : Length of the input indices.
      output_group_delims_arr : Combined indices where the values change (to be written after the sort + grouping).  Index values are relative to &sort_data_arr[0].
      output_group_delims_len : Length of the output indices (to be written after the sort + grouping).  Index values are relative to &sort_data_arr[0].
  */
  template <typename T, bool sort_ascending = true>
  inline const void sort_and_group_multiple(T             * sort_data_arr,
                                            const size_t    sort_data_len,
                                            size_t        * index_arr,
                                            const size_t  * input_group_delims_arr,
                                            const size_t    input_group_delims_len,
                                            size_t        * output_group_delims_arr,
                                            size_t        & output_group_delims_len) {
    // If there are more group delimiters than elements in the range, then that
    // means that each subset will contain just one element
    if (input_group_delims_len > sort_data_len) {
      memcpy(output_group_delims_arr, input_group_delims_arr, sizeof(size_t) * input_group_delims_len);
      output_group_delims_len = input_group_delims_len;
      return;
    }

    // Set the output_group_delims_arr to be of length 0
    output_group_delims_len = 0;
    output_group_delims_arr[output_group_delims_len++] = 0;

    // For each subset denoted by input_group_delims, perform sort + grouping
    // and get back a component delim group
    #pragma _NEC vector
    #pragma _NEC ivdep
    for (auto i = 1; i < input_group_delims_len; i++) {
      // Fetch the boundaries of the range containing subset i
      const auto subset_start = input_group_delims_arr[i - 1];
      const auto subset_end   = input_group_delims_arr[i];
      const auto subset_size  = subset_end - subset_start;

      if (subset_size == 1) {
        output_group_delims_arr[output_group_delims_len++] = subset_end;

      } else {
        // Sort the elements in subset i
        if constexpr (sort_ascending) {
          frovedis::radix_sort(&sort_data_arr[subset_start], &index_arr[subset_start], subset_size);
        } else {
          frovedis::radix_sort_desc(&sort_data_arr[subset_start], &index_arr[subset_start], subset_size);
        }

        // Construct the array of indices where the value of the keys change
        auto delims = frovedis::set_separate(&sort_data_arr[subset_start], subset_size);

        // Append them to the output_group_delims_arr (accounting for relative
        // positioning).  It is written in this format instead of
        // `arr[len++] = value` to enable vectorization
        {
          #pragma _NEC vector
          #pragma _NEC ivdep
          for (auto d = 1; d < delims.size(); d++) {
            // Adjust for offset with d - 1
            output_group_delims_arr[output_group_delims_len + d - 1] = subset_start + delims[d];
          }
          // Increment by n - 1 instead of n to avoid overcounting
          output_group_delims_len += delims.size() - 1;
        }
      }
    }

    return;
  }

  /*
    Perform sort + group on multiple contiguous ranges.  This is the C++ version
    of the function signature to be used mainly for testing and development.
  */
  template <typename T, bool sort_ascending = true>
  inline const std::vector<size_t> sort_and_group_multiple(std::vector<T>             & sort_data,
                                                           std::vector<size_t>        & index_data,
                                                           const std::vector<size_t>  & input_group_delims) {
    std::vector<size_t> output_group_delims(input_group_delims.back() - input_group_delims.front());
    size_t output_group_delims_len;

    sort_and_group_multiple<T, sort_ascending>(
      sort_data.data(),
      sort_data.size(),
      index_data.data(),
      input_group_delims.data(),
      input_group_delims.size(),
      output_group_delims.data(),
      output_group_delims_len
    );

    // Resize the vector to match output_group_delims_len
    output_group_delims.resize(output_group_delims_len);
    return output_group_delims;
  }

  inline const std::vector<std::vector<size_t>> separate_to_groups(const std::vector<size_t> &ids,
                                                                   std::vector<size_t> &group_keys) {
    auto groups = ids;
    frovedis::radix_sort(groups);
    auto unique_groups = frovedis::set_unique(groups);
    std::vector<size_t> group_counts(unique_groups.size());

    auto ids_count = ids.size();
    auto ids_arr = ids.data();
    auto groups_arr = groups.data();
    auto groups_size = groups.size();
    auto groups_count_arr = group_counts.data();
    auto unique_groups_arr = unique_groups.data();
    auto unique_groups_size = unique_groups.size();

    // Count the number of groups
    for (size_t g = 0; g < unique_groups_size; g++) {
      size_t current_count = 0;
      size_t the_group = unique_groups_arr[g];

      #pragma _NEC vector
      for (size_t i = 0; i < groups_size; i++) {
        if (groups_arr[i] == the_group) {
          current_count++;
        }
      }
      groups_count_arr[g] = current_count;
    }

    std::vector<std::vector<size_t>> ret(unique_groups_size);
    for (size_t i = 0; i < unique_groups_size; i++) {
      ret[i].resize(groups_count_arr[i]);
    }

    auto ret_arr = ret.data();

    for (auto g = 0; g < unique_groups_size; g++) {
      size_t group_pos = 0;
      size_t group = unique_groups_arr[g];
      auto ret_g_arr = ret_arr[g].data();
      #pragma _NEC vector
      for (size_t i = 0; i < ids_count; i++) {
        if (ids_arr[i] == group) {
          ret_g_arr[group_pos++] = i;
        }
      }
    }

    group_keys = unique_groups;
    return ret;
  }
}
