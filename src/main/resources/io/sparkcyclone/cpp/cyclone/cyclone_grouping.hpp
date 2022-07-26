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
  template <typename T, bool sort_ascending = true>
  inline const std::vector<size_t> sort_and_group_multiple(T            * sort_data_arr,
                                                           size_t         sort_data_len,
                                                           size_t       * index_arr,
                                                           const size_t * input_group_delims_arr,
                                                           const size_t   input_group_delims_len) {
    // If there are more group delimiters than elements in the range, then that
    // means that each subset will contain just one element
    if (input_group_delims_len > sort_data_len) {
      std::vector<size_t> output_group_delims(input_group_delims_len);
      memcpy(output_group_delims.data(), input_group_delims_arr, sizeof(size_t) * input_group_delims_len);
      return output_group_delims;
    }

    std::vector<std::vector<size_t>> component_group_delims(input_group_delims_len - 1);

    // For each subset denoted by input_group_delims, perform sort + grouping
    // and get back a component delim group
    #pragma _NEC vector
    #pragma _NEC ivdep
    for (auto i = 1; i < input_group_delims_len; i++) {
      // Fetch the boundaries of the range containing subset i
      const auto subset_start = input_group_delims_arr[i-1];
      const auto subset_end   = input_group_delims_arr[i];
      const auto subset_size  = subset_end - subset_start;

      if (subset_size < 2) {
        component_group_delims[i - 1] = {{ 0, 1 }};

      } else {
        // Sort the elements in subset i
        if constexpr (sort_ascending) {
          frovedis::radix_sort(&sort_data_arr[subset_start], &index_arr[subset_start], subset_size);
        } else {
          frovedis::radix_sort_desc(&sort_data_arr[subset_start], &index_arr[subset_start], subset_size);
        }

        // Construct the array of indices where the value of the keys change
        component_group_delims[i - 1] = frovedis::set_separate(&sort_data_arr[subset_start], subset_size);
      }
    }

    // Compute a prefix sum to get the offsets of the data elements
    std::vector<size_t> data_len_offsets(input_group_delims_len, 0);
    #pragma _NEC vector
    for (auto i = 1; i < input_group_delims_len; i++) {
      const auto subset_start = input_group_delims_arr[i-1];
      const auto subset_end   = input_group_delims_arr[i];
      data_len_offsets[i] = data_len_offsets[i - 1] + (subset_end - subset_start);
    }

    // Compute a prefix sum to get the offsets of the component delim groups
    std::vector<size_t> component_group_offsets(input_group_delims_len, 0);
    #pragma _NEC vector
    for (auto i = 1; i < component_group_offsets.size(); i++) {
      component_group_offsets[i] = component_group_offsets[i - 1] + component_group_delims[i - 1].size() - 1;
    }

    // Using the component arrays, construct the final array of indices where the
    // value of the keys change
    std::vector<size_t> output_group_delims(component_group_offsets.back() + 1);

    // Populate the combined delimiters from the component delim groups in parallel
    #pragma _NEC vector
    #pragma _NEC ivdep
    for (auto i = 0; i < component_group_delims.size(); i++) {
      const auto delims = component_group_delims[i];

      #pragma _NEC vector
      #pragma _NEC ivdep
      for (auto j = 0; j < delims.size() - 1; j++) {
        // Each delim value in a component delim group is relative to the positions
        // specified in the orignal input group, and each delim value's position
        // in the final group array is relative as well.
        output_group_delims[j + component_group_offsets[i]] = delims[j] + data_len_offsets[i];
      }
    }

    // Populate the last delimiter
    output_group_delims.back() = data_len_offsets.back();

    return output_group_delims;
  }







  template <typename T, bool sort_ascending = true>
  inline const void sort_and_group_multiple2(T            * sort_data_arr,
                                                           size_t         sort_data_len,
                                                           size_t       * index_arr,
                                                           const size_t * input_group_delims_arr,
                                                           const size_t   input_group_delims_len,
                                                           size_t       * output_group_delims_arr,
                                                           size_t       & output_group_delims_len) {
    // If there are more group delimiters than elements in the range, then that
    // means that each subset will contain just one element
    if (input_group_delims_len > sort_data_len) {
      memcpy(output_group_delims_arr, input_group_delims_arr, sizeof(size_t) * input_group_delims_len);
      output_group_delims_len = input_group_delims_len;
      return;
    }

    // Set the output_group_delims_arr to be of length 0
    output_group_delims_len = 0;

    // For each subset denoted by input_group_delims, perform sort + grouping
    // and get back a component delim group
    #pragma _NEC vector
    #pragma _NEC ivdep
    for (auto i = 1; i < input_group_delims_len; i++) {
      // Fetch the boundaries of the range containing subset i
      const auto subset_start = input_group_delims_arr[i-1];
      const auto subset_end   = input_group_delims_arr[i];
      const auto subset_size  = subset_end - subset_start;

      if (subset_size == 1) {
        output_group_delims_arr[output_group_delims_len++] = subset_start;
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

        // Append them to the output_group_delims_arr (accounting for relative position)
        #pragma _NEC vector
        for (auto d = 0; d < delims.size(); d++) {
          output_group_delims_arr[output_group_delims_len++] = subset_start + delims[d];
        }
      }
    }

    return;
  }

  template <typename T, bool sort_ascending = true>
  inline const std::vector<size_t> sort_and_group_multiple2(std::vector<T>             & sort_data,
                                                           std::vector<size_t>        & index_data,
                                                           const std::vector<size_t>  & input_group_delims) {
    std::vector<size_t> output_group_delims(input_group_delims.back() - input_group_delims.front());
    size_t output_group_delims_len;

    sort_and_group_multiple2<T, sort_ascending>(
      sort_data.data(),
      sort_data.size(),
      index_data.data(),
      input_group_delims.data(),
      input_group_delims.size(),
      output_group_delims.data(),
      output_group_delims_len
    );

    output_group_delims.resize(output_group_delims_len);
    return output_group_delims;
  }

  template <typename T, bool sort_ascending = true>
  inline const std::vector<size_t> sort_and_group_multiple(std::vector<T>             & sort_data,
                                                           std::vector<size_t>        & index_data,
                                                           const std::vector<size_t>  & input_group_delims) {
    return sort_and_group_multiple<T, sort_ascending>(
      sort_data.data(),
      sort_data.size(),
      index_data.data(),
      input_group_delims.data(),
      input_group_delims.size()
    );
  }
}
