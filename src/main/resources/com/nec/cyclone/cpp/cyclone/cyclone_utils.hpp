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

#include <stddef.h>
#include <iostream>
#include <tuple>
#include <vector>
#include "frovedis/core/radix_sort.hpp"
#include "frovedis/core/set_operations.hpp"
#include "frovedis/dataframe/join.hpp"

namespace cyclone {
  template<typename T>
  void print_vec(char *name, std::vector<T> a) {
        std::cout << name << " = [";
        std::string comma = "";
        for (int i = 0; i < a.size(); i++) {
            std::cout << comma << a[i];
            comma = ",";
        }
        std::cout << "]" << std::endl;
  }

  inline const std::vector<std::vector<size_t>> separate_to_groups(const std::vector<size_t> &ids, std::vector<size_t> &group_keys) {
    std::vector<size_t> groups = ids;
    frovedis::radix_sort(groups);
    std::vector<size_t> unique_groups = frovedis::set_unique(groups);
    std::vector<size_t> group_counts(unique_groups.size());

    // Count the number of groups
    for (size_t g = 0; g < unique_groups.size(); g++) {
      size_t current_count = 0;
      size_t the_group = unique_groups[g];

      #pragma _NEC vector
      for (size_t i = 0; i < groups.size(); i++) {
        if (groups[i] == the_group) {
          current_count++;
        }
      }
      group_counts[g] = current_count;
    }

    std::vector<std::vector<size_t>> ret(unique_groups.size());
    for (size_t i = 0; i < ret.size(); i++) {
      ret[i].resize(group_counts[i]);
    }

    for (size_t g = 0; g < unique_groups.size(); g++) {
      size_t group_pos = 0;
      size_t group = unique_groups[g];
      #pragma _NEC vector
      for (size_t i = 0; i < ids.size(); i++) {
        if (ids[i] == group) {
          ret[g][group_pos++] = i;
        }
      }
    }

    group_keys = unique_groups;
    return ret;
  }

  /**
   * Creates groups based on multiple key columns
   * @param multiple_ids key columns
   * @param group_keys actual keys iff only a single key column is provided, surrogate keys otherwise
   * @return grouping based on all key columns
   */
  inline const std::vector<std::vector<size_t>> separate_to_groups_multi(const std::vector<std::vector<size_t>> &multiple_ids, std::vector<size_t> &group_keys) {
      auto key_col_count = multiple_ids.size();

      // Short-circuit on simple cases
      if(key_col_count == 1) return separate_to_groups(multiple_ids[0], group_keys);

      // Split given key columns into first and rest
      std::vector<size_t> first_keys = multiple_ids[0];
      std::vector<std::vector<size_t>> rest_keys = std::vector<std::vector<size_t>>(multiple_ids.begin() + 1, multiple_ids.end());
      auto rest_keys_count = rest_keys.size();

      // Create groups with the first key column
      std::vector<size_t> _keys;
      std::vector<std::vector<size_t>> groups = separate_to_groups(first_keys, _keys);

      // For every group: Apply grouping to rest of key columns
      // Create list of grouped key columns
      int group_count = groups.size();
      std::vector<std::vector<std::vector<size_t>>> grouped_rest_keys(group_count);
      for (int i = 0; i < group_count; ++i){
          std::vector<size_t> idxs = groups[i];
          grouped_rest_keys[i].resize(rest_keys_count);

          auto group_size = idxs.size();
          for (int rk = 0; rk < rest_keys_count; ++rk){
              std::vector<size_t> key_group(group_size);
              for (int j = 0; j < group_size; ++j){
                  key_group[j] = rest_keys[rk][idxs[j]];
              }
              grouped_rest_keys[i][rk] = key_group;
          }
      }


      // Recursive step: Apply grouping on all subgroups
      // Note: indexes returned in this step are relative to regrouped keys,
      //       therefore they need translation back to appropriate index space
      std::vector<std::vector<size_t>> result;
      for (int i = 0; i < group_count; ++i){
          std::vector<size_t> group_idxs = groups[i];
          std::vector<std::vector<size_t>> key_group = grouped_rest_keys[i];
          std::vector<size_t> _keys;
          std::vector<std::vector<size_t>> sub_groups = separate_to_groups_multi(key_group, _keys);

          // Convert indexes back
          for (const std::vector<size_t> &sub_group : sub_groups){
              auto group_size = sub_group.size();
              std::vector<size_t> converted(group_size);
              for (int j = 0; j < group_size; ++j){
                  converted[j] = group_idxs[sub_group[j]];
              }
              result.push_back(converted);
          }
      }

      auto final_group_count = result.size();
      std::vector<size_t> surrogate_keys(final_group_count);
      for (int i = 0; i < final_group_count; ++i){
          surrogate_keys[i] =  i;
      }

      group_keys = surrogate_keys;
      return result;
  }

  inline const std::vector<size_t> bitmask_to_matching_ids(const std::vector<size_t> &mask) {
    // Count the number of 1-bits
    size_t m_count = 0;
    #pragma _NEC vector
    for (int i = 0; i < mask.size(); i++) {
      m_count += mask[i];
    }

    // Allocate the output
    std::vector<size_t> output(m_count);

    // Append the positions for which the bit is 1
    // This loop will be vectorized on the VE as vector compress instruction (`vcp`)
    size_t pos = 0;
    #pragma _NEC vector
    for (int i = 0; i < mask.size(); i++) {
      if (mask[i]) {
        output[pos++] = i;
      }
    }

    return output;
  }

  inline void equi_join_indices(std::vector<size_t> &left, std::vector<size_t> &right, std::vector<size_t> &matchingLeft, std::vector<size_t> &matchingRight) {
      size_t left_len = left.size();
      size_t right_len = right.size();
      std::vector<size_t> left_idxs(left_len);
      std::vector<size_t> right_idxs(right_len);
      for(auto i = 0; i < left_len; i++){
          left_idxs[i] = i;
      }
      for(auto i = 0; i < right_len; i++){
          right_idxs[i] = i;
      }
      frovedis::equi_join(left, left_idxs, right, right_idxs, matchingLeft, matchingRight);
  }

  // Print out a std::tuple to ostream
  template<typename Ch, typename Tr, typename... Ts>
  auto& operator<<(std::basic_ostream<Ch, Tr> &stream,
                   std::tuple<Ts...> const &tup) {
    std::basic_stringstream<Ch, Tr> tmp;
    tmp << "(";
    // Based on: https://stackoverflow.com/questions/6245735/pretty-print-stdtuple
    std::apply(
      [&tmp] (auto&&... args) { ((tmp << args << ", "), ...); },
      tup
    );
    tmp.seekp(-2, tmp.cur);
    tmp << ")";
    return stream << tmp.str();
  }

  // Print out a std::vector to ostream
  // Define this AFTER defining operator<< for std::tuple
  // so that we can print std::vector<std::tuple<Ts...>>
  template<typename Ch, typename Tr, typename T>
  auto& operator<<(std::basic_ostream<Ch, Tr> &stream,
                   std::vector<T> const &vec) {
    std::basic_stringstream<Ch, Tr> tmp;
    tmp << "[ ";
    for (const auto &elem : vec) {
      tmp << elem << ", ";
    }
    tmp.seekp(-2, tmp.cur);
    tmp << " ]";
    return stream << tmp.str();
  }
}

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
