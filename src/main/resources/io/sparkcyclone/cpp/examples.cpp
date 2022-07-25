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
#include "cyclone/cyclone.hpp"
#include "frovedis/core/radix_sort.hpp"
#include "frovedis/core/set_operations.hpp"
#include "frovedis/core/utility.hpp"
#include "frovedis/dataframe/join.hpp"
#include <array>
#include <bitset>
#include <cmath>
#include <iostream>
#include <string>
#include <tuple>
#include <vector>

using namespace cyclone;

nullable_varchar_vector * project_eval(const nullable_varchar_vector *input_0)  {
  auto output_0_input_words = input_0->to_words();
  std::vector<size_t> output_0_starts(input_0->count);
  std::vector<size_t> output_0_lens(input_0->count);

  for ( int32_t i = 0; i < input_0->count; i++ ) {
    output_0_starts[i] = output_0_input_words.starts[i];
    output_0_lens[i] = output_0_input_words.lens[i];
  }

  std::vector<size_t> output_0_new_starts;
  std::vector<int> output_0_new_chars = frovedis::concat_words(
    output_0_input_words.chars,
    (const std::vector<size_t>&)(output_0_starts),
    (const std::vector<size_t>&)(output_0_lens),
    "",
    (std::vector<size_t>&)(output_0_new_starts)
  );

  output_0_input_words.chars = output_0_new_chars;
  output_0_input_words.starts = output_0_new_starts;
  output_0_input_words.lens = output_0_lens;

  auto *output_0 = new nullable_varchar_vector(output_0_input_words);

  for ( int i = 0; i < output_0->count; i++ ) {
    output_0->set_validity(i, input_0->get_validity(i));
  }

  return output_0;
}

void filter_test() {
  std::vector<std::string> data { "AIR", "MAIL", "RAIL", "SHIP", "TRUCK", "REG AIR", "FOB" };
  std::vector<size_t> selected_ids { 1, 3, 5 };

  auto *input = new nullable_varchar_vector(data);
  input->set_validity(3, 0);

  std::cout << "================================================================================" << std::endl;
  std::cout << "FILTER TEST\n" << std::endl;

  std::cout << "Original nullable_varchar_vector:" << std::endl;
  input->print();

  const auto *output = input->select(selected_ids);
  std::cout << "Filtered nullable_varchar_vector:" << std::endl;
  output->print();

  std::cout << "================================================================================" << std::endl;
}

void projection_test() {
  std::vector<std::string> data { "AIR", "MAIL", "RAIL", "SHIP", "TRUCK", "REG AIR", "FOB" };

  auto *input = new nullable_varchar_vector(data);
  input->set_validity(3, 0);

  std::cout << "================================================================================" << std::endl;
  std::cout << "PROJECTION TEST\n" << std::endl;

  std::cout << "Original nullable_varchar_vector:" << std::endl;
  input->print();

  auto *output = project_eval(input);
  std::cout << "Cloned nullable_varchar_vector:" << std::endl;
  output->print();

  std::cout << "================================================================================" << std::endl;
}

void test_sort1() {
  const std::vector<std::tuple<int32_t, float, int64_t, double>> elements {
    std::make_tuple(1, 2.106764f,   2ll, 2.029292l),
    std::make_tuple(0, 7.29214f,    3ll, 1.6248848l),
    std::make_tuple(2, 4.0789514f,  3ll, 5.4606824l),
    std::make_tuple(2, 2.1760006f,  6ll, 7.483787l),
  };

  const auto sorted_indices = sort_tuples(elements, std::array<int, 4> {{ 1, 1, 1, 1 }});

  std::cout << "================================================================================" << std::endl;
  std::cout << "SORT TEST\n" << std::endl;
  std::cout << sorted_indices << std::endl;
  std::cout << "================================================================================" << std::endl;
}

void test_sort2() {
  std::vector<int64_t>  data1 { 106, 951, 586,  };
  std::vector<float>    data2 { 3.14, 2.71, 42.0, };
  std::vector<int32_t>  data3 { 586, 951, 106, };

  const auto sorted_indices = cyclone::sort_columns(
    3,
    std::make_tuple(1, data1.data()),
    std::make_tuple(1, data2.data()),
    std::make_tuple(1, data3.data())
  );

  std::cout << "================================================================================" << std::endl;
  std::cout << "SORT TEST\n" << std::endl;
  std::cout << sorted_indices << std::endl;
  std::cout << "================================================================================" << std::endl;
}

void test_lambda() {
  std::vector<int32_t> input { 0, 1, 2, 3, 3, 4 };
  const auto condition = [&] (const size_t i) {
    return input[i] % 2 == 0;
  };

  const auto *expected = new nullable_varchar_vector(std::vector<std::string> { "foobar", "baz", "foobar", "baz", "baz", "foobar" });
  const auto *output = nullable_varchar_vector::from_binary_choice(input.size(), condition, "foobar", "baz");
  expected->print();
  output->print();
}

void test_statement_expressions() {
  auto x = ({
    int y = 10;
    y + 32;
  });

  ({
    std::cout << "Did this get executed?" << std::endl;
    21;
  });

  auto tmp = 10;

  auto y = ({
    std::cout << "Testing the condition" << std::endl;
    tmp < 20;
  }) ? (({
    std::cout << "Reached the true case" << std::endl;
    33;
  })) : ({
    std::cout << "Reached the false case" << std::endl;
    66;
  });

  auto t = 1;
  auto z = 2;
  z = t = 5;

  ( (x < 10) ) ? (({ x = (x * 3); })) : ({ x = 5; });

  std::cout << "================================================================================" << std::endl;
  std::cout << "STATEMENT EXPRESSIONS TEST\n" << std::endl;
  std::cout << "Basic test output: " << x << std::endl;
  std::cout << "If statement test output: " << y << std::endl;
  std::cout << "Multiple assignment test output: " << z << " " << t << std::endl;
  std::cout << "================================================================================" << std::endl;
}


template <typename T, bool sort_ascending = true>
inline const std::vector<size_t> multiple_radix_sort_and_group(T      * sort_data_arr,
                                                               size_t * index_data_arr,
                                                               const std::vector<size_t> & input_group_delims) {

  std::vector<std::vector<size_t>> component_group_delims(input_group_delims.size() - 1);

  // For each subset denoted by input_group_delims, perform sort + grouping
  // and get back a component delim group
  #pragma _NEC vector
  #pragma _NEC ivdep
  for (auto i = 1; i < input_group_delims.size(); i++) {
    // Fetch the boundaries of the range containing subset i
    const auto subset_start = input_group_delims[i-1];
    const auto subset_end   = input_group_delims[i];
    const auto subset_size  = subset_end - subset_start;

    if (subset_size < 2) {
      component_group_delims[i - 1] = {{ 0, 1 }};

    } else {
      // Sort the elements in subset i
      if constexpr (sort_ascending) {
        frovedis::radix_sort(&sort_data_arr[subset_start], &index_data_arr[subset_start], subset_size);
      } else {
        frovedis::radix_sort_desc(&sort_data_arr[subset_start], &index_data_arr[subset_start], subset_size);
      }

      // Construct the array of indices where the value of the keys change
      component_group_delims[i - 1] = frovedis::set_separate(&sort_data_arr[subset_start], subset_size);
    }
  }

  // Compute a prefix sum to get the offsets of the data elements
  std::vector<size_t> data_len_offsets(input_group_delims.size(), 0);
  #pragma _NEC vector
  for (auto i = 1; i < input_group_delims.size(); i++) {
    const auto subset_start = input_group_delims[i-1];
    const auto subset_end   = input_group_delims[i];
    data_len_offsets[i] = data_len_offsets[i - 1] + (subset_end - subset_start);
  }

  // Compute a prefix sum to get the offsets of the component delim groups
  std::vector<size_t> component_group_offsets(input_group_delims.size(), 0);
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

void group_strings(const nullable_varchar_vector *input) {
  // Fetch the validity vector and re-use the underyling data for sorting later on
  auto tmp = input->validity_vec();
  auto * sorted_data = tmp.data();

  // Set up the indices
  std::vector<size_t> index(input->count);
  #pragma _NEC vector
  #pragma _NEC ivdep
  for (auto i = 0; i < index.size(); i++) index[i] = i;

  // Set up the initial grouping, which is [0, count]
  std::vector<size_t> grouping {{ 0, static_cast<size_t>(input->count) }};

  {
    // STEP 1: Separate out the elements by those marked as valid vs invalid

    // Sort DESC by validity bits (valid values go left and invalid values
    // go right)
    grouping = multiple_radix_sort_and_group<int32_t, false>(sorted_data, index.data(), grouping);
    std::cout << "grouping: " << grouping << std::endl;

    // The returned grouping should have either 2 elements (all strings are
    // valid) or 3 elements (there is a partition separating valid and invalid
    // elements). Remove the last element if needed so we can focus on sorting
    // only the valid eleements
    if (grouping.size() > 2) {
      grouping.resize(2);
    }
  }

  {
    // STEP 2: From here on out, we are working only with the subset of the
    // elements that are valid.  Sort by the element lengths

    // Collect the element lengths into sorted_data
    #pragma _NEC vector
    #pragma _NEC ivdep
    for (auto i = 0; i < grouping.back(); i++) {
      sorted_data[i] = input->lengths[index[i]];
    }

    // Sort ASC by element length
    grouping = multiple_radix_sort_and_group<int32_t, true>(sorted_data, index.data(), grouping);
  }

  {
    // STEP 3: Iterate over n, where n = max length of a valid element, and sort
    // ASC by the ith character of each element in each iteration.  The grouping
    // will be constructed slowly over each iteration

    // Compute the length of the largest valid element
    auto maxlen = 0;
    #pragma _NEC vector
    for (auto i = 0; i < grouping.back(); i++) {
      auto len = input->lengths[index[i]];
      if (len > maxlen) {
        maxlen = len;
      }
    }

    // Iterate the sorting over each element and accumulate the new grouping
    // with each iteration
    for (auto pos = 0; pos < maxlen; pos++) {
      // Collect the pos-th character of every valid string into sorted_data
      #pragma _NEC vector
      #pragma _NEC ivdep
      for (auto i = 0; i < grouping.back(); i++) {
        auto j = index[i];
        sorted_data[i] = (pos < input->lengths[j]) ? input->data[input->offsets[j] + pos] : -1;
      }

      // Sort ASC by elem[pos]. using the existing grouping
      grouping = multiple_radix_sort_and_group<int32_t, true>(sorted_data, index.data(), grouping);
    }
  }

  std::cout << "index: " << index << std::endl;
  std::cout << "grouping: " << grouping << std::endl;
  input->select(index)->print();
}

void test_grouping() {
  std::vector<int32_t>      input { 0, 1, 1, 1, 1, 1, 2, 3, 0, 1, 6, 9, 6, };
  std::vector<size_t>       index(input.size());
  const std::vector<size_t> grouping {{ 0, 5, 10, 13 }};
  for (auto i = 0; i < index.size(); i++) index[i] = i;

  std::cout << "input: " << input << std::endl;
  std::cout << "index: " << index << std::endl;
  std::cout << "grouping: " << grouping << std::endl;

  auto new_grouping = multiple_radix_sort_and_group(input.data(), index.data(), grouping);

  std::cout << "values after grouping: " << input << std::endl;
  std::cout << "new grouping: " << new_grouping << std::endl;
}

void test_grouping_desc() {
  std::vector<int32_t>      input { 0, 1, 1, 1, 1, 1, 2, 3, 0, 1, 6, 9, 6, };
  std::vector<size_t>       index(input.size());
  const std::vector<size_t> grouping {{ 0, 5, 10, 13 }};
  for (auto i = 0; i < index.size(); i++) index[i] = i;

  std::cout << "input: " << input << std::endl;
  std::cout << "index: " << index << std::endl;
  std::cout << "grouping: " << grouping << std::endl;

  auto new_grouping = multiple_radix_sort_and_group<int32_t, false>(input.data(), index.data(), grouping);

  std::cout << "values after grouping: " << input << std::endl;
  std::cout << "new grouping: " << new_grouping << std::endl;
}

int main() {
  // projection_test();
  // filter_test();
  // test_sort1();
  // test_sort2();
  // test_lambda();
  // test_statement_expressions();

  // test_grouping();
  // test_grouping_desc();

  auto vec1 = nullable_varchar_vector(std::vector<std::string> { "JAN", "JANU", "FEBU", "FEB", "MARCH", "MARCG", "APR", "JANU", "SEP", "OCT", "NOV", "DEC" });
  vec1.set_validity(3, 0);
  vec1.set_validity(9, 0);
  vec1.print();
  group_strings(&vec1);
}
