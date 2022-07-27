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

void group_strings(const nullable_varchar_vector *input) {
  // Fetch the validity vector and re-use the underyling data for sorting later on
  auto sorted_data = input->validity_vec();

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
    grouping = cyclone::grouping::sort_and_group_multiple<int32_t, false>(sorted_data, index, grouping);
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
    grouping = cyclone::grouping::sort_and_group_multiple<int32_t, true>(sorted_data, index, grouping);
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
      grouping = cyclone::grouping::sort_and_group_multiple<int32_t, true>(sorted_data, index, grouping);
    }
  }

  {
    // STEP 4: Iterate over n, where n = max length of a valid element, and sort
    grouping.resize(grouping.size() + 1);
    grouping.back() = input->count;
  }

  std::cout << "index: " << index << std::endl;
  std::cout << "grouping: " << grouping << std::endl;
  input->select(index)->print();

  std::vector<std::vector<size_t>> result(grouping.size() - 1);
  #pragma _NEC vector
  #pragma _NEC ivdep
  for (auto i = 1; i < grouping.size(); i++) {
    result[i - 1] = std::vector<size_t>(&index[grouping[i - 1]], &index[grouping[i]]);
  }
  std::cout << "result: " << result << std::endl;
  std::cout << "result: " << result.size() << std::endl;

  std::cout << "reference: " << input->group_indexes() << std::endl;
}

void test_grouping() {
  std::cout << "================================================================================" << std::endl;
  std::cout << "GROUPING TEST 1\n" << std::endl;

  std::vector<int32_t>      input { 0, 1, 1, 1, 1, 1, 2, 3, 0, 1, 6, 9, 6, };
  std::vector<size_t>       index(input.size());
  const std::vector<size_t> grouping {{ 0, 5, 10, 13 }};
  for (auto i = 0; i < index.size(); i++) index[i] = i;

  std::cout << "input: " << input << std::endl;
  std::cout << "index: " << index << std::endl;
  std::cout << "grouping: " << grouping << std::endl;

  auto new_grouping = cyclone::grouping::sort_and_group_multiple2(input, index, grouping);

  std::cout << "values after grouping: " << input << std::endl;
  std::cout << "new grouping: " << new_grouping << std::endl;
  std::cout << "================================================================================" << std::endl;
}

void test_grouping_desc() {
  std::cout << "================================================================================" << std::endl;
  std::cout << "GROUPING TEST 2\n" << std::endl;

  std::vector<int32_t>      input { 0, 1, 1, 1, 1, 1, 2, 3, 0, 1, 6, 9, 6, };
  std::vector<size_t>       index(input.size());
  const std::vector<size_t> grouping {{ 0, 5, 10, 13 }};
  for (auto i = 0; i < index.size(); i++) index[i] = i;

  std::cout << "input: " << input << std::endl;
  std::cout << "index: " << index << std::endl;
  std::cout << "grouping: " << grouping << std::endl;

  auto new_grouping = cyclone::grouping::sort_and_group_multiple2<int32_t, false>(input, index, grouping);

  std::cout << "values after grouping: " << input << std::endl;
  std::cout << "new grouping: " << new_grouping << std::endl;
  std::cout << "================================================================================" << std::endl;
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

  auto input = std::vector<std::string> { "JAN", "JANU", "FEBU", "FEB", "MARCH", "MARCG", "APR", "APR", "JANU", "SEP", "OCT", "NOV", "DEC2", "DEC1", "DEC0" };
  auto vec1 = nullable_varchar_vector(input);
  vec1.set_validity(3, 0);
  vec1.set_validity(10, 0);
  // vec1.print();
  // group_strings(&vec1);
  // std::cout << "\n\n\n\n\n" << std::endl;

  // std::cout << "original" << vec1.group_indexes()  << std::endl;
  // std::cout << "new" << vec1.group_indexes3()  << std::endl;
  // std::cout << "new" << vec1.group_indexes2()  << std::endl;

  // std::cout << input << std::endl;
  vec1.group_indexes2();
}
