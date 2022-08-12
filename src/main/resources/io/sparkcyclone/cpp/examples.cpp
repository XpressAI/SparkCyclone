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

void test_multiple_grouping() {
  std::cout << "================================================================================" << std::endl;
  std::cout << "GROUPING TEST\n" << std::endl;

  std::vector<int32_t>      input1 { 23, 0, 1, 4, 3, -2, 1, 5, 3, 0, 1, 6, 9, 6, 42, -100 };
  const std::vector<size_t> grouping {{ 1, 6, 11, 14 }};
  std::vector<size_t>       index1(input1.size());
  for (auto i = 0; i < index1.size(); i++) index1[i] = i;

  auto input2 = input1;
  auto index2 = index1;

  std::cout << "input: " << input1 << std::endl;
  std::cout << "index: " << index1 << std::endl;
  std::cout << "grouping: " << grouping << std::endl;

  auto new_grouping1 = cyclone::grouping::sort_and_group_multiple<int32_t, true>(input1, index1, grouping);
  auto new_grouping2 = cyclone::grouping::sort_and_group_multiple<int32_t, false>(input2, index2, grouping);

  std::cout << std::endl;
  std::cout << "new values1: " << input1 << std::endl;
  std::cout << "new index1: " << index1 << std::endl;
  std::cout << "new grouping1: " << new_grouping1 << std::endl;

  std::cout << std::endl;
  std::cout << "new values2: " << input2 << std::endl;
  std::cout << "new index2: " << index1 << std::endl;
  std::cout << "new grouping2: " << new_grouping2 << std::endl;
  std::cout << "================================================================================" << std::endl;
}

void test_scalar_grouping() {
  auto input = std::vector<int32_t> { 77, 1, 2, 2, 3, 3, 4, 11, 3, 9, 9, 4, 1, 9, 10, 11, 12, 42, -8 };
  auto vec1 = new NullableScalarVec<int32_t>(input);
  vec1->set_validity(7, 0);
  vec1->set_validity(11, 0);
  vec1->set_validity(13, 0);


  // Sort 3 subsets separately
  auto input_group_delims = std::vector<size_t> { 3, 8, 14, 17 };

  // Set up output
  std::vector<size_t> output_index(vec1->count);
  std::vector<size_t> output_group_delims(vec1->count + 1);
  size_t output_group_delims_len;

  // Group indices
  vec1->group_indexes_on_subset(
    nullptr,
    input_group_delims.data(),
    input_group_delims.size(),
    output_index.data(),
    output_group_delims.data(),
    output_group_delims_len
  );

  // Adjust the output
  output_group_delims.resize(output_group_delims_len);

  std::cout << input << std::endl;
  std::cout << input_group_delims << std::endl;
  vec1->print();
  vec1->select(output_index)->print();
  std::cout << output_group_delims << std::endl;
}
void test_varchar_grouping() {
  auto input = std::vector<std::string> { "JAN", "JANU", "FEBU", "FEB", "MARCH", "MARCG", "APR", "NOV", "MARCG", "SEPT", "SEPT", "APR", "JANU", "SEP", "OCT", "NOV", "DEC2", "DEC1", "DEC0" };
  auto vec1 = new nullable_varchar_vector(input);
  vec1->set_validity(7, 0);
  vec1->set_validity(11, 0);
  vec1->set_validity(13, 0);

  // Sort 3 subsets separately
  auto input_group_delims = std::vector<size_t> { 3, 8, 14, 17 };

  // Set up output
  std::vector<size_t> output_index(vec1->count);
  std::vector<size_t> output_group_delims(vec1->count + 1);
  size_t output_group_delims_len;

  // Group indices
  vec1->group_indexes_on_subset(
    nullptr,
    input_group_delims.data(),
    input_group_delims.size(),
    output_index.data(),
    output_group_delims.data(),
    output_group_delims_len
  );

  // Adjust the output
  output_group_delims.resize(output_group_delims_len);

  std::cout << input << std::endl;
  std::cout << input_group_delims << std::endl;
  vec1->print();
  vec1->select(output_index)->print();
  std::cout << output_group_delims << std::endl;
}


void foo(const std::string_view &input) {
  std::cout << "inside foo " << input << "\n";
}

int main() {
  // projection_test();
  // filter_test();
  // test_sort1();
  // test_sort2();
  // test_lambda();
  // test_statement_expressions();

  // test_multiple_grouping();
  // test_scalar_grouping();
  // test_varchar_grouping();
  auto start = cyclone::time::now();
  std::cout << cyclone::time::utc() << std::endl;

  auto duration = cyclone::time::nanos_since(start);
  std::cout << duration << " ns" << std::endl;

  cyclone::log::strace << "trace message" << std::endl;
  cyclone::log::sdebug << "debug message" << std::endl;
  cyclone::log::sinfo << "info message" << std::endl;
  cyclone::log::swarn << "warn message" << std::endl;
  cyclone::log::serror << "error message" << std::endl;
  cyclone::log::sfatal << "fatal message" << std::endl;

  int i = 3;
  float f = 5.f;
  char* s0 = "hello";
  std::string s1 = "world";


  cyclone::log::trace("trace message");
  cyclone::log::debug("debug message");
  cyclone::log::info("i=%d, f=%f, s=%s %s", i, f, s0, s1);
  cyclone::log::warn("warn message");
  cyclone::log::error("error message");
  cyclone::log::fatal("fatal message");

  auto output = cyclone::io::format("i=%d, f=%f, s=%s %s", i, f, s0, s1);
  auto expected = std::string("i=3, f=5.000000, s=hello world");
  foo(cyclone::io::format("i=%d, f=%f, s=%s %s", i, f, s0, s1));
  std::cout << (output == expected) << "\n";
}
