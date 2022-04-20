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
#include "tests/doctest.h"
#include <stddef.h>

namespace cyclone::tests {
  TEST_SUITE("NullableScalarVec<T>") {
    // Instantiate for each template case to be tested
    template<typename T> const std::vector<T> raw { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 };

    TEST_CASE_TEMPLATE("Allocate works for T=", T, int32_t, int64_t, float, double) {
      auto *vec1 = NullableScalarVec<T>::allocate();
      CHECK(vec1->is_default());
    }

    TEST_CASE_TEMPLATE("Creating a vector of N copies of the same value works for T=", T, int32_t, int64_t, float, double) {
      const auto size = 6;
      const auto value = raw<T>[3];

      auto *vec1 = NullableScalarVec<T>::constant(size, value);
      auto *vec2 = new NullableScalarVec(std::vector<T> { value, value, value, value, value, value });
      CHECK(vec1->equals(vec2));
    }

    TEST_CASE_TEMPLATE("Print works for T=", T, int32_t, int64_t, float, double) {
      auto *vec1 = new NullableScalarVec(raw<T>);
      vec1->set_validity(1, 0);
      vec1->set_validity(4, 0);
      vec1->set_validity(8, 0);

      vec1->print();
    }

    TEST_CASE_TEMPLATE("Equality checks work for T=", T, int32_t, int64_t, float, double) {
      auto *vec1 = new NullableScalarVec(raw<T>);
      vec1->set_validity(1, 0);
      vec1->set_validity(4, 0);
      vec1->set_validity(8, 0);

      // Different count
      auto *vec2 = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538, 620, 553 });

      // Different data
      auto *vec3 = new NullableScalarVec(raw<T>);
      vec3->data[3] = 184;

      // Different validityBuffer
      auto *vec4 = new NullableScalarVec(raw<T>);
      vec4->set_validity(3, 0);

      CHECK(vec1->equals(vec1));
      CHECK(not vec1->equals(vec2));
      CHECK(not vec1->equals(vec3));
      CHECK(not vec1->equals(vec4));
    }

    TEST_CASE_TEMPLATE("Check default works for T=", T, int32_t, int64_t, float, double) {
      auto *empty = new NullableScalarVec<T>;
      auto *nonempty = new NullableScalarVec(raw<T>);

      CHECK(empty->is_default());
      CHECK(not nonempty->is_default());
    }

    TEST_CASE_TEMPLATE("Reset works for T=", T, int32_t, int64_t, float, double) {
      auto *empty = new NullableScalarVec<T>;

      auto *input = new NullableScalarVec(raw<T>);
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      input->set_validity(8, 0);

      CHECK(not input->equals(empty));
      input->reset();
      CHECK(input->equals(empty));
    }

    TEST_CASE_TEMPLATE("Resize works for T=", T, int32_t, int64_t, float, double) {
      auto *vec = new NullableScalarVec<T>;
      CHECK(vec->is_default());

      vec->resize(10);
      CHECK(not vec->is_default());
      CHECK(vec->count == 10);
      CHECK(vec->data != nullptr);
      CHECK(vec->validityBuffer != nullptr);
    }

    TEST_CASE_TEMPLATE("Pseudo move assignment works for T=", T, int32_t, int64_t, float, double) {
      auto *input = new NullableScalarVec<T>;

      // Need to allocate by malloc() since it will be freed by free() in the pseudo move
      auto *copy1 = static_cast<NullableScalarVec<T> *>(malloc(sizeof(NullableScalarVec<T>)));
      new (copy1) NullableScalarVec(raw<T>);
      auto *copy2 = new NullableScalarVec(raw<T>);

      input->move_assign_from(copy1);
      CHECK(input->equals(copy2));
    }

    TEST_CASE_TEMPLATE("Clone works for T=", T, int32_t, int64_t, float, double) {
      auto *input = new NullableScalarVec(raw<T>);
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      input->set_validity(8, 0);

      auto *output = input->clone();
      CHECK(output != input);
      CHECK(output->equals(input));
    }

    TEST_CASE_TEMPLATE("Hash value works for T=", T, int32_t, int64_t, float, double) {
      auto *input = new NullableScalarVec(raw<T>);

      const auto output = input->hash_at(3, 42);
      CHECK(output == 31 * 42 + 318);
    }

    TEST_CASE_TEMPLATE("Get and Set validity bit works for T=", T, int32_t, int64_t, float, double) {
      auto *input = new NullableScalarVec(raw<T>);

      input->set_validity(4, 0);
      CHECK(input->get_validity(4) == 0);

      input->set_validity(4, 1);
      CHECK(input->get_validity(4) == 1);
    }

    TEST_CASE_TEMPLATE("Validity vector generation works for T=", T, int32_t, int64_t, float, double) {
      auto *input = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538, 620 });
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      std::vector<int32_t> expected { 1, 0, 1, 1, 0, 1 };

      CHECK(input->validity_vec() == expected);
    }

    TEST_CASE_TEMPLATE("size_t value vector generation works for T=", T, int32_t, int64_t) {
      auto *input = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538, 620 });
      std::vector<size_t> expected { 586, 951, 106, 318, 538, 620 };

      CHECK(input->size_t_data_vec() == expected);
    }

    TEST_CASE_TEMPLATE("Select works for T=", T, int32_t, int64_t, float, double) {
      auto *input = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 });
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      input->set_validity(8, 0);

      { // Subset filter
        const std::vector<size_t> selected_ids { 1, 2, 4, 7 };

        auto *expected = new NullableScalarVec(std::vector<T> { 951, 106, 538, 605 });
        expected->set_validity(0, 0);
        expected->set_validity(2, 0);

        const auto *output = input->select(selected_ids);
        CHECK(output != input);
        CHECK(output->equals(expected));
      }

      { // Subset filter with out of order indices
        const std::vector<size_t> selected_ids { 7, 2, 4, 1 };

        auto *expected = new NullableScalarVec(std::vector<T> { 605, 106, 538, 951 });
        expected->set_validity(2, 0);
        expected->set_validity(3, 0);

        const auto *output = input->select(selected_ids);
        CHECK(output != input);
        CHECK(output->equals(expected));
      }
    }

    TEST_CASE_TEMPLATE("Bucket works for T=", T, int32_t, int64_t, float, double) {
      auto *input = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 });
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      input->set_validity(8, 0);

      const std::vector<size_t> bucket_assignments { 1, 2, 0, 1, 1, 2, 0, 2, 2, 2 };
      const std::vector<size_t> bucket_counts { 2, 3, 5 };

      auto **output = input->bucket(bucket_counts, bucket_assignments);

      const std::vector<size_t> selected_ids_0 { 2, 6 };
      const std::vector<size_t> selected_ids_1 { 0, 3, 4, };
      const std::vector<size_t> selected_ids_2 { 1, 5, 7, 8, 9 };

      CHECK(output[0]->equals(input->select(selected_ids_0)));
      CHECK(output[1]->equals(input->select(selected_ids_1)));
      CHECK(output[2]->equals(input->select(selected_ids_2)));
    }

    TEST_CASE_TEMPLATE("Merge works for T=", T, int32_t, int64_t, float, double) {
      std::vector<NullableScalarVec<T> *> inputs(3);

      inputs[0] = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538 });
      inputs[0]->set_validity(1, 0);
      inputs[0]->set_validity(4, 0);

      inputs[1] = new NullableScalarVec(std::vector<T> { 620, 553, 605 });
      inputs[1]->set_validity(2, 0);

      inputs[2] = new NullableScalarVec(std::vector<T> { 46, 726, 563, 515, });
      inputs[2]->set_validity(3, 0);

      auto *expected = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538, 620, 553, 605, 46, 726, 563, 515 });
      expected->set_validity(1, 0);
      expected->set_validity(4, 0);
      expected->set_validity(7, 0);
      expected->set_validity(11, 0);

      const auto *output = NullableScalarVec<T>::merge(&inputs[0], 3);
      CHECK(output != expected);
      CHECK(output->equals(expected));
    }

    TEST_CASE_TEMPLATE("Evaluation of IN works for T=", T, int32_t, int64_t, float, double) {
      const auto *input = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538 });
      const std::vector<size_t> expected1 { 1, 0, 0, 0, 0 };
      const std::vector<size_t> expected2 { 0, 0, 1, 0, 1 };

      CHECK(input->eval_in(std::vector<T> { 586, 42 }) == expected1);
      CHECK(input->eval_in(std::vector<T> { 106, 538 }) == expected2);
    }
  }

  TEST_CASE_TEMPLATE("group_indexes works with all valid values", T, int32_t, int64_t, float, double){
    std::vector<T> grouping = { 10, 10, 11, 10, 10, 10, 10, 11, 11, 11, 11, 10, 10, 11, 11 };
    std::vector<size_t> expected_0 = { 0, 1, 3, 4, 5, 6, 11, 12 };
    std::vector<size_t> expected_1 = { 2, 7, 8, 9, 10, 13, 14 };

    const auto *input = new NullableScalarVec(grouping);
    auto grouped = input->group_indexes();

    cyclone::print_vec("grouped[0]", grouped[0]);
    cyclone::print_vec("grouped[1]", grouped[1]);

    CHECK(grouped[0] == expected_0);
    CHECK(grouped[1] == expected_1);
    CHECK(grouped.size() == 2);


  }

  TEST_CASE_TEMPLATE("group_indexes works with all valid values (3 groups)", T, int32_t, int64_t, float, double){
    std::vector<T> grouping = { 10, 10, 11, 12, 10, 10, 10, 11, 11, 11, 12, 10, 10, 11, 11 };
    std::vector<size_t> expected_0 = { 0, 1, 4, 5, 6, 11, 12 };
    std::vector<size_t> expected_1 = { 2, 7, 8, 9, 13, 14 };
    std::vector<size_t> expected_2 = { 3, 10 };

    const auto *input = new NullableScalarVec(grouping);
    auto grouped = input->group_indexes();

    cyclone::print_vec("grouped[0]", grouped[0]);
    cyclone::print_vec("grouped[1]", grouped[1]);
    cyclone::print_vec("grouped[2]", grouped[2]);

    CHECK(grouped[0] == expected_0);
    CHECK(grouped[1] == expected_1);
    CHECK(grouped[2] == expected_2);
    CHECK(grouped.size() == 3);
  }

  TEST_CASE_TEMPLATE("group_indexes works with some invalid values", T, int32_t, int64_t, float, double){
    std::vector<T> grouping = { 10, 10, 11, 10, 10, 10, 10, 11, 11, 11, 11, 10, 10, 11, 11 };
    std::vector<size_t> expected_0 = { 1, 3, 4, 5, 6, 11, 12 };
    std::vector<size_t> expected_1 = { 7, 8, 9, 10, 13, 14 };
    std::vector<size_t> expected_2 = { 2, 0 };

    auto *input = new NullableScalarVec(grouping);
    input->set_validity(0, 0);
    input->set_validity(2, 0);

    auto grouped = input->group_indexes();

    cyclone::print_vec("grouped[0]", grouped[0]);
    cyclone::print_vec("grouped[1]", grouped[1]);
    cyclone::print_vec("grouped[2]", grouped[2]);

    CHECK(grouped[0] == expected_0);
    CHECK(grouped[1] == expected_1);
    CHECK(grouped[2] == expected_2);
    CHECK(grouped.size() == 3);
  }

  TEST_CASE("group_indexes_on_subset works (with different types)"){
    std::vector<long> grouping_1 = { 10, 10, 11, 10, 10, 10, 10, 11, 11, 11, 11, 10, 10, 11, 11 };
    auto *input1 = new NullableScalarVec(grouping_1);

    std::vector<double> grouping_2 = { 10,  7, 11,  7,  5, 10, 10,  5, 11,  3, 11,  3, 10, 11,  7 };
    auto *input2 = new NullableScalarVec(grouping_2);

    std::vector<int> grouping_3 = {  1,  8,  1,  8,  1,  8,  1,  8,  1,  8,  1,  8,  1,  8,  1 };
    auto *input3 = new NullableScalarVec(grouping_3);

    std::vector<std::vector<size_t>> expected = {{11}, {4}, {1, 3}, {0, 6, 12}, {5}, {9}, {7}, {14}, {2, 8, 10}, {13}};

    size_t count = grouping_1.size();
    size_t* a_arr = static_cast<size_t *>(malloc(sizeof(size_t) * count));
    size_t* b_arr = static_cast<size_t *>(malloc(sizeof(size_t) * count));

    std::vector<size_t> a_pos_idxs;
    std::vector<size_t> b_pos_idxs;

    input1->group_indexes_on_subset(nullptr, {0, count}, a_arr, a_pos_idxs);
    cyclone::print_vec("first grouping", a_pos_idxs);
    std::cout << "Result Array: ";
    for(auto i = 0; i < count; i++){
      std::cout << a_arr[i] << " ";
    }
    std::cout << std::endl;
    input2->group_indexes_on_subset(a_arr, a_pos_idxs, b_arr, b_pos_idxs);
    cyclone::print_vec("second grouping", b_pos_idxs);
    std::cout << "Result Array: ";
    for(auto i = 0; i < count; i++){
      std::cout << b_arr[i] << " ";
    }
    std::cout << std::endl;
    input3->group_indexes_on_subset(b_arr, b_pos_idxs, a_arr, a_pos_idxs);
    cyclone::print_vec("third grouping", a_pos_idxs);

    std::cout << "Result Array: ";
    for(auto i = 0; i < count; i++){
      std::cout << a_arr[i] << " ";
    }
    std::cout << std::endl;

    cyclone::print_vec("a_pos_idxs", a_pos_idxs);

    std::vector<std::vector<size_t>> result;
    for(auto g = 1; g < a_pos_idxs.size(); g++){
      std::vector<size_t> output_group(&a_arr[a_pos_idxs[g - 1]], &a_arr[a_pos_idxs[g]]);
      result.push_back(output_group);
    }
    cyclone::print_vec("result", result);

    free(a_arr);
    free(b_arr);
    free(input1);
    free(input2);
    free(input3);

    CHECK(result == expected);
  }
}
