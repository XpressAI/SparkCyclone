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
#include "benchmarks/nanobench.h"
#include "tests/doctest.h"
#include "frovedis/core/set_operations.hpp"
#include "cyclone/cyclone.hpp"


namespace cyclone::benchmarks {
  NullableScalarVec<int64_t>* create_input(int vector_size, int group_count){
    NullableScalarVec<int64_t>* res = NullableScalarVec<int64_t>::allocate();
    res->resize(vector_size);
    for(auto i = 0; i < vector_size; ++i){
      res->data[i] = i % group_count;
      res->set_validity(i, 1);
    }
    
    return res;
  }

  size_t tuple_group_by(NullableScalarVec<int64_t> *input){
    std::vector<std::tuple<int64_t, int>> grouping_vec(input->count);
    std::vector<size_t> sorted_idx(input->count);

    for (auto i = 0; i < input->count; i++) {
      grouping_vec[i] = std::tuple<int64_t, int>(input->data[i], input->get_validity(i));
    }

    sorted_idx = cyclone::sort_tuples(grouping_vec, std::array<int, 2>{{ 1, 1 }});

    for (auto j = 0; j < input->count; j++) {
      auto i = sorted_idx[j];
      grouping_vec[j] = std::tuple<int64_t, int>(input->data[i], input->get_validity(i));
    }

    std::vector<size_t> groups_indices = frovedis::set_separate(grouping_vec);
    auto groups_count = groups_indices.size() - 1;

    std::vector<size_t> matching_ids(groups_count);
    for (auto g = 0; g < groups_count; g++) {
      matching_ids[g] = sorted_idx[groups_indices[g]];
    }

    return matching_ids.size();
  }

  size_t vector_group_by_with_validity(NullableScalarVec<int64_t> *input){
    std::vector<std::vector<size_t>> input_keys(2);
    input_keys[0] = input->size_t_data_vec();
    input_keys[1] = input->size_t_validity_vec();

    std::vector<size_t> keys;
    std::vector<std::vector<size_t>> groups = cyclone::separate_to_groups_multi(input_keys, keys);
    return groups.size();
  }

  size_t vector_group_by_no_validity(NullableScalarVec<int64_t> *input){
    std::vector<std::vector<size_t>> input_keys(1);
    input_keys[0] = input->size_t_data_vec();

    std::vector<size_t> keys;
    std::vector<std::vector<size_t>> groups = cyclone::separate_to_groups_multi(input_keys, keys);
    return groups.size();
  }


  size_t vector_group(NullableScalarVec<int64_t> *input){
    std::vector<std::vector<size_t>> groups = input->group_indexes();
    return groups.size();
  }

    size_t vector_multi_group(NullableScalarVec<int64_t> *input1, NullableScalarVec<int64_t> *input2, NullableScalarVec<int64_t> *input3){
      size_t count = static_cast<size_t>(input1->count);
      std::vector<size_t> all_group = {0, count};
      size_t* idx1_arr = static_cast<size_t *>(malloc(sizeof(size_t) * count));
      size_t* idx2_arr = static_cast<size_t *>(malloc(sizeof(size_t) * count));

      std::vector<size_t> group1_pos_idxs;
      std::vector<size_t> group2_pos_idxs;

      input1->group_indexes_on_subset(nullptr, all_group, idx1_arr, group1_pos_idxs);
      input2->group_indexes_on_subset(idx1_arr, group1_pos_idxs, idx2_arr, group2_pos_idxs);
      input3->group_indexes_on_subset(idx2_arr, group2_pos_idxs, idx1_arr, group1_pos_idxs);

      std::vector<std::vector<size_t>> result;

      #pragma _NEC vector
      for(auto g = 1; g < group1_pos_idxs.size(); g++){
        std::vector<size_t> output_group(&idx1_arr[group1_pos_idxs[g - 1]], &idx1_arr[group1_pos_idxs[g]]);
        result.push_back(output_group);
      }

      free(idx1_arr);
      free(idx2_arr);

      return result.size();
    }

  TEST_CASE("Benchmarking group by implementations") {
    NullableScalarVec<int64_t>* input = create_input(3500000, 150);
    NullableScalarVec<int64_t>* input_with_invalids = create_input(3500000, 150);
    input_with_invalids->set_validity(1, 0);

    ankerl::nanobench::Bench().run("tuple_group_by", [&]() {
      ankerl::nanobench::doNotOptimizeAway(tuple_group_by(input));
    });

    ankerl::nanobench::Bench().run("vector_group_by(with_validity)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_group_by_with_validity(input));
    });

    ankerl::nanobench::Bench().run("vector_group_by(no validity)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_group_by_no_validity(input));
    });

    ankerl::nanobench::Bench().run("vector_group(with validity, all valid input)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_group(input));
    });

    ankerl::nanobench::Bench().run("vector_group(with validity, some invalid input)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_group(input_with_invalids));
    });

    ankerl::nanobench::Bench().run("tuple_group_by (some invalid input)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(tuple_group_by(input_with_invalids));
    });

    ankerl::nanobench::Bench().run("vector_group_by(with_validity, some invalid input)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_group_by_with_validity(input_with_invalids));
    });

    free(input);
    free(input_with_invalids);

    NullableScalarVec<int64_t>* input1 = create_input(3500000,   69);
    NullableScalarVec<int64_t>* input2 = create_input(3500000,  420);
    NullableScalarVec<int64_t>* input3 = create_input(3500000, 9001);

    ankerl::nanobench::Bench().run("vector_multi_group", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_multi_group(input1, input2, input3));
    });
  }
}
