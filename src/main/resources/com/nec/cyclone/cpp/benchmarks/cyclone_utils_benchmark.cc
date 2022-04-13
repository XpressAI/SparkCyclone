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

  TEST_CASE("Benchmarking group by implementations") {
    NullableScalarVec<int64_t>* input = create_input(3500000, 150);
    
    ankerl::nanobench::Bench().run("tuple_group_by", [&]() {
      ankerl::nanobench::doNotOptimizeAway(tuple_group_by(input));
    });

    ankerl::nanobench::Bench().run("vector_group_by(with_validity)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_group_by_with_validity(input));
    });

    ankerl::nanobench::Bench().run("vector_group_by(no validity)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_group_by_no_validity(input));
    });
  }
}
