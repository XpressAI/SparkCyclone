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

namespace cyclone::tests {
  TEST_SUITE("nullable_varchar_vector") {
    TEST_CASE("Allocate works") {
      auto *vec1 = nullable_varchar_vector::allocate();
      CHECK(vec1->is_default());
    }

    TEST_CASE("Creating a vector of N copies of the same value works") {
      const auto size = 6;
      const auto value = "FOOBAR";

      auto *vec1 = nullable_varchar_vector::constant(size, value);
      auto *vec2 = new nullable_varchar_vector(std::vector<std::string> { value, value, value, value, value, value });
      CHECK(vec1->equivalent_to(vec2));
    }

    TEST_CASE("Print works") {
      std::vector<std::string> raw { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };
      auto *vec1 = new nullable_varchar_vector(raw);
      vec1->set_validity(1, 0);
      vec1->set_validity(4, 0);
      vec1->set_validity(10, 0);

      vec1->print();
    }

    TEST_CASE("Equality checks work") {
      std::vector<std::string> raw { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };
      auto *vec1 = new nullable_varchar_vector(raw);
      vec1->set_validity(1, 0);
      vec1->set_validity(4, 0);
      vec1->set_validity(10, 0);

      // Different content
      auto *vec2 = new nullable_varchar_vector(std::vector<std::string> { "MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN" });

      // Different validityBuffer
      auto *vec3 = new nullable_varchar_vector(raw);
      vec3->set_validity(7, 0);

      CHECK(vec1->equals(vec1));
      CHECK(not vec1->equals(vec2));
      CHECK(not vec1->equals(vec3));
    }

    TEST_CASE("Equivalence checks work") {
      auto *vec1 = new nullable_varchar_vector(std::vector<std::string> { "FOO", "BAR", "FOO" });
      vec1->set_validity(1, 0);

      auto *vec2 = new nullable_varchar_vector;
      {
        std::vector<int32_t> letters { 'F', 'O', 'O', 'B', 'A', 'R' };

        vec2->count = 3;
        vec2->dataSize = 6;

        vec2->data = static_cast<int32_t *>(malloc(letters.size() * sizeof(int32_t)));
        for (auto i = 0; i < letters.size(); i++) {
          vec2->data[i] = letters[i];
        }

        vec2->offsets = static_cast<int32_t *>(calloc(sizeof(int32_t) * 3, 1));
        vec2->offsets[0] = 0;
        vec2->offsets[1] = 3;
        vec2->offsets[2] = 0;

        vec2->lengths = static_cast<int32_t *>(calloc(sizeof(int32_t) * 3, 1));
        vec2->lengths[0] = 3;
        vec2->lengths[1] = 3;
        vec2->lengths[2] = 3;

        size_t vcount = frovedis::ceil_div(vec2->count, int32_t(64));
        vec2->validityBuffer = static_cast<uint64_t *>(malloc(vcount * sizeof(uint64_t)));
        for (auto i = 0; i < vcount; i++) {
          vec2->validityBuffer[i] = 0xffffffffffffffff;
        }
        vec2->set_validity(1, 0);
      }

      CHECK(vec1->equivalent_to(vec1));
      CHECK(vec1->equivalent_to(vec2));
    }

    TEST_CASE("Conversions between nullable_varchar_vector and frovedis::words work (ignoring the validity buffer)") {
      const auto *input = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "", "OCT", "NOV", "DEC" });
      const auto input_as_words = input->to_words();

      auto *output = nullable_varchar_vector::from_words(input_as_words);
      CHECK(output != input);
      CHECK(output->equals(input));
    }

    TEST_CASE("Check default works") {
      auto *empty = new nullable_varchar_vector;
      auto *nonempty = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "", "OCT", "NOV", "DEC" });

      CHECK(empty->is_default());
      CHECK(not nonempty->is_default());
    }

    TEST_CASE("Reset works") {
      auto *empty = new nullable_varchar_vector;

      auto *input = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "", "OCT", "NOV", "DEC" });
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      input->set_validity(10, 0);

      CHECK(not input->equals(empty));
      input->reset();
      CHECK(input->equals(empty));
    }

    TEST_CASE("Pseudo move assignment works") {
      auto raw = std::vector<std::string> { "FEB", "MAR", "MAY", "", "DEC" };
      auto *input = new nullable_varchar_vector;

      // Need to allocate by malloc() since it will be freed by free() in the pseudo move
      auto *copy1 = static_cast<nullable_varchar_vector *>(malloc(sizeof(nullable_varchar_vector)));
      new (copy1) nullable_varchar_vector(raw);
      auto *copy2 = new nullable_varchar_vector(raw);

      input->move_assign_from(copy1);
      CHECK(input->equals(copy2));
    }

    TEST_CASE("Clone works") {
      // Include empty string value
      auto *input = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "", "OCT", "NOV", "DEC" });
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      input->set_validity(10, 0);

      auto *output = input->clone();
      CHECK(output != input);
      CHECK(output->equals(input));
    }

    TEST_CASE("Hash value works") {
      auto *input = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "", "OCT", "NOV", "DEC" });

      const auto seed = 42;
      const auto output = input->hash_at(3, seed);
      CHECK(output == 31 * (31 * (31 * seed + 'A') + 'P') + 'R');
    }

    TEST_CASE("Hash vector generation works") {
      auto *input = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR" });

      const auto seed = 1;
      std::vector<int64_t> expected {
        31 * (31 * (31 * seed + 'J') + 'A') + 'N',
        31 * (31 * (31 * seed + 'F') + 'E') + 'B',
        31 * (31 * (31 * seed + 'M') + 'A') + 'R'
      };

      const auto output = input->hash_vec();
      CHECK(output == expected);
    }

    TEST_CASE("Get and Set validity bit works") {
      auto *input = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "", "OCT", "NOV", "DEC" });

      input->set_validity(4, 0);
      CHECK(input->get_validity(4) == 0);

      input->set_validity(4, 1);
      CHECK(input->get_validity(4) == 1);
    }

    TEST_CASE("Validity vector generation works") {
      auto *input = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "", "JUN" });
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      std::vector<int32_t> expected { 1, 0, 1, 1, 0, 1 };

      CHECK(input->validity_vec() == expected);
    }

    TEST_CASE("Select works") {
      // Include empty string value
      auto *input = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "", "SEP", "OCT", "NOV", "DEC" });
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      input->set_validity(10, 0);

      const std::vector<size_t> selected_ids { 1, 2, 4, 7, 11 };

      auto *expected = new nullable_varchar_vector(std::vector<std::string> { "FEB", "MAR", "MAY", "", "DEC" });
      expected->set_validity(0, 0);
      expected->set_validity(2, 0);

      const auto *output = input->select(selected_ids);
      CHECK(output != input);
      CHECK(output->equals(expected));
    }

    TEST_CASE("Select works (for nullable_varchar_vector backed by a compressed data encoding)") {
      std::vector<int32_t> input { 0, 1, 2, 3, 3, 4 };
      const auto condition = [&] (const size_t i) -> bool {
        return input[i] % 2 == 0;
      };

      auto *vec = nullable_varchar_vector::from_binary_choice(input.size(), condition, "YES", "NO");
      vec->set_validity(0, 0);
      vec->set_validity(2, 0);

      auto *expected1 = new nullable_varchar_vector(std::vector<std::string> { "YES", "NO", "YES", "NO", "NO", "YES" });
      expected1->set_validity(0, 0);
      expected1->set_validity(2, 0);
      CHECK(vec->equivalent_to(expected1));

      const std::vector<size_t> selected_ids { 5, 2, 4, 0 };
      const auto *output = vec->select(selected_ids);

      auto *expected2 = new nullable_varchar_vector(std::vector<std::string> { "YES", "YES", "NO", "YES" });
      expected2->set_validity(1, 0);
      expected2->set_validity(3, 0);
      CHECK(output->equivalent_to(expected2));
    }

    TEST_CASE("Bucket works") {
      // Include empty string value
      auto *input = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "", "SEP", "OCT", "NOV", "DEC" });
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      input->set_validity(10, 0);

      const std::vector<size_t> bucket_assignments { 1, 2, 0, 1, 1, 2, 0, 2, 2, 2, 1, 0 };
      const std::vector<size_t> bucket_counts { 3, 4, 5 };

      auto **output = input->bucket(bucket_counts, bucket_assignments);

      const std::vector<size_t> selected_ids_0 { 2, 6, 11 };
      const std::vector<size_t> selected_ids_1 { 0, 3, 4, 10 };
      const std::vector<size_t> selected_ids_2 { 1, 5, 7, 8, 9 };

      CHECK(output[0]->equals(input->select(selected_ids_0)));
      CHECK(output[1]->equals(input->select(selected_ids_1)));
      CHECK(output[2]->equals(input->select(selected_ids_2)));
    }

    TEST_CASE("Merge works") {
      std::vector<nullable_varchar_vector *> inputs(3);

      inputs[0] = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY" });
      inputs[0]->set_validity(1, 0);
      inputs[0]->set_validity(4, 0);

      inputs[1] = new nullable_varchar_vector(std::vector<std::string> { "MON", "", "WED" });
      inputs[1]->set_validity(2, 0);

      inputs[2] = new nullable_varchar_vector(std::vector<std::string> { "JUL", "AUG", "", "OCT" });
      inputs[2]->set_validity(3, 0);

      auto *expected = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "MON", "", "WED", "JUL", "AUG", "", "OCT" });
      expected->set_validity(1, 0);
      expected->set_validity(4, 0);
      expected->set_validity(7, 0);
      expected->set_validity(11, 0);

      const auto *output = nullable_varchar_vector::merge(&inputs[0], 3);
      CHECK(output != expected);
      CHECK(output->equals(expected));
    }

    TEST_CASE("Evaluation of ILIKE works") {
      const auto *input = new nullable_varchar_vector(std::vector<std::string> { "foo", "foobar", "bazfoobar", "bazfoo", "baz" });
      const std::vector<size_t> expected1 { 1, 0, 0, 0, 0 };
      const std::vector<size_t> expected2 { 1, 1, 0, 0, 0 };
      const std::vector<size_t> expected3 { 1, 0, 0, 1, 0 };
      const std::vector<size_t> expected4 { 1, 1, 1, 1, 0 };

      CHECK(input->eval_like("foo") == expected1);
      CHECK(input->eval_like("foo%") == expected2);
      CHECK(input->eval_like("%foo") == expected3);
      CHECK(input->eval_like("%foo%") == expected4);
    }

    TEST_CASE("Evaluation of IN works") {
      const auto *input = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY" });
      const std::vector<size_t> expected1 { 1, 0, 0, 0, 0 };
      const std::vector<size_t> expected2 { 0, 1, 0, 0, 1 };

      CHECK(input->eval_in(std::vector<std::string> { "JAN", "FOO" }) == expected1);
      CHECK(input->eval_in(std::vector<std::string> { "FEB", "MAY" }) == expected2);
    }

    TEST_CASE("Date cast works") {
      const auto *input = new nullable_varchar_vector(std::vector<std::string> { "1964-05-24", "1967-03-14", "1981-03-20", "2022-02-21" });
      const std::vector<int32_t> expected { -2048, -1024, 4096, 19044 };

      const auto output = input->date_cast();
      CHECK(output == expected);
    }

    TEST_CASE("Binary choose works") {
      std::vector<int32_t> input { 0, 1, 2, 3, 3, 4 };
      const auto condition = [&] (const size_t i) -> bool {
        return input[i] % 2 == 0;
      };
      const auto *expected = new nullable_varchar_vector(std::vector<std::string> { "foobar", "baz", "foobar", "baz", "baz", "foobar" });

      const auto *output = nullable_varchar_vector::from_binary_choice(input.size(), condition, "foobar", "baz");
      CHECK(output->equivalent_to(expected));
    }

    TEST_CASE("group_indexes_on_subset works [0]") {
      auto *input1 = new nullable_varchar_vector(std::vector<std::string> {"A", "A", "B", "A", "A", "A", "A", "B", "B", "B", "B", "A", "A", "B", "B" });
      auto *input2 = new nullable_varchar_vector(std::vector<std::string> {"A", "C", "B", "C", "D", "A", "A", "D", "B", "E", "B", "E", "A", "B", "C" });
      auto *input3 = new nullable_varchar_vector(std::vector<std::string> {"G", "H", "G", "H", "G", "H", "G", "H", "G", "H", "G", "H", "G", "H", "G" });

      std::vector<std::vector<size_t>> expected = {{0, 6, 12}, {5}, {1, 3}, {4}, {11}, {2, 8, 10}, {13}, {14}, {7}, {9}};

      size_t count = static_cast<size_t>(input1->count);
      size_t start_group_pos[2] = {0, count};
      size_t* a_arr = static_cast<size_t *>(malloc(sizeof(size_t) * count));
      size_t* b_arr = static_cast<size_t *>(malloc(sizeof(size_t) * count));

      size_t* a_pos_idxs = static_cast<size_t *>(malloc(sizeof(size_t) * (count + 1)));
      size_t* b_pos_idxs = static_cast<size_t *>(malloc(sizeof(size_t) * (count + 1)));

      size_t a_pos_idxs_size;
      size_t b_pos_idxs_size;

      input1->group_indexes_on_subset(nullptr, start_group_pos, 2, a_arr, a_pos_idxs, a_pos_idxs_size);
      input2->group_indexes_on_subset(a_arr, a_pos_idxs, a_pos_idxs_size, b_arr, b_pos_idxs, b_pos_idxs_size);
      input3->group_indexes_on_subset(b_arr, b_pos_idxs, b_pos_idxs_size, a_arr, a_pos_idxs, a_pos_idxs_size);

      std::vector<std::vector<size_t>> result;
      for(auto g = 1; g < a_pos_idxs_size; g++){
        std::vector<size_t> output_group(&a_arr[a_pos_idxs[g - 1]], &a_arr[a_pos_idxs[g]]);
        result.push_back(output_group);
      }
      cyclone::print_vec("result", result);

      free(a_arr);
      free(b_arr);
      free(a_pos_idxs);
      free(b_pos_idxs);

      CHECK(result == expected);
    }

    TEST_CASE("group_indexes_on_subset works [1]") {
      auto input1 = std::vector<std::string> { "JAN", "JANU", "FEBU", "FEB", "MARCH", "MARCG", "APR", "APR", "JANU", "SEP", "OCT", "NOV", "DEC2", "DEC1", "DEC0" };
      auto vec1 = new nullable_varchar_vector(input1);
      vec1->set_validity(3, 0);
      vec1->set_validity(10, 0);

      auto input2 = std::vector<std::string> { "APR", "APR", "JAN", "NOV", "SEP", "DEC0", "DEC1", "DEC2", "FEBU", "JANU", "JANU", "MARCG", "MARCH", "OCT", "FEB" };
      auto vec2 = new nullable_varchar_vector(input2);
      vec2->set_validity(13, 0);
      vec2->set_validity(14, 0);

      auto groups = vec1->group_indexes();
      std::vector<size_t> indices;
      for (auto group : groups) {
        for (auto i : group) indices.emplace_back(i);
      }

      CHECK(vec1->select(indices)->equivalent_to(vec2));
    }

    TEST_CASE("group_indexes_on_subset short-circuit works") {
      auto *input1 = new nullable_varchar_vector(std::vector<std::string> {"A", "B", "C", "D", "E"});

      size_t count = 5;
      size_t start_arr[5] = {0, 1, 2, 3, 4};
      size_t start_group_pos[6] = {0, 1, 2, 3, 4, 5};

      size_t* a_arr = static_cast<size_t *>(malloc(sizeof(size_t) * count));
      size_t* a_pos_idxs = static_cast<size_t *>(malloc(sizeof(size_t) * (count + 1)));
      size_t a_pos_idxs_size;

      input1->group_indexes_on_subset(start_arr, start_group_pos, 6, a_arr, a_pos_idxs, a_pos_idxs_size);

      CHECK(a_pos_idxs_size == 6);
      CHECK(a_arr[0] == start_arr[0]);
      CHECK(a_arr[1] == start_arr[1]);
      CHECK(a_arr[2] == start_arr[2]);
      CHECK(a_arr[3] == start_arr[3]);
      CHECK(a_arr[4] == start_arr[4]);

      CHECK(a_pos_idxs[0] == start_group_pos[0]);
      CHECK(a_pos_idxs[1] == start_group_pos[1]);
      CHECK(a_pos_idxs[2] == start_group_pos[2]);
      CHECK(a_pos_idxs[3] == start_group_pos[3]);
      CHECK(a_pos_idxs[4] == start_group_pos[4]);
      CHECK(a_pos_idxs[5] == start_group_pos[5]);

      free(a_arr);
      free(a_pos_idxs);
    }
  }
}
