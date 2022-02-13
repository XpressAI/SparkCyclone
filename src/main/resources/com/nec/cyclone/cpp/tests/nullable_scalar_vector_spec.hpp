/*
 * Copyright (c) 2021 Xpress AI.
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

#include "cyclone/cyclone.hpp"
#include "cyclone/transfer-definitions.hpp"
#include "tests/doctest.h"
#include "tests/test_utils.hpp"
#include <stddef.h>

namespace cyclone::tests {
  TEST_SUITE("NullableScalarVec<T>") {
    TEST_CASE_TEMPLATE("Equality checks work for T=", T, int32_t, int64_t, float, double) {
      // Instantiate for int32_t
      std::vector<T> raw { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 };

      auto *vec1 = new NullableScalarVec(raw);
      vec1->set_validity(1, 0);
      vec1->set_validity(4, 0);
      vec1->set_validity(8, 0);

      // Different count
      auto *vec2 = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538, 620, 553 });

      // Different data
      auto *vec3 = new NullableScalarVec(raw);
      vec3->data[3] = 184;

      // Different validityBuffer
      auto *vec4 = new NullableScalarVec(raw);
      vec4->set_validity(3, 0);

      CHECK(vec1->equals(vec1));
      CHECK(not vec1->equals(vec2));
      CHECK(not vec1->equals(vec3));
      CHECK(not vec1->equals(vec4));
    }

    TEST_CASE_TEMPLATE("Check default works for T=", T, int32_t, int64_t, float, double) {
      auto *empty = new NullableScalarVec<T>;
      auto *nonempty = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 });

      CHECK(empty->is_default());
      CHECK(not nonempty->is_default());
    }

    TEST_CASE_TEMPLATE("Reset works for T=", T, int32_t, int64_t, float, double) {
      auto *empty = new NullableScalarVec<T>;

      auto *input = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 });
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      input->set_validity(8, 0);

      CHECK(not input->equals(empty));
      input->reset();
      CHECK(input->equals(empty));
    }

    TEST_CASE_TEMPLATE("Clone works for T=", T, int32_t, int64_t, float, double) {
      auto *input = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 });
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      input->set_validity(8, 0);

      auto *output = input->clone();
      CHECK(output != input);
      CHECK(output->equals(input));
    }

    TEST_CASE_TEMPLATE("Filter works for T=", T, int32_t, int64_t, float, double) {
      auto *input = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 });
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      input->set_validity(8, 0);

      const std::vector<size_t> matching_ids { 1, 2, 4, 7 };

      auto *expected = new NullableScalarVec(std::vector<T> { 951, 106, 538, 605 });
      expected->set_validity(0, 0);
      expected->set_validity(2, 0);

      auto *output = input->filter(matching_ids);
      CHECK(output != input);
      CHECK(output->equals(expected));
    }

    TEST_CASE_TEMPLATE("Bucket works for T=", T, int32_t, int64_t, float, double) {
      auto *input = new NullableScalarVec(std::vector<T> { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 });
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      input->set_validity(8, 0);

      const std::vector<size_t> bucket_assignments { 1, 2, 0, 1, 1, 2, 0, 2, 2, 2 };
      const std::vector<size_t> bucket_counts { 2, 3, 5 };

      auto **output = input->bucket(bucket_counts, bucket_assignments);

      const std::vector<size_t> matching_ids_0 { 2, 6 };
      const std::vector<size_t> matching_ids_1 { 0, 3, 4, };
      const std::vector<size_t> matching_ids_2 { 1, 5, 7, 8, 9 };

      CHECK(output[0]->equals(input->filter(matching_ids_0)));
      CHECK(output[1]->equals(input->filter(matching_ids_1)));
      CHECK(output[2]->equals(input->filter(matching_ids_2)));
    }
  }
}
