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

      auto *vec1 = to_nullable_scalar_vector(raw);
      set_validity(vec1->validityBuffer, 1, 0);
      set_validity(vec1->validityBuffer, 4, 0);
      set_validity(vec1->validityBuffer, 8, 0);

      // Different count
      auto *vec2 = to_nullable_scalar_vector(std::vector<T> { 586, 951, 106, 318, 538, 620, 553 });

      // Different data
      auto *vec3 = to_nullable_scalar_vector(raw);
      vec3->data[3] = 184;

      // Different validityBuffer
      auto *vec4 = to_nullable_scalar_vector(raw);
      set_validity(vec4->validityBuffer, 3, 0);

      CHECK(vec1->equals(vec1));
      CHECK(not vec1->equals(vec2));
      CHECK(not vec1->equals(vec3));
      CHECK(not vec1->equals(vec4));
    }

    TEST_CASE_TEMPLATE("Clone works for T=", T, int32_t, int64_t, float, double) {
      auto *input = to_nullable_scalar_vector(std::vector<T> { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 });
      set_validity(input->validityBuffer, 1, 0);
      set_validity(input->validityBuffer, 4, 0);
      set_validity(input->validityBuffer, 8, 0);

      auto *output = input->clone();
      CHECK(output != input);
      CHECK(output->equals(input));
    }

    TEST_CASE_TEMPLATE("Filter works for T=", T, int32_t, int64_t, float, double) {
      auto *input = to_nullable_scalar_vector(std::vector<T> { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 });
      set_validity(input->validityBuffer, 1, 0);
      set_validity(input->validityBuffer, 4, 0);
      set_validity(input->validityBuffer, 8, 0);

      const std::vector<size_t> matching_ids { 1, 2, 4, 7 };

      auto *expected = to_nullable_scalar_vector(std::vector<T> { 951, 106, 538, 605 });
      set_validity(expected->validityBuffer, 0, 0);
      set_validity(expected->validityBuffer, 2, 0);

      CHECK(input->filter(matching_ids)->equals(expected));
    }

    TEST_CASE_TEMPLATE("Bucket works for T=", T, int32_t, int64_t, float, double) {
      auto *input = to_nullable_scalar_vector(std::vector<T> { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 });
      set_validity(input->validityBuffer, 1, 0);
      set_validity(input->validityBuffer, 4, 0);
      set_validity(input->validityBuffer, 8, 0);

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
