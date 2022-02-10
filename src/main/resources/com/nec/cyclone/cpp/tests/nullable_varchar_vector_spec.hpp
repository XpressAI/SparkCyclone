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

namespace cyclone::tests {
  TEST_SUITE("nullable_varchar_vector") {
    TEST_CASE("Equality checks work") {
      std::vector<std::string> raw { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };
      auto *vec1 = to_nullable_varchar_vector(raw);
      set_validity(vec1->validityBuffer, 1, 0);
      set_validity(vec1->validityBuffer, 4, 0);
      set_validity(vec1->validityBuffer, 10, 0);

      // Different content
      auto *vec2 = to_nullable_varchar_vector(std::vector<std::string> { "MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN" });

      // Different validityBuffer
      auto *vec3 = to_nullable_varchar_vector(raw);
      set_validity(vec3->validityBuffer, 7, 0);

      CHECK(vec1->equals(vec1));
      CHECK(not vec1->equals(vec2));
      CHECK(not vec1->equals(vec3));
    }

    TEST_CASE("Clone works") {
      // Include empty string value
      auto *input = to_nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "", "OCT", "NOV", "DEC" });
      set_validity(input->validityBuffer, 1, 0);
      set_validity(input->validityBuffer, 4, 0);
      set_validity(input->validityBuffer, 10, 0);

      auto *output = input->clone();
      CHECK(output != input);
      CHECK(output->equals(input));
    }

    TEST_CASE("Filter works") {
      // Include empty string value
      auto *input = to_nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "", "SEP", "OCT", "NOV", "DEC" });
      set_validity(input->validityBuffer, 1, 0);
      set_validity(input->validityBuffer, 4, 0);
      set_validity(input->validityBuffer, 10, 0);

      const std::vector<size_t> matching_ids { 1, 2, 4, 7, 11 };

      auto *expected = to_nullable_varchar_vector(std::vector<std::string> { "FEB", "MAR", "MAY", "", "DEC" });
      set_validity(expected->validityBuffer, 0, 0);
      set_validity(expected->validityBuffer, 2, 0);

      CHECK(input->filter(matching_ids)->equals(expected));
    }

    TEST_CASE("Bucket works") {
      // Include empty string value
      auto *input = to_nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "", "SEP", "OCT", "NOV", "DEC" });
      set_validity(input->validityBuffer, 1, 0);
      set_validity(input->validityBuffer, 4, 0);
      set_validity(input->validityBuffer, 10, 0);

      const std::vector<size_t> bucket_assignments { 1, 2, 0, 1, 1, 2, 0, 2, 2, 2, 1, 0 };
      const std::vector<size_t> bucket_counts { 3, 4, 5 };

      auto **output = input->bucket(bucket_counts, bucket_assignments);

      const std::vector<size_t> matching_ids_0 { 2, 6, 11 };
      const std::vector<size_t> matching_ids_1 { 0, 3, 4, 10 };
      const std::vector<size_t> matching_ids_2 { 1, 5, 7, 8, 9 };

      CHECK(output[0]->equals(input->filter(matching_ids_0)));
      CHECK(output[1]->equals(input->filter(matching_ids_1)));
      CHECK(output[2]->equals(input->filter(matching_ids_2)));
    }
  }
}
