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

namespace cyclone::tests {
  TEST_SUITE("nullable_varchar_vector") {
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

      const auto output = input->hash_at(3, 42);
      CHECK(output == 31 * (31 * (31 * 42 + 'A') + 'P') + 'R');
    }

    TEST_CASE("Get and Set validity bit works") {
      auto *input = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "", "OCT", "NOV", "DEC" });

      input->set_validity(4, 0);
      CHECK(input->get_validity(4) == 0);

      input->set_validity(4, 1);
      CHECK(input->get_validity(4) == 1);
    }

    TEST_CASE("Filter works") {
      // Include empty string value
      auto *input = new nullable_varchar_vector(std::vector<std::string> { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "", "SEP", "OCT", "NOV", "DEC" });
      input->set_validity(1, 0);
      input->set_validity(4, 0);
      input->set_validity(10, 0);

      const std::vector<size_t> matching_ids { 1, 2, 4, 7, 11 };

      auto *expected = new nullable_varchar_vector(std::vector<std::string> { "FEB", "MAR", "MAY", "", "DEC" });
      expected->set_validity(0, 0);
      expected->set_validity(2, 0);

      auto *output = input->filter(matching_ids);
      CHECK(output != input);
      CHECK(output->equals(expected));
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

      const std::vector<size_t> matching_ids_0 { 2, 6, 11 };
      const std::vector<size_t> matching_ids_1 { 0, 3, 4, 10 };
      const std::vector<size_t> matching_ids_2 { 1, 5, 7, 8, 9 };

      CHECK(output[0]->equals(input->filter(matching_ids_0)));
      CHECK(output[1]->equals(input->filter(matching_ids_1)));
      CHECK(output[2]->equals(input->filter(matching_ids_2)));
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
  }
}
