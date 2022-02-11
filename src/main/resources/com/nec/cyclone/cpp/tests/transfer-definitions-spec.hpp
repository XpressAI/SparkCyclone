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
#include "tests/test-utils.hpp"

namespace cyclone::tests {
  TEST_SUITE("NullableScalarVec<T>") {
    TEST_CASE("Equality checks work") {
      // Instantiate for int32_t
      std::vector<int32_t> raw1 { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 };
      auto *input1 = to_nullable_scalar_vector(raw1);
      set_validity(input1->validityBuffer, 1, 0);
      set_validity(input1->validityBuffer, 4, 0);
      set_validity(input1->validityBuffer, 8, 0);

      // Instantiate for int64_t
      std::vector<int64_t> raw2 { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 };
      auto *input2 = to_nullable_scalar_vector(raw2);
      set_validity(input2->validityBuffer, 2, 0);
      set_validity(input2->validityBuffer, 5, 0);

      // Instantiate for float
      std::vector<float> raw3 { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 };
      auto *input3 = to_nullable_scalar_vector(raw2);
      set_validity(input3->validityBuffer, 3, 0);
      set_validity(input3->validityBuffer, 7, 0);

      // Instantiate for double
      std::vector<double> raw4 { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 };
      auto *input4 = to_nullable_scalar_vector(raw4);
      set_validity(input4->validityBuffer, 2, 0);
      set_validity(input4->validityBuffer, 6, 0);
      set_validity(input4->validityBuffer, 8, 0);

      CHECK(input1->equals(input1));
      CHECK(input2->equals(input2));
      CHECK(input3->equals(input3));
      CHECK(input4->equals(input4));
    }

    TEST_CASE("Clone works") {
      std::vector<int32_t> raw { 586, 951, 106, 318, 538, 620, 553, 605, 822, 941 };
      auto *input = to_nullable_scalar_vector(raw);
      set_validity(input->validityBuffer, 1, 0);
      set_validity(input->validityBuffer, 4, 0);
      set_validity(input->validityBuffer, 8, 0);

      auto *output = input->clone();
      CHECK(output->equals(input));
    }
  }

  TEST_SUITE("nullable_varchar_vector") {
    TEST_CASE("Equality checks work") {
      std::vector<std::string> raw { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };
      auto *input = to_nullable_varchar_vector(raw);
      set_validity(input->validityBuffer, 1, 0);
      set_validity(input->validityBuffer, 4, 0);
      set_validity(input->validityBuffer, 10, 0);

      CHECK(input->equals(input));
    }

    TEST_CASE("Clone works") {
      std::vector<std::string> raw { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };
      auto *input = to_nullable_varchar_vector(raw);
      set_validity(input->validityBuffer, 1, 0);
      set_validity(input->validityBuffer, 4, 0);
      set_validity(input->validityBuffer, 10, 0);

      auto *output = input->clone();
      CHECK(output->equals(input));
    }
  }
}
