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
#include "cyclone/util/memory.hpp"
#include "tests/doctest.h"

namespace cyclone::tests {
  TEST_SUITE("cyclone::memory") {
    TEST_CASE("cyclone_alloc and cyclone_free should work") {
      uintptr_t addresses[2];

      // Allocate
      CHECK(cyclone_alloc(1024, &addresses[0]) == 0);
      CHECK(cyclone_alloc(2048, &addresses[1]) == 0);

      // Free should not crash
      CHECK(cyclone_free(&addresses[0], 2) == 0);
    }
  }
}
