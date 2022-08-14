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
#include "tests/doctest.h"
#include <iostream>

namespace cyclone::tests {
  /*
    These tests exist to ensure that GNU extensions to C/C++ work for the
    compiler and platform that the code will be running on, since the C++ code
    generated by the Spark Cyclone plugin will be leveraging those language
    extensions.
  */

  TEST_CASE("Statement Expressions work") {
    auto tmp = 10;

    auto result1 = ({
      int y = 10;
      y + 32;
    });

    auto result2 = ({
      std::cout << "Testing the condition" << std::endl;
      tmp < 20;
    }) ? (({
      std::cout << "Reached the true case" << std::endl;
      33;
    })) : ({
      std::cout << "Reached the false case" << std::endl;
      66;
    });

    CHECK(result1 == 42);
    CHECK(result2 == 33);
  }
}