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
#include "cyclone/util/log.hpp"
#include "tests/doctest.h"

namespace cyclone::tests {
  TEST_SUITE("cyclone::log") {
    TEST_CASE("Stream logging works") {
      cyclone::log::strace << "trace message" << std::endl;
      cyclone::log::sdebug << "debug message" << std::endl;
      cyclone::log::sinfo << "info message" << std::endl;
      cyclone::log::swarn << "warn message" << std::endl;
      cyclone::log::serror << "error message" << std::endl;
      cyclone::log::sfatal << "fatal message" << std::endl;
    }

    TEST_CASE("Method logging works") {
      int i = 3;
      float f = 5.f;
      char* s0 = "hello";
      std::string s1 = "world";

      cyclone::log::trace("trace message");
      cyclone::log::debug("debug message");
      cyclone::log::info("i=%d, f=%f, s=%s %s", i, f, s0, s1);
      cyclone::log::warn("warn message");
      cyclone::log::error("error message");
      cyclone::log::fatal("fatal message");
    }
  }
}
