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
#pragma once

#include <chrono>
#include <vector>

namespace cyclone::time {
  std::string utc();

  inline decltype(auto) now() {
    return std::chrono::high_resolution_clock::now();
  }

  template<typename T>
  inline int64_t nanos_since(const std::chrono::time_point<T> &start) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now() - start).count();
  }
}
