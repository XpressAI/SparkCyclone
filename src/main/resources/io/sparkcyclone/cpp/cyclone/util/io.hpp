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

#include <cstdio>

namespace cyclone::io {
  template <typename T>
  T format_arg(T value) noexcept {
    return value;
  }

  template <typename T>
  T const * format_arg(std::basic_string<T> const & value) noexcept {
    return value.c_str();
  }

  template<typename ... T>
  std::string format(const std::string& fmt, T const & ...args) {
    // Run the first tmie to obtain the would-be size of the output string if it were written
    const auto size = std::snprintf(nullptr, 0, fmt.c_str(), format_arg(args) ...) + 1;
    // Allocate the string buffer with the right size
    std::string buffer(size, 0);
    // Run the second time to actually write to a buffer
    std::snprintf(const_cast<char *>(buffer.c_str()), size, fmt.c_str(), format_arg(args) ...);
    // Remove the null-terminator at the end
    buffer.resize(size - 1);
    // Return
    return buffer;
  }
}
