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

#include "frovedis/text/words.hpp"
#include <cstdio>
#include <iostream>
#include <string>

namespace cyclone {
  // Print out a std::tuple to ostream
  template<typename Ch, typename Tr, typename... Ts>
  auto& operator<<(std::basic_ostream<Ch, Tr> &stream,
                   std::tuple<Ts...> const &tup) {
    std::basic_stringstream<Ch, Tr> tmp;
    tmp << "(";
    // Based on: https://stackoverflow.com/questions/6245735/pretty-print-stdtuple
    std::apply(
      [&tmp] (auto&&... args) { ((tmp << args << ", "), ...); },
      tup
    );
    tmp.seekp(-2, tmp.cur);
    tmp << ")";
    return stream << tmp.str();
  }

  // Print out a std::vector to ostream
  // Define this AFTER defining operator<< for std::tuple
  // so that we can print std::vector<std::tuple<Ts...>>
  template<typename Ch, typename Tr, typename T>
  auto& operator<<(std::basic_ostream<Ch, Tr> &stream,
                   std::vector<T> const &vec) {
    std::basic_stringstream<Ch, Tr> tmp;
    if (vec.size() > 0) {
      tmp << "[ ";
      for (const auto &elem : vec) {
        tmp << elem << ", ";
      }
      tmp.seekp(-2, tmp.cur);
      tmp << " ]";
    } else {
      tmp << "[ ]";
    }
    return stream << tmp.str();
  }

  namespace io {
    template<typename T>
    void print_vec(const std::string_view &name, const std::vector<T> &vec) {
      std::cout << name <<  " = " << vec << std::endl;
    }

    template<typename T>
    void print_vec(const std::string_view &name, const T * array, const size_t len) {
      std::cout << name <<  " = [ ";
      for (auto i = 0; i < len; i++) {
        std::cout << array[i] << ", ";
      }
      std::cout << "]" << std::endl;
    }

    void print_words(const frovedis::words &in);

    template <typename T>
    T format_arg(T value) noexcept {
      return value;
    }

    template <typename T>
    T const * format_arg(std::basic_string<T> const & value) noexcept {
      return value.c_str();
    }

    template<typename ... T>
    std::string format(const std::string_view& fmt, T const & ...args) {
      // Run the first tmie to obtain the would-be size of the output string if it were written
      const auto size = std::snprintf(nullptr, 0, fmt.data(), format_arg(args) ...) + 1;
      // Allocate the string buffer with the right size
      std::string buffer(size, 0);
      // Run the second time to actually write to a buffer
      std::snprintf(const_cast<char *>(buffer.c_str()), size, fmt.data(), format_arg(args) ...);
      // Remove the null-terminator at the end
      buffer.resize(size - 1);
      // Return
      return buffer;
    }
  }
}
