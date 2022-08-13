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

#include "cyclone/util/io.hpp"
#include <algorithm>
#include <sstream>

void cyclone::io::print_words(const frovedis::words &in) {
  std::stringstream stream;
  stream  << "frovedis::words @ " << &in << "\n"
          << "words char count: " << in.chars.size() << "\n"
          << "words starts count: " << in.starts.size() << "\n"
          << "words lens count: " << in.lens.size() << "\n"
          << "First word starts at: " << in.starts[0] << " length: " << in.lens[0] << " '";

  size_t start = in.starts[0];
  for (int i = 0; i < std::min((long)in.lens[0], 64L); i++) {
      stream << (char)in.chars[start + i];
  }
  stream << "'\n";

  stream << "Last word " << in.starts.size() - 1 << " starts at: " << in.starts[in.starts.size() -1] << " length[" << in.lens.size() - 1 << "]: " << in.lens[in.lens.size() - 1] << " '";
  start = in.starts[in.starts.size() - 1];
  for (int i = 0; i < std::min((long)in.lens[in.lens.size() - 1], 64L); i++) {
      stream << (char)in.chars[start + i];
  }
  stream << "'\n";
  std::cout << stream.str() << std::endl;
}
