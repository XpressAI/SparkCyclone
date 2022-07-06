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
#define DOCTEST_CONFIG_IMPLEMENT
#include "tests/doctest.h"

int main(int argc, char** argv) {
  // Based on: https://github.com/doctest/doctest/blob/master/doc/markdown/main.md
  doctest::Context context;

  // Apply defaults
  context.setOption("abort-after", 5);    // Stop test execution after 5 failed assertions
  context.setOption("duration", true);    // Prints the time duration of each test
  context.setOption("order-by", "name");  // Sort the test cases by their name
  context.setOption("success", true);     // Include successful assertions in output

  // Apply CLI flags
  context.applyCommandLine(argc, argv);

  // Apply overrides
  context.setOption("no-breaks", true);   // Don't break in the debugger when assertions fail

  // Run all the tests
  int res = context.run();

  // Important - query flags (and --exit) rely on the user doing this
  if (context.shouldExit()) {
    // Propagate the result of the tests
    return res;
  }

  // Your program - if the testing framework is integrated in your production code
  int client_stuff_return_code = 0;

  // The result from doctest is propagated here as well
  return res + client_stuff_return_code;
}
