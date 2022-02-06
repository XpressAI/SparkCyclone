#define DOCTEST_CONFIG_IMPLEMENT
#include "tests/doctest.h"

// Include all tests to be run
#include "tests/example.hpp"

int main(int argc, char** argv) {
  // Based on: https://github.com/doctest/doctest/blob/master/doc/markdown/main.md
  doctest::Context context;

  // Defaults
  context.setOption("abort-after", 5);        // Stop test execution after 5 failed assertions
  context.setOption("order-by", "name");      // Sort the test cases by their name

  // Apply CLI flags
  context.applyCommandLine(argc, argv);

  // Overrides
  context.setOption("no-breaks", true);       // Don't break in the debugger when assertions fail

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
