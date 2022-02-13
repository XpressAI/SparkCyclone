# Cyclone C++ Library

The Cyclone C++ library is linked to by the Spark Cyclone plugin as it generates
C++ code to perform Spark SQL queries on the Vector Engine.  The library contains
the following:

* A subset of the [Frovedis](https://github.com/frovedis/frovedis) library that
  contains vectorizable data structures and algorithms that are used by Spark
  Cyclone as part of query execution.
* A definition of data structures used for transferring Arrow data in Spark over
  to C++.
* A collection of common functions that are to be called by Spark Cyclone-generated
  C++ code.

This library has been written so that it can be built standalone.  This allows
for the following:

* Experiment with the Frovedis API and algorithm optimizations as needed.
* Test and benchmark the C++ code on its own before integrating it into the Spark
  Cyclone plugin's code generation.
* Reduce the complexity of C++ code generation from the Spark Cyclone plugin,
  which for all intents and purposes is not unit-testable.



## Library Development

### Building the Library

The Cyclone C++ library is generally portable, and building the library only
requires `make` and a `c++` compiler that is visible in the `PATH` and supports
**C++17** (with GNU extensions):

```sh
make
```

On a machine with `nc++` installed, the script will look for `/opt/nec/ve/bin/nc++`.

### Running Tests

To run unit tests:

```sh
make test
```

Cyclone unit tests are run using [doctest](https://github.com/doctest/doctest),
which is a modern single-header C++ testing framework.  The full and latest
source code for doctest can be found
[here](https://raw.githubusercontent.com/doctest/doctest/master/doctest/doctest.h).

### Running Example Code

To run example code:

```sh
make examples
```

### Adding New Code

The steps for adding new code to the Cyclone library are generally as follows:

1.  Add the new source and header files to the `cyclone/` subdirectory (e.g.
    `cyclone/example.hpp` and `cyclone/example.cc`).

1.  Make sure the code is under the `cyclone` namespace, and the `#includes`
    reference the full path from project root (e.g. `cyclone/cyclone.hpp` instead
    of `cyclone.hpp`).

1.  Add the corresponding spec as a header file to the `tests/` subdirectory
    (e.g. `tests/example_spec.hpp`).

1.  `#include` the spec header file inside `tests/driver.cpp`
    (e.g. `#include "tests/example_spec.hpp"`).

Re-running `make test`should include the newly added tests into the tests executable.