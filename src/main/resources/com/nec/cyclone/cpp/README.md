# Cyclone C++ Library

This Cyclone C++ library is and linked to by the Spark Cyclone plugin as it
generates C++ code to perform Spark SQL queries on the Vector Engine.  The
library contains the following:

* A subset of the [Frovedis](https://github.com/frovedis/frovedis) library that
  contains vectorizable data structures and algorithms that are used by Spark
  Cyclone as part of query execution.
* A definition of data structures used for transferring Arrow data in Spark over
  to C++.
* A collection of common functions that are to be called by Spark Cyclone-generated
  C++ code.

This library has been written so that it can be built standalone.  This allows us
the following:

* To experiment with Frovedis and algorithm optimizations as needed.
* Enables for C++ code to be tested and benchmarked on its own
* Reduces the complexity of C++ code generation from the Spark Cyclone plugin,
  which for all intents and purposes is not unit-testable.



## Library Development

### Building the Library

The Cyclone C++ library is generally portable, and building the library only
requires `make` and a `c++` compiler that is visible in the `PATH`:

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

New code should be added to the `cyclone/` subdirectory and `cyclone` namespace.
Corresponding tests should be added to the `tests/` subdirectory as a header
file and `cyclone::tests` namespace.  **The tests file will then need to be
`#include`d in `tests/driver.cc` in order to run.**

See `cyclone/example.cc` and `tests/example.hpp` for examples of adding library
code and tests, respectively.
