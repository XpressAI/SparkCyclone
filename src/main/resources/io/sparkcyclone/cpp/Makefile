# Set the compiler to nc++ if available, or fall back to system default
CPP = $(shell ls /opt/nec/ve/bin/nc++ 2>/dev/null || echo c++)
CPP_OPTS = -std=gnu++17 \
	-I. \
	-O4 \
	-finline-functions \
	-pthread \
	-fPIC \
	-Werror=return-type

# Add extra flags if we're using nc++
ifeq (${shell ls /opt/nec/ve/bin/nc++ > /dev/null 2>&1 && echo 1 || echo 0}, 1)
	CPP_OPTS += -fno-defer-inline-template-instantiation \
		-finline-max-depth=10 \
		-msched-block \
		-report-all \
		-fdiag-vector=2
endif

ALL = libcyclone.so

all: $(ALL)

.PHONY: clean

DEPS = $(shell find frovedis -name \*.cc -o -name \*.hpp) \
	$(shell find cyclone -name \*.cc -o -name \*.hpp)

# Collect all source files from the frovedis/ and cyclone/ subdirectories
SOURCES = $(shell find frovedis -name \*.cc) \
	$(shell find cyclone -name \*.cc)

TEST_SOURCES = $(shell find tests -name \*.cc)

BENCHMARK_SOURCES = $(shell find benchmarks -name \*.cc)

OBJECTS = $(SOURCES:.cc=.o)

TEST_OBJECTS = $(TEST_SOURCES:.cc=.o)

BENCHMARK_OBJECTS = $(BENCHMARK_SOURCES:.cc=.o)

# Compile the sources separately in parallel into object files
$(OBJECTS): $(SOURCES)
	$(CPP) $(CPP_OPTS) $(@:.o=.cc) -c -o $@

$(TEST_OBJECTS): $(TEST_SOURCES)
	$(eval CPP_OPTS:=$(filter-out -fno-defer-inline-template-instantiation,$(CPP_OPTS)))
	$(CPP) $(CPP_OPTS) $(@:.o=.cc) -c -o $@

$(BENCHMARK_OBJECTS): $(BENCHMARK_SOURCES)
	$(eval CPP_OPTS:=$(filter-out -fno-defer-inline-template-instantiation,$(CPP_OPTS)))
	$(CPP) $(CPP_OPTS) $(@:.o=.cc) -c -o $@

# Create a BOM file of the source files (for use by the Spark Cyclone plugin)
bom:
	find frovedis -name \*.cc -o -name \*.hpp -o -name \*.incl\* | sort > sources.bom
	find cyclone -name \*.cc -o -name \*.hpp | sort >> sources.bom
	echo Makefile >> sources.bom

# Build the library from the object files
libcyclone.so: $(DEPS) $(OBJECTS)
	$(CPP) $(CPP_OPTS) $(OBJECTS) -shared -o libcyclone.so

# Build the tests executable linked by tests/driver.cpp
# Remove `-fno-defer-inline-template-instantiation` because it breaks compilation of doctest
test: libcyclone.so  $(TEST_OBJECTS)
	$(eval CPP_OPTS:=$(filter-out -fno-defer-inline-template-instantiation,$(CPP_OPTS)))
	$(CPP) $(CPP_OPTS) $(TEST_OBJECTS) -o cyclone-tests.out -L. -lcyclone
	VE_LD_LIBRARY_PATH=. ./cyclone-tests.out

# Build the benchmarks executable linked by benchmarks/driver.cpp
# Remove `-fno-defer-inline-template-instantiation` because it breaks compilation of doctest
benchmark: libcyclone.so  $(BENCHMARK_OBJECTS)
	$(eval CPP_OPTS:=$(filter-out -fno-defer-inline-template-instantiation,$(CPP_OPTS)))
	$(CPP) $(CPP_OPTS) $(BENCHMARK_OBJECTS) -o cyclone-benchmarks.out -L. -lcyclone
	VE_LD_LIBRARY_PATH=. ./cyclone-benchmarks.out

# Build examples
examples: libcyclone.so
	$(CPP) $(CPP_OPTS) -c examples.cpp
	$(CPP) $(CPP_OPTS) -o cyclone-examples.out examples.o -L. -lcyclone
	VE_LD_LIBRARY_PATH=. ./cyclone-examples.out

clean:
	find . -name "*.o" -type f -delete
	rm -f *.s *.L *.o *.out

cleanall:
	find . -name "*.o" -type f -delete
	rm -f *.s *.L *.o *.out *.so *.bom
