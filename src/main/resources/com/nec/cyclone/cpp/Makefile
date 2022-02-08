# Set the compiler to nc++ if available, or fall back to system default
CPP = $(shell ls /opt/nec/ve/bin/nc++ 2>/dev/null || echo c++)
CPP_OPTS = -std=gnu++17 \
	-I. \
	-O4 \
	-finline-functions \
	-pthread

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

# Build the library
libcyclone.so: $(DEPS)
	$(CPP) $(CPP_OPTS) -shared -fPIC -o libcyclone.so $(SOURCES)

# Build and run the tests, which are defined as header files included in tests/driver.cpp
# Remove `-fno-defer-inline-template-instantiation` because it breaks compilation of doctest
test: libcyclone.so
	$(eval CPP_OPTS:=$(filter-out -fno-defer-inline-template-instantiation,$(CPP_OPTS)))
	$(CPP) $(CPP_OPTS) ${TEST_SOURCES} -o cyclone-tests.out -L. -lcyclone
	VE_LD_LIBRARY_PATH=. ./cyclone-tests.out

# Build examples
examples: libcyclone.so
	$(CPP) $(CPP_OPTS) -c examples.cpp
	$(CPP) $(CPP_OPTS) -o cyclone-examples.out examples.o -L. -lcyclone
	VE_LD_LIBRARY_PATH=. ./cyclone-examples.out

clean:
	rm -f *.s *.L *.o *.so *.out
