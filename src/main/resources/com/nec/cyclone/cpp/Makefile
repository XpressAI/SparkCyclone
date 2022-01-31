CPP = $(shell ls /opt/nec/ve/bin/nc++ 2>/dev/null || echo c++)
CPP_OPTS = -std=c++17 \
	-O4 \
	-fno-defer-inline-template-instantiation \
	-finline-functions \
	-finline-max-depth=10 \
	-msched-block \
	-pthread \
	-report-all \
	-fdiag-vector=2

ALL = libcyclone

all: $(ALL)

.PHONY: clean

DEPS = cyclone.cc \
	cyclone.hpp \
	frovedis/core/conditions_for_find.hpp \
	frovedis/core/config.hpp \
	frovedis/core/find_condition.hpp \
	frovedis/core/lower_bound.hpp \
	frovedis/core/prefix_sum.hpp \
	frovedis/core/radix_sort.hpp \
	frovedis/core/set_operations.hpp \
	frovedis/core/upper_bound.hpp \
	frovedis/core/utility.cc \
	frovedis/core/utility.hpp \
	frovedis/dataframe/hashtable.hpp \
	frovedis/dataframe/join.cc \
	frovedis/dataframe/join.hpp \
	frovedis/text/char_int_conv.cc \
	frovedis/text/char_int_conv.hpp \
	frovedis/text/datetime_to_words.cc \
	frovedis/text/datetime_to_words.hpp \
	frovedis/text/datetime_utility.hpp \
	frovedis/text/dict.cc \
	frovedis/text/dict.hpp \
	frovedis/text/find.cc \
	frovedis/text/find.hpp \
	frovedis/text/float_to_words.cc \
	frovedis/text/float_to_words.hpp \
	frovedis/text/int_to_words.cc \
	frovedis/text/int_to_words.hpp \
	frovedis/text/parsedatetime.cc \
	frovedis/text/parsedatetime.hpp \
	frovedis/text/parsefloat.cc \
	frovedis/text/parsefloat.hpp \
	frovedis/text/parseint.hpp \
	frovedis/text/words.cc \
	frovedis/text/words.hpp \
	transfer-definitions.hpp \
	tuple_hash.hpp \
	utility.hpp

SOURCES = cyclone.cc \
	frovedis/core/utility.cc \
	frovedis/dataframe/join.cc \
	frovedis/text/char_int_conv.cc \
	frovedis/text/datetime_to_words.cc \
	frovedis/text/dict.cc \
	frovedis/text/find.cc \
	frovedis/text/float_to_words.cc \
	frovedis/text/int_to_words.cc \
	frovedis/text/parsedatetime.cc \
	frovedis/text/parsefloat.cc \
	frovedis/text/words.cc \

libcyclone: $(DEPS)
	$(CPP) $(CPP_OPTS) -shared -fPIC -pthread -o libcyclone.so $(SOURCES)

test: libcyclone
	$(CPP) $(CPP_OPTS) -c test.cpp
	$(CPP) $(CPP_OPTS) -o test.out test.o -L. -lcyclone

clean:
	rm -f *.o *.so *.out
