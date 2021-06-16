#include "frovedis/core/radix_sort.hpp"
#include "transfer-definitions.c"
#include "frovedis/dataframe/join.hpp"
#include <vector>
#include <iostream>

extern "C" long join_doubles(non_null_double_vector* input, non_null_double_vector* out) {
    std::cout << "TEZT\n";
   return 0;
}
