#include "frovedis/core/radix_sort.hpp"
#include "transfer-definitions.c"
#include "frovedis/dataframe/join.hpp"

extern "C" long join_doubles(non_null_double_vector* left,
    non_null_double_vector* right,
    non_null_double_vector* out) {
    std::cout << "TEZT\n";
    frovedis::equi_join(input, input, out);
   return 0;
}
