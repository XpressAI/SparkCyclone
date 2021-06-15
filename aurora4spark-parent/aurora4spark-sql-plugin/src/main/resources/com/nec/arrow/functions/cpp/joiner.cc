#include "frovedis/core/radix_sort.hpp"
#include "transfer-definitions.c"
#include "frovedis/dataframe/join.hpp"

extern "C" long join_doubles(non_null_double_vector* input, non_null_double_vector* out) {
    std::cout << "TEZT\n";
    frovedis::equi_join2(input, input, out);
   return 0;
}
