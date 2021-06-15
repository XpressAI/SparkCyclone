#include "frovedis/core/radix_sort.hpp"
#include "transfer-definitions.c"
#include <iostream>
#include <vector>
extern "C" long sort_doubles(non_null_double_vector* input, non_null_double_vector* out) {

    std::vector<int> vec(4);
    out->data = (double *)malloc(input->count * sizeof(double));
    out->count = input->count;
    memcpy(out->data, input -> data, input->count * sizeof(double));
    frovedis::insertion_sort<double>(out->data, input->count);
    return 0;
}
