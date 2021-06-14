#include "frovedis/core/radix_sort.hpp"
#include "transfer-definitions.c"
#include "frovedis/dataframe/join.hpp"

extern "C" long join_doubles(non_null_double_vector* input, non_null_double_vector* out) {
    // ret.append_column(cnames[i], col);

    out->data = (double *)malloc(input->count * sizeof(double));
    out->count = input->count;
    memcpy(out->data, input -> data, input->count * sizeof(double));
    frovedis::insertion_sort<double>(out->data, input->count);
    return 0;
}
