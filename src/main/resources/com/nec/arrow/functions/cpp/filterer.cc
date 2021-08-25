#include "frovedis/core/radix_sort.hpp"
#include "frovedis/dataframe/join.hpp"
#include "frovedis/dataframe/join.cc"
#include <iostream>
#include <vector>

extern "C" long filter_doubles_over_15(non_null_double_vector* in,
                                        non_null_double_vector* out)
{
    std::vector<double> in_vec_filtered;
    for ( long i = 0; i < in->count; i++ ) {
        if ( in->data[i] < 15 ) {
            in_vec_filtered.push_back(in->data[i]);
        }
    }
    double *out_data = (double *) malloc(in_vec_filtered.size() * sizeof(double));
    out->count = in_vec_filtered.size();
    memcpy(out_data, in_vec_filtered.data(), out->count * sizeof(double));
    out->data = out_data;

    return 0;
}
