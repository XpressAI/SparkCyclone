#include "frovedis/core/radix_sort.hpp"
#include "frovedis/dataframe/join.hpp"
#include "frovedis/dataframe/join.cc"
#include <iostream>
#include <vector>
#include <cmath>
extern "C" long filter_doubles_over_15(nullable_double_vector* in,
                                        nullable_double_vector* out)
{
    std::vector<double> in_vec_filtered;
    for ( long i = 0; i < in->count; i++ ) {
        if ( in->data[i] < 15 ) {
            in_vec_filtered.push_back(in->data[i]);
        }
    }
    double *out_data = (double *) malloc(in_vec_filtered.size() * sizeof(double));
    out->count = in_vec_filtered.size();

    int validitySize = ceil(in_vec_filtered.size() / 64.0);
    memcpy(out_data, in_vec_filtered.data(), out->count * sizeof(double));
    out->data = out_data;
    out->validityBuffer = (uint64_t *)malloc(validitySize * sizeof(uint64_t));
    for(long i =0; i < validitySize; i++) {
        out->validityBuffer[i] = 0xFFFFFFFFFFFFFFFF;
    }
    return 0;
}
