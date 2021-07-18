#include "frovedis/core/radix_sort.hpp"
#include "frovedis/dataframe/join.hpp"
#include "frovedis/dataframe/join.cc"
#include <iostream>
#include <vector>

extern "C" long join_doubles(non_null_double_vector* left,
                             non_null_double_vector* right,
                             non_null_int_vector* left_key,
                             non_null_int_vector* right_key,
                             non_null_double_vector* out)
{
    double *left_data = left->data;
    double *right_data = right->data;

    long left_key_count = left_key->count;
    long right_key_count = right_key->count;

    std::vector<double> left_vec(left_key->data, left_key->data + left_key->count);
    std::vector<double> right_vec(right_key->data, right_key->data + right_key->count);
    std::vector<size_t> left_idx(left_key_count);

    #pragma _NEC vector
    for(size_t i = 0; i < left_key_count; i++) {
        left_idx[i] = i;
     }

    std::vector<size_t> right_idx(right_key_count);

    #pragma _NEC vector
    for(size_t i = 0; i < right_key_count; i++) {
        right_idx[i] = i;
    }

    std::vector<size_t> right_out;
    std::vector<size_t> left_out;

    frovedis::equi_join<double>(left_vec, left_idx, right_vec, right_idx, left_out, right_out);

    size_t left_out_size = left_out.size();
    size_t right_out_size = right_out.size();

    int total_elems = left_out_size + right_out_size;
    double *out_data = (double *) malloc(total_elems * sizeof(double));
    int counter = 0;

    #pragma _NEC vector
    for(int i = 0; i < left_out_size; i++) {
        out_data[i] = left_data[left_out[i]];
    }

    #pragma _NEC vector
    for(int i = 0; i < right_out_size; i++) {
        out_data[left_out_size + i] = right_data[right_out[i]];
    }

    out->data = out_data;
    out->count = total_elems;

    return 0;
}
