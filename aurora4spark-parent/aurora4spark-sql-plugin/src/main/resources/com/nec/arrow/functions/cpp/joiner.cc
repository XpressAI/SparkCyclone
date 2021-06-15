#include "frovedis/core/radix_sort.hpp"
#include "transfer-definitions.c"
#include "frovedis/dataframe/join.hpp"
#include <iostream>

extern "C" long join_doubles(non_null_double_vector* left,
    non_null_double_vector* right,
    non_null_double_vector* out) {
    // ret.append_column(cnames[i], col);
    double* dd;

    std::vector<double> left_vec(left -> data, left->data + left -> count);
    std::vector<double> right_vec(right -> data, right->data + right -> count);
    size_t left_size = 0;
    for(size_t i = 0; i < left -> count; i++)
        left_size += sizeof(left -> data[i]);

    std::vector<size_t> left_idx(left_size);
    size_t right_size = 0;

    for(size_t i = 0; i < right -> count; i++)
          right_size += sizeof(left -> data[i]);

    std::vector<size_t> right_idx(right_size);

    std::vector<size_t> right_out;
    std::vector<size_t> left_out;

    frovedis::equi_join<double>(left_vec, left_idx, right_vec, right_idx, left_out, right_out);

    for(int i =0; i < right_out.size(); i++)
        std::cout << right_out[i] << "\n";
    return 0;
}
