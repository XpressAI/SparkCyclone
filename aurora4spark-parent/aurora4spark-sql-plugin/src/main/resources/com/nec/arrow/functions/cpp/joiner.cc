#include "frovedis/core/radix_sort.hpp"
#include "transfer-definitions.c"
#include "frovedis/dataframe/join.hpp"
#include "frovedis/dataframe/join.cc"

#include <vector>
#include <iostream>

extern "C" long join_doubles(non_null_double_vector* left,
                            non_null_double_vector* right,
                            non_null_int_vector* left_key,
                            non_null_int_vector* right_key,
                            non_null_double_vector* out) {
    std::vector<double> left_vec(left_key -> data, left_key->data + left_key -> count);
    std::vector<double> right_vec(right_key -> data, right_key->data + right_key -> count);
    std::vector<size_t> left_idx;

    for(size_t i = 0; i < left_key->count; i++) {
        left_idx.push_back(i);
        std::cout << "Left value: "  << left->data[i] << " Left Key: " << left_key->data[i];
     }

    std::vector<size_t> right_idx;

    for(size_t i = 0; i < right_key->count; i++) {
        right_idx.push_back(i);
        std::cout << "Right value: "  << right->data[i] << " Right Key: " << right_key->data[i];

    }

    std::vector<size_t> right_out;
    std::vector<size_t> left_out;

    frovedis::equi_join<double>(left_vec, left_idx, right_vec, right_idx, left_out, right_out);
    int totalElems = right_out.size() + left_out.size();
    out->data =  (double *) malloc(totalElems * sizeof(double));
    out -> count = totalElems;
    int counter = 0;
    for(int i= 0;i< left_out.size(); i++) {
        std::cout << "HERE";
        std::cout << "ADDING :" << left -> data[left_out[i]];
        out->data[counter] = left->data[left_out[i]];
        counter++;
    }
    for(int i= 0;i< right_out.size(); i++) {
            out->data[counter] = right->data[right_out[i]];
            counter++;
    }

    return 0;
}
