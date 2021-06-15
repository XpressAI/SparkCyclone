#include "frovedis/core/radix_sort.hpp"
#include "transfer-definitions.c"
#include "frovedis/dataframe/join.hpp"
#include "frovedis/dataframe/join.cc"

#include <vector>
#include <iostream>

extern "C" long join_doubles(non_null_double_vector* input, non_null_double_vector* out) {
   std::vector<double> left_vec(input -> data, input->data + input -> count);
   std::vector<double> right_vec(input -> data, input->data + input -> count);
  std::vector<size_t> left_idx;

    left_vec[3] = 10.0;
    left_vec[4] = 20.0;
   for(size_t i = 0; i < left_vec.size(); i++)
         left_idx.push_back(i);

   std::vector<size_t> right_idx;

   for(size_t i = 0; i < right_vec.size(); i++) {
        if(i>=2) {
            right_idx.push_back(i+ 10);
        } else {
            right_idx.push_back(i);
        }
   }

   std::vector<size_t> right_out;
   std::vector<size_t> left_out;
    for(int i=0; i<left_idx.size(); ++i)
          std::cout << "Left: " << left_idx[i] << ' ' << "Right: " << right_idx[i] << "\n";
    std::cout << "\n";
    frovedis::equi_join<double>(left_vec, left_idx, right_vec, right_idx, left_out, right_out);
   for(int i=0; i<left_out.size(); ++i)
             std::cout << "Left: " << left_out[i] << ' ' << "Right: " << right_out[i] << "\n";
    out->data = (double *)malloc(input->count * sizeof(double));
    out->count = input->count;
    memcpy(out->data, input -> data, input->count * sizeof(double));
    frovedis::insertion_sort<double>(out->data, input->count);
  return 0;
}
