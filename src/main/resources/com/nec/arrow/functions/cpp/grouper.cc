#include "frovedis/core/radix_sort.hpp"
#include "frovedis/core/radix_sort.hpp"
#include "frovedis/core/set_operations.hpp"
#include <vector>
#include <iostream>

extern "C" long group_by(non_null_double_vector* grouping_col,
                         non_null_double_vector* values_col,
                         non_null_double_vector* values,
                         non_null_double_vector* groups,
                         non_null_int_vector* counts
                         ) {

     std::vector<double> grouping_vec(grouping_col -> data, grouping_col->data + grouping_col -> count);
     std::vector<size_t> idx;
     for(size_t i = 0; i < grouping_col->count; i++) {
       idx.push_back(i);
     }
     std::cout << "HERE";
     frovedis::radix_sort(grouping_vec, idx, grouping_vec.size());
      std::cout << "HERE2";

     values->data = (double *) malloc(grouping_vec.size() * sizeof(double));
     values->count = grouping_vec.size();
     std::cout << "HERE3";
     std::cout << "COUNT: " << counts->count << "\n";

     //Pre-allocate enough data so that we can return even if only unique ids are there
     (int *) malloc(values_col->count * sizeof(int));
     (double *) malloc(values_col->count * sizeof(double));
     std::cout << "HERE4";

     double last = grouping_vec[0];
     int groups_count = 0;
     int curr_group_size = 0;
      std::cout << "HERE5";

     for(size_t i = 0; i < grouping_vec.size(); i++) {
        if(grouping_vec[i] == last) {
           curr_group_size++;
        } else {
          groups->data[groups_count] = last;
          counts->data[groups_count] = curr_group_size;
          curr_group_size = 1;
          last = grouping_vec[i];
          groups_count++;
        }
        values->data[i] = values_col->data[idx[i]];
     }
           std::cout << "HERE6";

     groups->data[groups_count] = last;
     counts->data[groups_count] = curr_group_size;
     groups_count++;
     groups->count =  groups_count;
     counts->count = groups_count;

     return 0;
}
