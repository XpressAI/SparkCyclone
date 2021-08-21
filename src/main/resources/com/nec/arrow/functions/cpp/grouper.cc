#include "frovedis/core/radix_sort.hpp"
#include "frovedis/core/set_operations.hpp"
#include <vector>
#include <iostream>
#include <cmath>

extern "C" long group_by(nullable_double_vector* grouping_col,
                         nullable_double_vector* values_col,
                         nullable_double_vector* values,
                         nullable_double_vector* groups,
                         nullable_double_vector* counts)
{
    std::vector<double> grouping_vec(grouping_col->data, grouping_col->data + grouping_col->count);
    size_t count = grouping_col->count;

    std::vector<size_t> idx(count);

    #pragma _NEC ivdep
    for (size_t i = 0; i < count; i++) {
        idx[i] = i;
    }

    frovedis::radix_sort(grouping_vec, idx, grouping_vec.size());

    // {0,0,2,3,4,4,4,5} -> {0,2,3,4,7,8}
    std::vector<size_t> groups_indicies = frovedis::set_separate(grouping_vec);
    size_t groups_count = groups_indicies.size();

    double *values_data = (double *) malloc(grouping_vec.size() * sizeof(double));
    double *counts_data = (double *) malloc((groups_count - 1) * sizeof(double));
    double *groups_data = (double *) malloc((groups_count - 1) * sizeof(double));

    #pragma _NEC ivdep
    for (size_t i = 0; i < groups_count - 1; i++) {
        size_t idx_1 = groups_indicies[i];
        size_t idx_2 = groups_indicies[i + 1];
        size_t count = idx_2 - idx_1;
        counts_data[i] = count;
    }

    #pragma _NEC ivdep
    for (size_t i = 0; i < groups_count - 1; i++) {
        groups_data[i] = grouping_vec[groups_indicies[i]];
    }

    #pragma _NEC ivdep
    for (size_t i = 0; i < grouping_vec.size(); i++) {
        values_data[i] = values_col->data[idx[i]];
    }

    int groupsValidityBufferSize = ceil((groups_count - 1)/8.0);
    int valuesValidityBufferSize = ceil(grouping_vec.size()/8.0);
    values->validityBuffer = (unsigned char *)malloc(grouping_vec.size() * sizeof(unsigned char));
    groups->validityBuffer = (unsigned char *)malloc(groupsValidityBufferSize * sizeof(unsigned char));
    counts->validityBuffer = (unsigned char *)malloc(groupsValidityBufferSize * sizeof(unsigned char));

     for (int i =0; i < valuesValidityBufferSize; i++) {
        if(i < groupsValidityBufferSize) {
            groups->validityBuffer[i] = 255;
            counts->validityBuffer[i] = 255;
        }
        values->validityBuffer[i] = 255;
    }

    groups->data = groups_data;
    groups->count =  groups_count - 1;

    counts->data = counts_data;
    counts->count = groups_count - 1;

    values->data = values_data;
    values->count = grouping_vec.size();

    return 0;
}

extern "C" long group_by_sum(nullable_double_vector* grouping_col,
                            nullable_double_vector* values_col,
                            nullable_double_vector* values,
                            nullable_double_vector* groups)
{
     std::vector<double> grouping_vec(grouping_col->data, grouping_col->data + grouping_col->count);
     size_t count = grouping_col->count;


     std::vector<size_t> idx(count);
     #pragma _NEC ivdep
     for(size_t i = 0; i < count; i++) {
       idx[i] = i;
     }
     frovedis::radix_sort(grouping_vec, idx, grouping_vec.size());
    // {0,0,2,3,4,4,4,5} -> {0,2,3,4,7,8}
    std::vector<size_t> groups_indicies = frovedis::set_separate(grouping_vec);
    size_t groups_count = groups_indicies.size();

    double *values_data = (double *) malloc(grouping_vec.size() * sizeof(double));
    int *counts_data = (int *) malloc((groups_count - 1) * sizeof(int));
    double *groups_data = (double *) malloc((groups_count - 1) * sizeof(double));

    #pragma _NEC ivdep
    for (size_t i = 0; i < groups_count - 1; i++) {
        size_t idx_1 = groups_indicies[i];
        size_t idx_2 = groups_indicies[i + 1];
        size_t count = idx_2 - idx_1;
        counts_data[i] = count;
    }

    #pragma _NEC ivdep
    for (size_t i = 0; i < groups_count - 1; i++) {
        groups_data[i] = grouping_vec[groups_indicies[i]];
    }

    double *values_col_data = values_col->data;

    int validityBufferByteSize = ceil((groups_count - 1)/8.0);
    values->validityBuffer = (unsigned char *)malloc(validityBufferByteSize * sizeof(unsigned char));
    groups->validityBuffer = (unsigned char *)malloc(validityBufferByteSize * sizeof(unsigned char));

    for (int i =0; i < validityBufferByteSize; i++) {
    values->validityBuffer[i] = 255;
    groups->validityBuffer[i] = 255;
    }

    for (size_t i = 0; i < groups_count - 1; i++) {
        size_t start = groups_indicies[i];
        size_t end = groups_indicies[i + 1];
        double sum = 0;

        #pragma _NEC ivdep
        for (size_t j = start; j < end; j++) {
            sum += values_col_data[idx[j]];
        }
        values_data[i] = sum;

    }

    groups->data = groups_data;
    groups->count =  groups_count - 1;

    values->data = values_data;
    values->count = groups_count - 1;

    return 0;
}
