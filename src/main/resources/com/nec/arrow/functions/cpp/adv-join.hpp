#include "frovedis/core/radix_sort.hpp"
#include "frovedis/dataframe/join.hpp"
#include "frovedis/dataframe/join.cc"
#include "frovedis/text/words.hpp"
#include "frovedis/text/words.cc"

#include <iostream>
#include <vector>
#include <cmath>

/*
NativeArgument.input(x_a),
NativeArgument.input(x_b),
NativeArgument.input(x_c),
NativeArgument.input(y_a),
NativeArgument.input(y_b),
NativeArgument.input(y_c),
NativeArgument.output(o_a),
NativeArgument.output(o_b),
NativeArgument.output(o_c)
x_a <- vb.stringVector(left.map(_._1))
x_b <- vb.longVector(left.map(_._2))
x_c <- vb.intVector(left.map(_._3))
y_a <- vb.stringVector(right.map(_._1))
y_b <- vb.longVector(right.map(_._2))
y_c <- vb.doubleVector(right.map(_._3))
o_a <- vb.stringVector(Seq.empty)
o_b <- vb.intVector(Seq.empty)
o_c <- vb.doubleVector(Seq.empty)
*/
extern "C" long adv_join(
    nullable_varchar_vector *x_a,
    nullable_bigint_vector *x_b,
    nullable_int_vector *x_c,
    nullable_varchar_vector *y_a,
    nullable_bigint_vector *y_b,
    nullable_double_vector *y_c,
    nullable_varchar_vector *o_a
    nullable_int_vector *o_b,
    nullable_double_vector *o_c)
{

    long *left_data = left->data;
    long *right_data = right->data;

    long left_key_count = left_key->count;
    long right_key_count = right_key->count;

    std::vector<double> left_vec(left_key->data, left_key->data + left_key->count);
    std::vector<double> right_vec(right_key->data, right_key->data + right_key->count);
    std::vector<size_t> left_idx(left_key_count);

    #pragma _NEC ivdep
    for(size_t i = 0; i < left_key_count; i++) {
        left_idx[i] = i;
     }

    std::vector<size_t> right_idx(right_key_count);

    #pragma _NEC ivdep
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
    int validityBufferSize = ceil(total_elems / 64.0);
    out->validityBuffer = (uint64_t *) malloc(validityBufferSize * sizeof(uint64_t));

    #pragma _NEC ivdep
    for(int i = 0; i < left_out_size; i++) {
        out_data[i] = left_data[left_out[i]];
    }

    #pragma _NEC ivdep
    for(int i = 0; i < validityBufferSize ; i++) {
        out->validityBuffer[i] = 0xFFFFFFFFFFFFFFFF;
    }

    #pragma _NEC ivdep
    for(int i = 0; i < right_out_size; i++) {
        out_data[left_out_size + i] = right_data[right_out[i]];
    }

    out->data = out_data;
    out->count = total_elems;

    return 0;
}
