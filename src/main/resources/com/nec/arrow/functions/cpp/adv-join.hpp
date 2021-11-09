#include "frovedis/core/radix_sort.hpp"
#include "frovedis/dataframe/join.hpp"
#include "frovedis/dataframe/join.cc"
#include "frovedis/text/words.hpp"
#include "frovedis/text/words.cc"
#include "frovedis/text/dict.hpp"
#include "frovedis/text/dict.cc"

#include <iostream>
#include <vector>
#include <cmath>

extern "C" long adv_join(
    nullable_varchar_vector *x_a,
    nullable_bigint_vector *x_b,
    nullable_int_vector *x_c,
    nullable_varchar_vector *y_a,
    nullable_bigint_vector *y_b,
    nullable_double_vector *y_c,
    nullable_varchar_vector *o_a,
    nullable_int_vector *o_b,
    nullable_double_vector *o_c)
{
    // SELECT X.A, X.C, Y.C FROM X LEFT JOIN Y ON X.A = Y.A AND X.B = Y.B
    // X = [A: String, B: Long, C: Int]
    // Y = [A: String, B: Long, C: Double]

    frovedis::words left_words = varchar_vector_to_words(x_a);
    frovedis::words right_words = varchar_vector_to_words(y_a);

    frovedis::compressed_words left_cwords = frovedis::make_compressed_words(left_words);
    frovedis::compressed_words right_cwords = frovedis::make_compressed_words(right_words);

    frovedis::dict left_dict = frovedis::make_dict(left_words);
    frovedis::dict right_dict = frovedis::make_dict(right_words);

    std::vector<size_t> a = left_dict.lookup(right_cwords);
    std::cout << "a = [";
    for (int i = 0; i < a.size(); i++) {
        std::cout << a[i] << ", ";
    }
    std::cout << "]" << std::endl;

    std::vector<size_t> b = right_dict.lookup(left_cwords);
    std::cout << "b = [";
    for (int i = 0; i < b.size(); i++) {
        std::cout << b[i] << ", ";
    }
    std::cout << "]" << std::endl;

    frovedis::words result = left_dict.index_to_words(a);
    words_to_varchar_vector(result, o_a);

    o_b->count = a.size();
    o_b->data = (int32_t *)malloc(o_b->count * sizeof(int32_t));
    o_b->validityBuffer = (uint64_t *)malloc(ceil(o_b->count / 64.0) * sizeof(uint64_t));
    for (int i = 0; i < a.size(); i++) {
        o_b->data[i] = x_c->data[a[i]];
        set_validity(o_b->validityBuffer, i, check_valid(x_c->validityBuffer, a[i]));
    }

    o_c->count = a.size();
    o_c->data = (double *)malloc(o_c->count * sizeof(double));
    o_c->validityBuffer = (uint64_t *)malloc(ceil(o_c->count / 64.0) * sizeof(uint64_t));
    for (int i = 0; i < a.size(); i++) {
        o_c->data[i] = y_c->data[a[i]];
        set_validity(o_c->validityBuffer, i, check_valid(y_c->validityBuffer, a[i]));
    }

    return 0;
}
