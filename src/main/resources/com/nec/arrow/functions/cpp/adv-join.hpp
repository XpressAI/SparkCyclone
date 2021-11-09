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
    frovedis::words left_words = varchar_vector_to_words(x_a);
    frovedis::words right_words = varchar_vector_to_words(y_a);

    frovedis::compressed_words left_cwords = frovedis::make_compressed_words(left_words);
    frovedis::compressed_words right_cwords = frovedis::make_compressed_words(right_words);

    frovedis::dict left_dict = frovedis::make_dict(left_words);
    frovedis::dict right_dict = frovedis::make_dict(right_words);

    left_words.print();
    std::cout << std::endl;
    right_words.print();
    std::cout << std::endl;

    std::vector<size_t> a = left_dict.lookup(right_cwords);

    std::cout << "keys: ";
    for (int i = 0; i < a.size(); i++) {
        std::cout << a[i] << ", ";
    }
    std::cout << std::endl << "Results: " << std::endl;

    frovedis::words result = left_dict.index_to_words(a);
    result.print();

    std::cout << std::endl;

    words_to_varchar_vector(result, out_a);

    out_b->count = a.size();
    out_b->data = (int32_t *)malloc(out_b->count * sizeof(int32_t));
    out_b->validityBuffer = (uint64_t *)malloc(ceil(out_b->count / 64.0) * sizeof(uint64_t));
    for (int i = 0; i < a.size(); i++) {
        out_b->data[i] = x_c->data[a[i]];
        set_validity(out_b->validityBuffer, i, check_valid(x_c->validityBuffer, a[i]));
    }

    out_c->count = a.size();
    out_c->data = (double *)malloc(out_c->count * sizeof(double));
    out_c->validityBuffer = (uint64_t *)malloc(ceil(out_c->count / 64.0) * sizeof(uint64_t));
    for (int i = 0; i < a.size(); i++) {
        out_c->data[i] = y_c->data[a[i]];
        set_validity(out_b->validityBuffer, i, check_valid(y_c->validityBuffer, a[i]));
    }

    return 0;
}
