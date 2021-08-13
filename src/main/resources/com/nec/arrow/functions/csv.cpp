#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <utility>
#include <sstream>
#include <vector>
#include "words.hpp"
#include "words.cc"
#include "parsefloat.hpp"
#include "parsefloat.cc"
#include "char_int_conv.hpp"
#include "char_int_conv.cc"

extern "C" long parse_csv(  non_null_c_bounded_string* csv_data,
                            non_null_double_vector* output_a,
                            non_null_double_vector* output_b,
                            non_null_double_vector* output_c)
{
    #if DEBUG
        std::cout << "Parsing " << csv_data->length / 1024.0 / 1024.0 << " MB" << std::endl;
    #endif

    int length = csv_data->length;
    char *data = csv_data->data;

    #if DEBUG    
        std::cout << "Converting to words" << std::endl;
    #endif

    std::vector<int> int_string(length);
    frovedis::char_to_int(data, length, int_string.data());
    
    std::string delims = "\n,";

    #if DEBUG    
        std::cout << "Splitting to words" << std::endl;
    #endif

    frovedis::words words;
    frovedis::split_to_words(int_string, words.starts, words.lens, delims.data());
    words.chars = int_string;

    #if DEBUG
        std::cout << "Parsing to doubles" << std::endl;
    #endif

    std::vector<double> doubles = frovedis::parsenumber<double>(words);

    int count = (doubles.size() / 3) - 1; // ignore header
    size_t mem_len = sizeof (double) * count;
    double *a_data = (double *)malloc(mem_len);
    double *b_data = (double *)malloc(mem_len);
    double *c_data  = (double *)malloc(mem_len);

    #if DEBUG
        std::cout << "Assigning" << std::endl;
    #endif

    #pragma _NEC vector
    for (int i = 1; i <= count; i++) {
        a_data[i - 1] = doubles[i * 3 + 0];
        b_data[i - 1] = doubles[i * 3 + 1];
        c_data[i - 1] = doubles[i * 3 + 2];
    }

    output_a->data = a_data;
    output_a->count = count;
    output_b->data = b_data;
    output_b->count = count;
    output_c->data = c_data;
    output_c->count = count;
    
    return 0;
}
extern "C" long parse_csv_2(  non_null_c_bounded_string* csv_data,
                            non_null_double_vector* output_a,
                            non_null_double_vector* output_b)
{
    #if DEBUG
        std::cout << "Parsing " << csv_data->length / 1024.0 / 1024.0 << " MB" << std::endl;
    #endif

    int length = csv_data->length;
    char *data = csv_data->data;

    #if DEBUG    
        std::cout << "Converting to words" << std::endl;
    #endif

    std::vector<int> int_string(length);
    frovedis::char_to_int(data, length, int_string.data());
    
    std::string delims = "\n,";

    #if DEBUG    
        std::cout << "Splitting to words" << std::endl;
    #endif

    frovedis::words words;
    frovedis::split_to_words(int_string, words.starts, words.lens, delims.data());
    words.chars = int_string;

    #if DEBUG
        std::cout << "Parsing to doubles" << std::endl;
    #endif

    std::vector<double> doubles = frovedis::parsenumber<double>(words);

    int count = (doubles.size() / 2) - 1; // ignore header
    size_t mem_len = sizeof (double) * count;
    double *a_data = (double *)malloc(mem_len);
    double *b_data = (double *)malloc(mem_len);

    #if DEBUG
        std::cout << "Assigning" << std::endl;
    #endif

    #pragma _NEC vector
    for (int i = 1; i <= count; i++) {
        a_data[i - 1] = doubles[i * 2 + 0];
        b_data[i - 1] = doubles[i * 2 + 1];
    }

    output_a->data = a_data;
    output_a->count = count;
    output_b->data = b_data;
    output_b->count = count;
    
    return 0;
}

extern "C" long parse_csv_1(  non_null_c_bounded_string* csv_data,
                            non_null_double_vector* output_a)
{
    #if DEBUG
        std::cout << "Parsing " << csv_data->length / 1024.0 / 1024.0 << " MB" << std::endl;
    #endif

    int length = csv_data->length;
    char *data = csv_data->data;

    #if DEBUG    
        std::cout << "Converting to words" << std::endl;
    #endif

    std::vector<int> int_string(length);
    frovedis::char_to_int(data, length, int_string.data());
    
    std::string delims = "\n";

    #if DEBUG    
        std::cout << "Splitting to words" << std::endl;
    #endif

    frovedis::words words;
    frovedis::split_to_words(int_string, words.starts, words.lens, delims.data());
    words.chars = int_string;

    #if DEBUG
        std::cout << "Parsing to doubles" << std::endl;
    #endif

    std::vector<double> doubles = frovedis::parsenumber<double>(words);

    int count = doubles.size() - 1; // ignore header
    size_t mem_len = sizeof (double) * count;
    double *a_data = (double *)malloc(mem_len);

    #if DEBUG
        std::cout << "Assigning" << std::endl;
    #endif

    #pragma _NEC vector
    for (int i = 1; i <= count; i++) {
        a_data[i - 1] = doubles[i];
    }

    output_a->data = a_data;
    output_a->count = count;
    
    return 0;
}
