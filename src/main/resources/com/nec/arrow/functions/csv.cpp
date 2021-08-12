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

#ifndef _WIN32
#include "unix-read.cpp"
#endif

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

#ifndef _WIN32

extern "C" long parse_csv_ipc(  non_null_c_bounded_string* input_sock_name,
                            non_null_double_vector* output_a,
                            non_null_double_vector* output_b,
                            non_null_double_vector* output_c) {

    non_null_varchar_vector temp_input_str;
    read_fully_2(input_sock_name, &temp_input_str);
#ifdef DEBUG
    std::cout << "Read input size " << temp_input_str.size << std::endl << std::flush;
#endif
    non_null_c_bounded_string temp_input_str_;
    temp_input_str_.data = temp_input_str.data;
    temp_input_str_.length = temp_input_str.size;
    long result = parse_csv(&temp_input_str_, output_a, output_b, output_c);
#ifdef DEBUG
    std::cout << "output_a count = " << output_a->count << std::endl << std::flush;
#endif
    return result;
}

extern "C" long parse_csv_2_ipc(  non_null_c_bounded_string* input_sock_name,
                            non_null_double_vector* output_a,
                            non_null_double_vector* output_b) {

    non_null_varchar_vector temp_input_str;
    read_fully_2(input_sock_name, &temp_input_str);
#ifdef DEBUG
    std::cout << "Read input size " << temp_input_str.size << std::endl << std::flush;
#endif
    non_null_c_bounded_string temp_input_str_;
    temp_input_str_.data = temp_input_str.data;
    temp_input_str_.length = temp_input_str.size;
    return parse_csv_2(&temp_input_str_, output_a, output_b);
}
#endif
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

#ifndef _WIN32

extern "C" long parse_csv_1_ipc(  non_null_c_bounded_string* input_sock_name,
                            non_null_double_vector* output_a) {

    non_null_varchar_vector temp_input_str;
    read_fully_2(input_sock_name, &temp_input_str);
#ifdef DEBUG
    std::cout << "Read input size " << temp_input_str.size << std::endl << std::flush;
#endif
    non_null_c_bounded_string temp_input_str_;
    temp_input_str_.data = temp_input_str.data;
    temp_input_str_.length = temp_input_str.size;
    return parse_csv_1(&temp_input_str_, output_a);
}

/** Static stub implementation -- requires reimplementation for dynamic inputs here **/
extern "C" long parse_csv_double1_str2_int3_long4(
    non_null_c_bounded_string* csv_data,
    non_null_double_vector* output_a,
    non_null_varchar_vector* output_b,
    non_null_int_vector* output_c,
    non_null_bigint_vector* output_d)
{
    output_a->data = (double *)malloc(2 * sizeof(double));
    output_a->data[0] = 1.0;
    output_a->data[1] = 2.0;
    output_a->count = 2;

    const char* output_str = "one point zerotwoPointZero";
    output_b->data = (char *)malloc(26 * sizeof(char));
    strncpy(output_b->data, output_str, 26);
    output_b->offsets = (int *)malloc(3 * sizeof(int));
    output_b->offsets[0] = 0;
    output_b->offsets[1] = 14;
    output_b->offsets[2] = 26;
    output_b->count = 2;
    output_b->size = 26;

    output_c->data = (int *)malloc(2 * sizeof(int));
    output_c->data[0] = 1;
    output_c->data[1] = 2;
    output_c->count = 2;

    output_d->data = (long *)malloc(2 * sizeof(long));
    output_d->data[0] = 10000000000000L;
    output_d->data[1] = 10000000000001L;
    output_d->count = 2;

    return 0;
}

#endif
