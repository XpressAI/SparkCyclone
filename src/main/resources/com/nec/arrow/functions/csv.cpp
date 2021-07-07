#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <utility>
#include <sstream>
#include <vector>
#include <omp.h>
#include "words.hpp"
#include "parsefloat.hpp"

#ifdef __VE__

extern "C" long parse_csv(  non_null_c_bounded_string* csv_data,
                            non_null_double_vector* output_a,
                            non_null_double_vector* output_b,
                            non_null_double_vector* output_c)
{
    int length = csv_data->length;
    char *data = csv_data->data;

    // Find new line and comma indicies
    std::vector<int> new_lines = {};
    std::vector<int> commas = {};

    for (int i = 0; i < length; i++) {
        if (data[i] == '\n') {
            new_lines.push_back(i);
        } else if (data[i] == ',') {
            commas.push_back(i);
        }
    }

    int count = new_lines.size() - 2;

    // XXX: get "[VE] ERROR: signalHandler() Interrupt signal 11 received" unless I block something here.
    std::cout << std::endl;

    size_t mem_len = sizeof (double) * count;
    double *a_data = (double *)malloc(mem_len);
    double *b_data = (double *)malloc(mem_len);
    double *c_data  = (double *)malloc(mem_len);

    std::string *parts = (std::string *)malloc(sizeof(std::string) * (count * 3));

    #pragma _NEC cncall
    for (int i = 1; i <= count; i++) {
        int line_start = new_lines[i - 1] + 1;
        int line_end = new_lines[i];

        int col_a_end = commas[(i * 2)];
        int col_b_end = commas[(i * 2) + 1];

        std::string s1(data + line_start, col_a_end - line_start);
        std::string s2(data + col_a_end + 1, col_b_end - (col_a_end + 1));
        std::string s3(data + col_b_end + 1, line_end - (col_b_end + 1));

        parts[(i - 1) * 3 + 0] = s1;
        parts[(i - 1) * 3 + 1] = s2;
        parts[(i - 1) * 3 + 2] = s3;

        line_start = line_end + 1;
    }

    std::vector<std::string> strings;
    strings.assign(parts, parts + (count * 3));
    frovedis::words w = frovedis::vector_string_to_words(strings);
    std::vector<double> doubles = frovedis::parsenumber<double>(w);

    #pragma _NEC vector
    for (int i = 0; i < count; i++) {
        a_data[i] = doubles[i * 3 + 0];
        b_data[i] = doubles[i * 3 + 1];
        c_data[i] = doubles[i * 3 + 2];
    }

    output_a->data = a_data;
    output_a->count = count;
    output_b->data = b_data;
    output_b->count = count;
    output_c->data = c_data;
    output_c->count = count;

    free(parts);

    return 0;
}


extern "C" long parse_csv_2(  non_null_c_bounded_string* csv_data,
                            non_null_double_vector* output_a,
                            non_null_double_vector* output_b)
{
    std::cout << std::endl;

    int length = csv_data->length;
    char *data = csv_data->data;

    // Find new line and comma indicies
    std::vector<int> new_lines = {};
    std::vector<int> commas = {};

    for (int i = 0; i < length; i++) {
        if (data[i] == '\n') {
            new_lines.push_back(i);
        } else if (data[i] == ',') {
            commas.push_back(i);
        }
    }

    int count = new_lines.size() - 2;

    // XXX: get "[VE] ERROR: signalHandler() Interrupt signal 11 received" unless I block something here.
    std::cout << std::endl;

    size_t mem_len = sizeof (double) * count;
    double *a_data = (double *)malloc(mem_len);
    double *b_data = (double *)malloc(mem_len);

    std::string *parts = (std::string *)malloc(sizeof(std::string) * (count * 2));

    std::cout << std::endl;

    #pragma _NEC cncall
    for (int i = 1; i <= count; i++) {
        int line_start = new_lines[i - 1] + 1;
        int line_end = new_lines[i];

        int col_a_end = commas[i];

        std::string s1(data + line_start, col_a_end - line_start);
        std::string s2(data + col_a_end + 1, line_end - (col_a_end + 1));

        parts[(i - 1) * 2 + 0] = s1;
        parts[(i - 1) * 2 + 1] = s2;

        line_start = line_end + 1;
    }

    std::cout << std::endl;

    std::vector<std::string> strings;
    strings.assign(parts, parts + (count * 2));
    frovedis::words w = frovedis::vector_string_to_words(strings);
    
    std::vector<double> doubles = frovedis::parsenumber<double>(w);

    std::cout << std::endl;

    #pragma _NEC vector
    for (int i = 0; i < count; i++) {
        a_data[i] = doubles[i * 2 + 0];
        b_data[i] = doubles[i * 2 + 1];
    }

    std::cout << std::endl;

    output_a->data = a_data;
    output_a->count = count;
    output_b->data = b_data;
    output_b->count = count;

    free(parts);

    return 0;
}



extern "C" long parse_csv_1(  non_null_c_bounded_string* csv_data,
                            non_null_double_vector* output_a)
{
    int length = csv_data->length;
    char *data = csv_data->data;

    // Find new line and comma indicies
    std::vector<int> new_lines = {};

    for (int i = 0; i < length; i++) {
        if (data[i] == '\n') {
            new_lines.push_back(i);
        }
    }

    int count = new_lines.size() - 2;

    // XXX: get "[VE] ERROR: signalHandler() Interrupt signal 11 received" unless I block something here.
    std::cout << std::endl;

    size_t mem_len = sizeof (double) * count;
    double *a_data = (double *)malloc(mem_len);

    std::string *parts = (std::string *)malloc(sizeof(std::string) * count);

    #pragma _NEC cncall
    for (int i = 1; i <= count; i++) {
        int line_start = new_lines[i - 1] + 1;
        int line_end = new_lines[i];

        std::string s1(data + line_start, line_end - line_start);

        parts[i - 1] = s1;

        line_start = line_end + 1;
    }

    std::vector<std::string> strings;
    strings.assign(parts, parts + count);
    frovedis::words w = frovedis::vector_string_to_words(strings);
    std::vector<double> doubles = frovedis::parsenumber<double>(w);

    #pragma _NEC vector
    for (int i = 0; i < count; i++) {
        a_data[i] = doubles[i];
    }

    output_a->data = a_data;
    output_a->count = count;

    free(parts);

    return 0;
}

#else

extern "C" long parse_csv(  non_null_c_bounded_string* csv_data,
                            non_null_double_vector* output_a,
                            non_null_double_vector* output_b,
                            non_null_double_vector* output_c)
{
    std::string x(csv_data->data, csv_data->length);
    std::istringstream input2;
    input2.str(x);
    std::string line;
    std::vector<double> a_values = {};
    std::vector<double> b_values = {};
    std::vector<double> c_values = {};
    

    int line_idx = 0;
    int output_idx = -1;
    
    while(getline(input2, line, '\n')) {
        if ( !line.empty()) {
            if ( !(output_idx < 0) ) {
                std::stringstream ss(line);
                std::string part;
                getline(ss, part, ',');
                double val_a = std::stod(part);
                getline(ss, part, ',');
                double val_b = std::stod(part);
                getline(ss, part, ',');
                double val_c = std::stod(part);
                a_values.push_back(val_a);
                b_values.push_back(val_b);
                c_values.push_back(val_c);
            }
            line_idx++;
            output_idx++;
        }
    }

    int count = output_idx;

    size_t mem_len = sizeof (double) * count;

    output_a->data = (double *)malloc (mem_len);
    memcpy(output_a->data, a_values.data(), mem_len),
    output_a->count = count;

    output_b->data = (double *)malloc (mem_len);
    memcpy(output_b->data, b_values.data(), mem_len),
    output_b->count = count;

    output_c->data = (double *)malloc (mem_len);
    memcpy(output_c->data, c_values.data(), mem_len),
    output_c->count = count;


    return 0;
}


extern "C" long parse_csv_2(  non_null_c_bounded_string* csv_data,
                            non_null_double_vector* output_a,
                            non_null_double_vector* output_b)
{
    std::string x(csv_data->data, csv_data->length);
    std::istringstream input2;
    input2.str(x);
    std::string line;
    std::vector<double> a_values = {};
    std::vector<double> b_values = {};


    int line_idx = 0;
    int output_idx = -1;

    while(getline(input2, line, '\n')) {
        if ( !line.empty()) {
            if ( !(output_idx < 0) ) {
                std::stringstream ss(line);
                std::string part;
                getline(ss, part, ',');
                double val_a = std::stod(part);
                getline(ss, part, ',');
                double val_b = std::stod(part);
                a_values.push_back(val_a);
                b_values.push_back(val_b);
            }
            line_idx++;
            output_idx++;
        }
    }

    int count = output_idx;

    size_t mem_len = sizeof (double) * count;

    output_a->data = (double *)malloc (mem_len);
    memcpy(output_a->data, a_values.data(), mem_len),
    output_a->count = count;

    output_b->data = (double *)malloc (mem_len);
    memcpy(output_b->data, b_values.data(), mem_len),
    output_b->count = count;


    return 0;
}



extern "C" long parse_csv_1(  non_null_c_bounded_string* csv_data,
                            non_null_double_vector* output_a)
{
    std::string x(csv_data->data, csv_data->length);
    std::istringstream input2;
    input2.str(x);
    std::string line;
    std::vector<double> a_values = {};


    int line_idx = 0;
    int output_idx = -1;

    while(getline(input2, line, '\n')) {
        if ( !line.empty()) {
            if ( !(output_idx < 0) ) {
                std::stringstream ss(line);
                std::string part;
                getline(ss, part, ',');
                double val_a = std::stod(part);
                a_values.push_back(val_a);
            }
            line_idx++;
            output_idx++;
        }
    }

    int count = output_idx;

    size_t mem_len = sizeof (double) * count;

    output_a->data = (double *)malloc (mem_len);
    memcpy(output_a->data, a_values.data(), mem_len),
    output_a->count = count;


    return 0;
}
#endif

