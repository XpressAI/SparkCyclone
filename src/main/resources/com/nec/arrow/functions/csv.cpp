#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <utility>
#include <sstream>
#include <vector>

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
