#include <stdio.h>
#include <stdlib.h>
#include <string.h>

long add_n(non_null_double_vector* input, double num, non_null_double_vector* output)
{
    output->count = input->count;
    output->data = malloc(output->count * sizeof(double));
    for ( int i = 0; i < input->count; i++ ) {
        output->data[i] = input->data[i] + num;
    }
    return 0;
}

long mul_n(non_null_double_vector* input, double num, non_null_double_vector* output)
{
    output->count = input->count;
    output->data = malloc(output->count * sizeof(double));
    for ( int i = 0; i < input->count; i++ ) {
        output->data[i] = input->data[i] * num;
    }
    return 0;
}
