#include <stdio.h>
#include <stdlib.h>
#include <string.h>

long add1(non_null_double_vector* input, non_null_double_vector* output)
{
    output->count = input->count;
    output->data = malloc(output->count * sizeof(double));
    for ( int i = 0; i < input->count; i++ ) {
        output->data[i] = input->data[i] + 1;
    }
    return 0;
}

long mul2(non_null_double_vector* input, non_null_double_vector* output)
{
    output->count = input->count;
    output->data = malloc(output->count * sizeof(double));
    for ( int i = 0; i < input->count; i++ ) {
        output->data[i] = input->data[i] * 2;
    }
    return 0;
}
