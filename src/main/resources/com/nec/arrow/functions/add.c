#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern "C" long add(non_null_double_vector* input_a, non_null_double_vector* input_b, non_null_double_vector* output)
{
    int i;
    int j;

    non_null_double_vector input_data_a = input_a[0];
    non_null_double_vector input_data_b = input_b[0];

    long output_count = output->count;

#if DEBUG
    printf("Total number of elements received: %d \n", input_data.count);
    printf("Row count of received dataset: %d \n", row_count);
#endif

    double *output_data = (double*) malloc(output_count * sizeof(double));

    #pragma _NEC vector
    for (i = 0; i < output_count; i++) {
       output_data[i] = input_data_a.data[i] + input_data_b.data[i];
    }

    output->data = output_data;

    return 0;
}
