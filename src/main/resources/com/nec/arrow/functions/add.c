#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern "C" long add(non_null_double_vector* input_a, non_null_double_vector* input_b, non_null_double_vector* output)
{
    double *input_data_a = input_a[0].data;
    double *input_data_b = input_b[0].data;

    long output_count = output->count;

#if DEBUG
    printf("Total number of elements received: %d \n", input_a[0].count);
    printf("Row count of received dataset: %d \n", row_count);
#endif

    double *output_data = (double*) malloc(output_count * sizeof(double));

    #pragma _NEC ivdep
    for (int i = 0; i < output_count; i++) {
       output_data[i] = input_data_a[i] + input_data_b[i];
    }

    output->data = output_data;

    return 0;
}
