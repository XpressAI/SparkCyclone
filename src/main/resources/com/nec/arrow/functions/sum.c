#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern "C" long sum_vectors(non_null_double_vector* input, non_null_double_vector* output)
{
    double *input_data = input[0].data;
    int output_count = output->count;
    int row_count = input[0].count / output_count;

#if DEBUG
    printf("Total number of elements received: %d \n", input[0].count);
    printf("Row count of received dataset: %d \n", row_count);
#endif
    double *output_data = (double *)malloc(output_count * sizeof(double));

    for (int i = 0; i < output_count; i++) {
        double sum = 0;

        #pragma _NEC ivdep
        for (int j = 0; j < row_count; j++){
            sum += input_data[i + (j * output_count)];
        }

       output_data[i] = sum;
    }
    
    output->data = output_data;
    output->count = output_count;

    return 0;
}
