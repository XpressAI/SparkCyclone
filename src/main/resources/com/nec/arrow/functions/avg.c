#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern "C"  long vector_avg(non_null_double_vector* input, non_null_double_vector* output)
{
    double *input_data = input[0].data;
    int row_count = input_data.count / output->count;
    long output_count = output->count;

    #if DEBUG
        printf("Total number of elements received: %d \n", input_data.count);
        printf("Row count of received dataset: %d \n", row_count);
    #endif

    double *output_data = (double*)malloc(output_count * sizeof(double));
    
    for (int i = 0; i < output_count; i++) {
       double sum = 0;

       #pragma _NEC ivdep
       for(int j = 0; j < row_count; j++){
          sum += input_data.data[i + (j * output_count)];
       }

       output->data[i] = sum / row_count;
    }

    output->data = output_data;
    output->count = output_count;
    
    return 0;
}
