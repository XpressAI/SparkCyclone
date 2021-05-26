#include <stdio.h>
#include <stdlib.h>
#include <string.h>

long sum(non_null_double_vector* input, non_null_double_vector* output)
{
    int i;
    int j;

    non_null_double_vector input_data = input[0];
    int row_count = input_data.count/output->count;

    printf("%d \n", input_data.count);
    printf("%d \n", row_count);
    output->data = malloc(output->count * sizeof(double));

    for (i = 0; i < output->count; i++) {
       double sum = 0;
       for(j = 0; j< row_count; j++){
          sum += input_data.data[(i*row_count) + j];
       }

       printf("%f \n", sum);
       output->data[i] = sum;
    }
       printf("DONIECK");

    return 0;
}