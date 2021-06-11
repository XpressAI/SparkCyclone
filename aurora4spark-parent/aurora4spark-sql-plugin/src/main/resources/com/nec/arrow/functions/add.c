#include <stdio.h>
#include <stdlib.h>
#include <string.h>

long add(non_null_double_vector* input_a, non_null_double_vector* input_b, non_null_double_vector* output)
{
    int i;
    int j;

    non_null_double_vector input_data_a = input_a[0];
    non_null_double_vector input_data_b = input_b[0];

#if DEBUG
    printf("Total number of elements received: %d \n", input_data.count);
    printf("Row count of received dataset: %d \n", row_count);
#endif

    output->data = malloc(output->count * sizeof(double));

    #pragma omp parallel for
    for (i = 0; i < output->count; i++) {
       output->data[i] = input_data_a.data[i] + input_data_b.data[i];
    }

    return 0;
}
