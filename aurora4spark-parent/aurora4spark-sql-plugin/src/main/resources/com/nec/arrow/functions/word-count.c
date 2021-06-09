#include <stdio.h>
#include <stdlib.h>
#include <string.h>

long count_strings(varchar_vector* strings, non_null_int_vector* counted_string_ids, non_null_int_vector* counted_string_frequencies)
{
    varchar_vector input_strings = strings[0];

#if DEBUG
    printf("Received %i items in pointers data=%p, offsets=%p \n", input_strings.count, input_strings.data, input_strings.offsets);
#endif
    /** pre-allocate enough memory to return all the strings at the maximum **/
    counted_string_ids->data = malloc(input_strings.count * sizeof(int));
    counted_string_ids->count = 0;
    counted_string_frequencies->data = malloc(input_strings.count * sizeof(int));
    counted_string_frequencies->count = 0;

    for (int i = 0; i < input_strings.count; i++)
    {
        int found = 0;

        for (int _j = 0; _j < counted_string_ids->count; _j++)
        {
            int j = counted_string_ids->data[_j];
            int string_i_length = input_strings.offsets[i + 1] - input_strings.offsets[i];
            int string_j_length = input_strings.offsets[j + 1] - input_strings.offsets[j];

            if (string_i_length == string_j_length)
            {
                char *string_i = (char *)(input_strings.data + input_strings.offsets[i]);
                char *string_j = (char *)(input_strings.data + input_strings.offsets[j]);
                if (strncmp(string_i, string_j, string_i_length) == 0)
                {
                    counted_string_frequencies->data[_j]++;
                    found = 1;
                    break;
                }
            }
        }

        if (found == 0)
        {
            int new_item__j = counted_string_ids->count;
            counted_string_ids->data[new_item__j] = i;
            counted_string_frequencies->data[new_item__j] = 1;

            counted_string_ids->count++;
            counted_string_frequencies->count++;
        }
    }
    /** todo free the excess memory before returning **/
    return 0;
}


/** SUM(a-b), AVG(b) **/
int sum_avg_subtract(non_null_double_vector* a, non_null_double_vector* b, non_null_double_vector* c, non_null_double_vector* the_sum, non_null_double_vector* the_avg) {
    
    the_sum->data = malloc(1 * sizeof(double));
    the_avg->data = malloc(1 * sizeof(double));
    the_sum->count = 1;
    the_avg->count = 1;

    double total_sum = 0;
    double running_total = 0;
    for ( int i = 0; i < a->count; i++ ) {
         total_sum += a->data[i] - b->data[i];
         running_total += b->data[i];
    }

    the_sum->data[0] = total_sum;
    the_avg->data[0] = running_total / a->count;

    return 0;
}

