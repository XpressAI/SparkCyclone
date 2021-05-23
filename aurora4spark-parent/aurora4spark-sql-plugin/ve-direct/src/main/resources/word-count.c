#include <stdio.h>
#include <stdlib.h>
#include <string.h>

count_strings(varchar_vector strings, non_null_int_vector counted_string_ids, non_null_int_vector counted_string_frequencies)
{
    printf("Received %i items in pointers data=%p, offsets=%p \n", strings.offsets, strings.data, strings.count);
    /** pre-allocate enough memory to return all the strings at the maximum **/
    counted_string_ids.data = malloc(strings.count * sizeof(int));
    counted_string_ids.count = 0;
    counted_string_frequencies.data = malloc(strings.count * sizeof(int));
    counted_string_frequencies.count = 0;

    for (int i = 0; i < strings.count; i++)
    {
        int found = 0;

        for (int _j = 0; _j < counted_string_ids.count; _j++)
        {
            int j = counted_string_ids.data[_j];
            int string_i_length = strings.offsets[i + 1] - strings.offsets[i];
            int string_j_length = strings.offsets[j + 1] - strings.offsets[j];

            if (string_i_length == string_j_length)
            {
                char *string_i = (char *)(strings.data + strings.offsets[i]);
                char *string_j = (char *)(strings.data + strings.offsets[j]);
                if (strncmp(string_i, string_j, string_i_length) == 0)
                {
                    counted_string_frequencies.data[_j]++;
                    found = 1;
                    break;
                }
            }
        }

        if (found == 0)
        {
            int new_item__j = counted_string_ids.count;
            counted_string_ids.data[new_item__j] = i;
            counted_string_frequencies.data[new_item__j] = 1;

            counted_string_ids.count++;
            counted_string_frequencies.count++;
        }
    }
    /** todo free the excess memory before returning **/
    return 0;
}
