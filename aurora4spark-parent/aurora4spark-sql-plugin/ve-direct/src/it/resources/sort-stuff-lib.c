#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int add(int a, int b) {
    return a + b;
}

int add_nums(int* nums, int size) {
    int sum = 0;
    for ( int i = 0; i < size; i++ ) {
        sum += nums[i];
    }
    return sum;
}

typedef struct {
    int string_i;
    int count;
} unique_position_counter;

long count_strings_ve(void* strings, int* string_positions, int* string_lengths, int num_strings, long** ve_data_position) {
    return count_strings(strings, string_positions, string_lengths, num_strings, ve_data_position);
}

int count_strings(void* strings, int* string_positions, int* string_lengths, int num_strings, void** rets) {

    unique_position_counter* ress = malloc(num_strings * sizeof(unique_position_counter));
    int counted_items_size = 0;

    for ( int i = 0; i < num_strings; i++ ) {
        int found = 0;
        int i_string_position = string_positions[i];
        void *string_position = ((char *)strings) + i_string_position;
        char* str_i = (char*)(string_position);
        int i_length = string_lengths[i];

        for ( int j = 0; j < counted_items_size; j++ ) {
            int j_string_position = string_positions[ress[j].string_i];
            char* str_right = (char*)(((char*)strings) + j_string_position);

            if ( strncmp(str_i, str_right, string_lengths[i]) == 0 ) {
                ress[j].count++;
                found = 1;
                break;
            }
        }

        if ( found == 0 ) {
            ress[counted_items_size].string_i = i;
            ress[counted_items_size].count = 1;
            counted_items_size++;
        }
    }
    *rets = ress;
    return counted_items_size;
}