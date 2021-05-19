#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <winsock2.h>

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

typedef struct {
    void **data;
    size_t logical_total;
    size_t bytes_total;
} data_out;

int count_strings(char* strings, int* string_positions, int* string_lengths, int num_strings, data_out* counted_strings) {
    unique_position_counter* ress = malloc(num_strings * sizeof(unique_position_counter));
    long counted_items_size = 0;
    for ( int i = 0; i < num_strings; i++ ) {
        int found = 0;
        int string_i_position = string_positions[i];
        char* str_i = (char *)(strings + string_i_position);
        int i_length = string_lengths[i];

        for ( int j = 0; j < counted_items_size; j++ ) {
            int j_string_position = string_positions[ress[j].string_i];
            char* str_right = (char*)(strings + j_string_position);
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
    printf("\nx->%p\ny->%p\nz->%p\n", &(counted_strings->data), &(counted_strings->logical_total), &(counted_strings->bytes_total));
    counted_strings->data = ress;
    counted_strings->logical_total = counted_items_size;
    counted_strings->bytes_total = counted_items_size * sizeof(unique_position_counter);
    return 0;
}
