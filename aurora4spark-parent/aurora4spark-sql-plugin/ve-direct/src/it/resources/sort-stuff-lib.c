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

long get_veo_data(long* ve_data_position, int* items) {
    int* x = malloc(4 * 2);
    x[0] = 910;
    x[1] = 12345;
    *ve_data_position = x;
    *items = 2;
    return 56;
}

long count_strings(void* strings, int* string_positions, int* string_lengths, int num_strings, void** rets) {
    unique_position_counter* ress = malloc(num_strings * sizeof(unique_position_counter));
    long counted_items_size = 0;

    for ( int i = 0; i < num_strings; i++ ) {
        int found = 0;
        int i_string_position = string_positions[i];
        void *string_position = ((char *)strings) + i_string_position;
        char* str_i = (char*)(string_position);
        int i_length = string_lengths[i];

        for ( int j = 0; j < counted_items_size; j++ ) {
            /** VE does not like this loop. C is Ok with it.
             * 
             * It crashes here. If we remove this inner loop, the results are fine,
             * which means the interfacing is done Ok.
             * 
/home/william/aurora4spark/aurora4spark-parent/aurora4spark-sql-plugin/ve-direct/target/ve/1621380535167/wc.c", line 14: warning: a value of type
          "int *" cannot be assigned to an entity of type "long"
      *ve_data_position = x;
                        ^

ncc: vec( 103): /home/william/aurora4spark/aurora4spark-parent/aurora4spark-sql-plugin/ve-direct/target/ve/1621380535167/wc.c, line 30: Unvectorized loop.
ncc: vec( 110): /home/william/aurora4spark/aurora4spark-parent/aurora4spark-sql-plugin/ve-direct/target/ve/1621380535167/wc.c, line 30: Vectorization obstructive function reference.: strncmp
Warning: Could not load Loader: java.lang.UnsatisfiedLinkError: no jnijavacpp in java.library.path
[VE] ERROR: signalHandler() Interrupt signal 11 received
[VH] [TID 7344] ERROR: unpack_call_result() VE exception 11 <----
[VH] [TID 7344] ERROR: _progress_nolock() Internal error on executing a command(-4)
Call result = 1 <----
             * 
             *  **/
            int j_string_position = string_positions[ress[j].string_i];
            char* str_right = (char*)(((char*)strings) + j_string_position);

            if ( strncmp(str_i, str_right, string_lengths[i]) == 0 ) {
                ress[j].count++;
                found = 1;
            }
        }

        if ( found == 0 ) {
            ress[counted_items_size].string_i = i;
            ress[counted_items_size].count = 1;
            printf("WHUT %ld\n", counted_items_size);
            counted_items_size++;
            printf("WHUT %ld\n", counted_items_size);
        }
        printf("WHUT  next %ld\n", counted_items_size);
    }
    *rets = ress;
    printf("WHUT ended... %ld\n", counted_items_size);
    return counted_items_size;
}
