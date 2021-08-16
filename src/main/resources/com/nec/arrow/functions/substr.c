#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <string>
#include "cpp/frovedis/text/words.cc"

extern "C" long ve_substr(non_null_varchar_vector* input_strings, non_null_varchar_vector* output_strings) {
    int length = input_strings->offsets[input_strings->count];
    std::cout << input_strings->count << std::endl << std::flush;
    output_strings->size = input_strings->size;
    output_strings->count = input_strings->count;
    output_strings->data = (char*) malloc(input_strings->size);
    output_strings->offsets = (int*) malloc(4 * (input_strings->count + 1));
    memcpy(output_strings->data, input_strings->data, length);

    /**substr(size_t* starts, size_t* lens, size_t num_words, size_t pos)
    void substr(std::vector<size_t>& starts,
                std::vector<size_t>& lens,
                size_t pos) {
**/
    memcpy(output_strings->offsets, input_strings->offsets, 4 * (input_strings->count + 1));

    return 0;
}
