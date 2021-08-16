#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <string>
#include "cpp/frovedis/text/words.hpp"

extern "C" long ve_substr(non_null_varchar_vector* input_strings, non_null_varchar_vector* output_strings, int beginIndex, int endIndex) {
    int length = input_strings->offsets[input_strings->count];
    std::string output_result("");
    std::string input_str(input_strings->data, input_strings->size);
    std::vector<int> output_offsets;
    int currentOffset = 0;
    for ( int i = 0; i < input_strings->count; i++ ) {
        for ( int j = beginIndex; j < endIndex; j++ ) {
            output_result.append((input_strings->data + (input_strings->offsets[i] + j)), 1);
        }
        output_offsets.push_back(currentOffset);
        currentOffset += (endIndex - beginIndex);
    }
    output_offsets.push_back(currentOffset);

    output_strings->count = input_strings->count;
    output_strings->size = currentOffset;
    output_strings->data = (char*)malloc(output_strings->size);
    memcpy(output_strings->data, output_result.data(), output_strings->size);
    output_strings->offsets = (int*)malloc(4 * (output_strings->count + 1));
    memcpy(output_strings->offsets, output_offsets.data(), 4 * (output_strings->count + 1));
    return 0;
}
