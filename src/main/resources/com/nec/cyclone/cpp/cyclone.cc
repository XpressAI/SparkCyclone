/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <string>
#include <limits>
#include <iostream>
#include <vector>
#include <chrono>
#include <ctime>
#include <algorithm>
#include "cyclone.hpp"
#include "transfer-definitions.hpp"
#include "frovedis/text/words.hpp"
#include "frovedis/text/char_int_conv.hpp"
#include "frovedis/text/parsefloat.hpp"
#include "frovedis/text/parsedatetime.hpp"
#include "frovedis/text/datetime_utility.hpp"

std::string utcnanotime() {
    auto now = std::chrono::system_clock::now();
    auto seconds = std::chrono::system_clock::to_time_t(now);
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count() % 1000000000;
    char utc[32];
    strftime(utc, 32, "%FT%T", gmtime(&seconds));
    snprintf(strchr(utc, 0), 32 - strlen(utc), ".%09ldZ", ns);
    return utc;
}

frovedis::words data_offsets_to_words(
    const char *data,
    const int32_t *offsets,
    /** size of all the data **/
    const int32_t size,
    /** count of the words **/
    const int32_t count
    ) {
    frovedis::words ret;
    if (count == 0) {
        return ret;
    }

    #ifdef DEBUG
        std::cout << "count: " << count << std::endl;
    #endif

    ret.lens.resize(count);
    for (int i = 0; i < count; i++) {
        ret.lens[i] = offsets[i + 1] - offsets[i];
    }

    ret.starts.resize(count);
    for (int i = 0; i < count; i++) {
        ret.starts[i] = offsets[i];
    }

    #ifdef DEBUG
        std::cout << "size: " << size << std::endl;
        std::cout << "last offset: " << offsets[count] << std::endl;
    #endif

    ret.chars.resize(offsets[count]);
    frovedis::char_to_int(data, offsets[count], ret.chars.data());

    return ret;
}

frovedis::words varchar_vector_to_words(const non_null_varchar_vector *v) {
    return data_offsets_to_words(v->data, v->offsets, v->dataSize, v->count);
}

frovedis::words varchar_vector_to_words(const nullable_varchar_vector *v) {
    return data_offsets_to_words(v->data, v->offsets, v->dataSize, v->count);
}

void words_to_varchar_vector(frovedis::words& in, nullable_varchar_vector *out) {
    #ifdef DEBUG
        std::cout << utcnanotime().c_str() << " $$ " << "words_to_varchar_vector" << std::endl << std::flush;
    #endif

    out->count = in.lens.size();

    #ifdef DEBUG
    std::cout << "out->count = " << out->count << std::endl;
    #endif

    int32_t totalChars = 0;
    for (size_t i = 0; i < in.lens.size(); i++) {
        totalChars += in.lens[i];
    }
    out->dataSize = totalChars;

    #ifdef DEBUG
    std::cout << "out->dataSize = " << out->dataSize << std::endl;
    #endif

    out->data = (char *)malloc(totalChars * sizeof(char));
    if (out->data == NULL) {
        std::cout << "Failed to malloc " << out->dataSize << " * sizeof(char)." << std::endl;
        return;
    }
    std::vector<size_t> lastChars(in.lens.size() + 1);
    size_t sum = 0;
    for (int i = 0; i < in.lens.size(); i++) {
        lastChars[i] = sum;
        sum += in.lens[i];
    }

    for (int i = 0; i < out->count; i++) {
        size_t lastChar = lastChars[i];
        size_t wordStart = in.starts[i];
        size_t wordEnd = wordStart + in.lens[i];
        for (int j = wordStart; j < wordEnd; j++) {
            out->data[lastChar++] = (char)in.chars[j];
        }
    }

    out->offsets = (int32_t *)malloc((in.starts.size() + 1) * sizeof(int32_t));
    out->offsets[0] = 0;
    for (int i = 1; i < in.starts.size() + 1; i++) {
        out->offsets[i] = lastChars[i];
    }
    out->offsets[in.starts.size()] = totalChars;

    #ifdef DEBUG
        std::cout << "data: '";
        for (int i = 0; i < totalChars; i++) {
            std::cout << out->data[i];
        }
        std::cout << "'" << std::endl;

        std::cout << "offsets: ";
        for (int i = 0; i < out->count + 1; i++) {
            std::cout << out->offsets[i] << ", ";
        }
        std::cout << std::endl;
    #endif

    size_t validity_count = ceil(out->count / 64.0);
    out->validityBuffer = (uint64_t *)malloc(validity_count * sizeof(uint64_t));
    if (!out->validityBuffer) {
        std::cout << "Failed to malloc " << validity_count << " * sizeof(uint64_t)" << std::endl;
        return;
    }
    for (int i = 0; i < validity_count; i++) {
        out->validityBuffer[i] = 0xffffffffffffffff;
    }
}


void debug_words(frovedis::words &in) {
    std::cout << "words char count: " << in.chars.size() << std::endl;
    std::cout << "words starts count: " << in.starts.size() << std::endl;
    std::cout << "words lens count: " << in.lens.size() << std::endl;
    std::cout << "First word starts at: " << in.starts[0] << " length: " << in.lens[0] << " '";

    size_t start = in.starts[0];
    for (int i = 0; i < std::min((long)in.lens[0], 64L); i++) {
        std::cout << (char)in.chars[start + i];
    }
    std::cout << "'" << std::endl;

    std::cout << "Last word " << in.starts.size() - 1 << " starts at: " << in.starts[in.starts.size() -1] << " length[" << in.lens.size() - 1 << "]: " << in.lens[in.lens.size() - 1] << " '";
    start = in.starts[in.starts.size() - 1];
    for (int i = 0; i < std::min((long)in.lens[in.lens.size() - 1], 64L); i++) {
        std::cout << (char)in.chars[start + i];
    }
    std::cout << "'" << std::endl;
}

std::vector<size_t> idx_to_std(nullable_int_vector *idx) {
    std::vector<size_t> ret;
    for ( int i = 0; i < idx->count; i++ ) {
        ret.push_back(idx->data[i]);
    }
    return ret;
}

void print_indices(std::vector<size_t> vec) {
    std::cout << "vec:" << std::endl << std::flush;
    for ( int i = 0; i < vec.size(); i++ ) {
        std::cout << "["<< i << "] = " << vec[i] << std::endl << std::flush;
    }
    std::cout << "/vec" << std::endl << std::flush;
}

frovedis::words filter_words(frovedis::words &in_words, std::vector<size_t> to_select) {
    frovedis::words nw;
    std::vector<size_t> new_starts(to_select.size());
    std::vector<size_t> new_lens(to_select.size());
    for (size_t i = 0; i < to_select.size(); i++) {
        new_starts[i] = in_words.starts[to_select[i]];
        new_lens[i] = in_words.lens[to_select[i]];
    }

    nw.chars.swap(in_words.chars);
    nw.starts.swap(new_starts);
    nw.lens.swap(new_lens);
    const frovedis::words fww = nw;
    auto cw = make_compressed_words(fww);
    auto dct = make_dict_from_words(fww);
    auto new_indices = dct.lookup(cw);
    return dct.index_to_words(new_indices);
}

std::vector<size_t> filter_words_dict(frovedis::words &input_words, frovedis::words &filtering_set) {
    auto compressed_words = make_compressed_words(input_words);
    auto dct = make_dict_from_words(filtering_set);
    auto new_indices = dct.lookup(compressed_words);

    return new_indices;
}

void debug_nullable_varchar_vector(const nullable_varchar_vector *v) {
    std::cout << "[ ";
    for (auto i = 0; i < v->count; i++) {
        if (check_valid(v->validityBuffer, i)) {
            std::cout << std::string(v->data + v->offsets[i], v->offsets[i+1] - v->offsets[i]) << ", ";
        }
    }
    std::cout << "]\n";
}