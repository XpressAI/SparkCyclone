/*
 * Copyright (c) 2022 Xpress AI.
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
#include "frovedis/core/utility.hpp"
#include "frovedis/text/dict.hpp"
#include "frovedis/text/words.hpp"
#include "frovedis/text/char_int_conv.hpp"
#include "frovedis/text/parsefloat.hpp"
#include "frovedis/text/parsedatetime.hpp"
#include "frovedis/text/datetime_utility.hpp"
#include "cyclone/cyclone.hpp"
#include "cyclone/transfer-definitions.hpp"

std::string utcnanotime() {
    auto now = std::chrono::system_clock::now();
    auto seconds = std::chrono::system_clock::to_time_t(now);
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count() % 1000000000;
    char utc[32];
    strftime(utc, 32, "%FT%T", gmtime(&seconds));
    snprintf(strchr(utc, 0), 32 - strlen(utc), ".%09lldZ", ns);
    return utc;
}

frovedis::words data_offsets_to_words(
    const int32_t *data,
    const int32_t *offsets,
    /** size of all the data **/
    const int32_t size,
    const int32_t *lengths,
    /** count of the words **/
    const int32_t count
    ) {
    frovedis::words ret;
    if (count == 0) {
        return ret;
    }

        //TODO: TempFix
        std::cout << "Count: '" << count;
//        std::cout << "data: '";
//        for (int i = 0; i < size; i++) {
//            std::cout << (char)data[i];
//        }
        std::cout << "'" << std::endl;

        std::cout << "offsets: ";
        for (int i = 0; i < count; i++) {
            std::cout << offsets[i] << ", ";
        }

        std::cout << "lengths: ";
        for (int i = 0; i < count; i++) {
            std::cout << lengths[i] << ", ";
        }
        std::cout << std::endl;

    ret.lens.resize(count);
    for (int i = 0; i < count; i++) {
        ret.lens[i] = lengths[i];
    }

    ret.starts.resize(count);
    for (int i = 0; i < count; i++) {
        ret.starts[i] = offsets[i];
    }

    #ifdef DEBUG
        std::cout << "size: " << size << std::endl;
        std::cout << "last offset: " << offsets[count] << std::endl;
    #endif

    ret.chars.assign(data, data + size);
    return ret;
}

frovedis::words varchar_vector_to_words(const nullable_varchar_vector *v) {
    return data_offsets_to_words(v->data, v->offsets, v->dataSize, v->lengths, v->count);
}

void words_to_varchar_vector(frovedis::words& in, nullable_varchar_vector *out) {
    #ifdef DEBUG
        std::cout << utcnanotime().c_str() << " $$ " << "words_to_varchar_vector" << std::endl << std::flush;
    #endif

    out->count = in.lens.size();

    #ifdef DEBUG
    std::cout << "out->count = " << out->count << std::endl;
    #endif

    out->dataSize = in.chars.size();

    #ifdef DEBUG
    std::cout << "out->dataSize = " << out->dataSize << std::endl;
    #endif

    out->data = (int32_t *)malloc(in.chars.size() * sizeof(int32_t));
    if (out->data == NULL) {
        std::cout << "Failed to malloc " << out->dataSize << " * sizeof(int32_t)." << std::endl;
        return;
    }

    std::copy(in.chars.begin(), in.chars.end(), out->data);

    out->offsets = (int32_t *)malloc((in.starts.size()) * sizeof(int32_t));
    for (int i = 0; i < in.starts.size(); i++) {
        out->offsets[i] = in.starts[i];
    }

    out->lengths = (int32_t *)malloc(in.lens.size() * sizeof(int32_t));
    for (int i = 0; i < in.lens.size(); i++) {
        out->lengths[i] = in.lens[i];
    }

    #ifdef DEBUG
        std::cout << "data: '";
        for (int i = 0; i < out->dataSize; i++) {
            std::cout << (char)out->data[i];
        }
        std::cout << "'" << std::endl;

        std::cout << "offsets: ";
        for (int i = 0; i < out->count; i++) {
            std::cout << out->offsets[i] << ", ";
        }
        std::cout << "lengths: ";
        for (int i = 0; i < out->count; i++) {
            std::cout << out->lengths[i] << ", ";
        }
        std::cout << std::endl;
    #endif

    size_t validity_count = frovedis::ceil_div(out->count, int32_t(64));
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
