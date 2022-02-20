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
