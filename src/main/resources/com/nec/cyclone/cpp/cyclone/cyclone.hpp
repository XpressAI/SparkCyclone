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
#pragma once

#include <vector>
#include "frovedis/text/dict.hpp"
#include "frovedis/text/words.hpp"
#include "cyclone/transfer-definitions.hpp"

std::string utcnanotime();

void debug_words(frovedis::words &in);

std::vector<size_t> idx_to_std(nullable_int_vector *idx);

void print_indices(std::vector<size_t> vec);

frovedis::words filter_words(frovedis::words &in_words, std::vector<size_t> to_select);

std::vector<size_t> filter_words_dict(frovedis::words &input_words, frovedis::words &filtering_set);
