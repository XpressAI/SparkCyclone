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
#pragma once

#include <stddef.h>
#include <stdint.h>

int handle_transfer(char** td, uintptr_t* od);

void merge_varchar_transfer(size_t batch_count, char* col_header, char* input_data, char* data, uint64_t* validity_buffer, char* lengths, char* offsets, uintptr_t* od, size_t &output_pos);
template<typename T> void merge_scalar_transfer(size_t batch_count, char* col_header, char* input_data, char* data, uint64_t* validity_buffer, uintptr_t* od, size_t &output_pos);

const size_t COL_TYPE_SHORT = 0;
const size_t COL_TYPE_INT = 1;
const size_t COL_TYPE_BIGINT = 2;
const size_t COL_TYPE_FLOAT = 3;
const size_t COL_TYPE_DOUBLE = 4;
const size_t COL_TYPE_VARCHAR = 5;


struct transfer_header {
  size_t header_size;
  size_t batch_count;
  size_t column_count;
};

struct scalar_col_in {
  size_t element_count;
  size_t data_size;
  size_t validity_buffer_size;
};

struct varchar_col_in {
  size_t element_count;
  size_t data_size;
  size_t offsets_size;
  size_t lengths_size;
  size_t validity_buffer_size;
};
