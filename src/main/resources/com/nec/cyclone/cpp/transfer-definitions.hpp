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
#include <limits>
#include <iostream>
#include <vector>
#include <chrono>
#include <ctime>
#include <algorithm>
#include "frovedis/text/dict.hpp"
#include "frovedis/text/words.hpp"
#include "frovedis/text/char_int_conv.hpp"
#include "frovedis/text/parsefloat.hpp"
#include "frovedis/text/parsedatetime.hpp"
#include "frovedis/text/datetime_utility.hpp"

#ifndef VE_TD_DEFS
typedef struct
{
    void **data;
    size_t count;
    size_t size;
} data_out;

typedef struct
{
    char *data;
    int32_t *offsets;
    int32_t count;
} varchar_vector;

typedef struct
{
    int32_t *data;
    uint64_t *validityBuffer;
    int32_t count;
} nullable_int_vector;

typedef struct
{
    double *data;
    uint64_t *validityBuffer;
    int32_t count;
} nullable_double_vector;

typedef struct
{
    int64_t *data;
    uint64_t *validityBuffer;
    int32_t count;
} nullable_bigint_vector;


typedef struct
{
    char *data;
    int32_t *offsets;
    int32_t dataSize;
    int32_t count;
} non_null_varchar_vector;

typedef struct
{
    char *data;
    int32_t *offsets;
    uint64_t *validityBuffer;
    int32_t dataSize;
    int32_t count;
} nullable_varchar_vector;

typedef struct
{
    char *data;
    int32_t length;
} non_null_c_bounded_string;

#define VE_TD_DEFS 1
#endif

