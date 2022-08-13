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

/*
  This is the single main header to be included by code that uses the Cyclone
  C++ Library.
*/

#include "cyclone/transfer-definitions.hpp"
#include "cyclone/cyclone_grouping.hpp"
#include "cyclone/cyclone_sort.hpp"
#include "cyclone/util/func.hpp"
#include "cyclone/util/io.hpp"
#include "cyclone/util/log.hpp"
#include "cyclone/util/time.hpp"
#include "cyclone/cyclone_utils.hpp"
#include "cyclone/packed_transfer.hpp"
#include "cyclone/tuple_hash.hpp"
#include "frovedis/text/dict.hpp"
#include "frovedis/text/words.hpp"

/*
#ifdef __ve__

extern "C" int attach_vh_shm(char *path, int32_t id, size_t size_mb, void **out_p, uint64_t *out_data_vehva);
extern "C" int dettach_vh_shm(void *p, uint64_t data_vehva);

#endif
*/
