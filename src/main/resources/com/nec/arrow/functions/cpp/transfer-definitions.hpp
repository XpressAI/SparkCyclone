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
#include "words.hpp"
#include "words.cc"
#include "char_int_conv.hpp"
#include "char_int_conv.cc"
#include "parsefloat.hpp"
#include "parsefloat.cc"


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

static std::string utcnanotime() {
    auto now = std::chrono::system_clock::now();
    auto seconds = std::chrono::system_clock::to_time_t(now);
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count() % 1000000000;
    char utc[32];
    strftime(utc, 32, "%FT%T", gmtime(&seconds));
    snprintf(strchr(utc, 0), 32 - strlen(utc), ".%09ldZ", ns);
    return utc;
}

inline void log(std::string msg) {
    std::cout << utcnanotime().c_str() << " " << msg.c_str() << std::endl;
}

inline void set_validity(uint64_t *validityBuffer, int32_t idx, int32_t validity) {
    int32_t byte = idx / 64;
    int32_t bitIndex = idx % 64;

    if (validity) {
        validityBuffer[byte] |= (1UL << bitIndex);
    } else {
        validityBuffer[byte] &= ~(1UL << bitIndex);
    }
}

inline uint64_t check_valid(uint64_t *validityBuffer, int32_t idx) {
    uint64_t byte = idx / 64;
    uint64_t bitIndex = idx % 64;
    uint64_t res = (validityBuffer[byte] >> bitIndex) & 1;

    return res;
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
        std::cout << utcnanotime().c_str() << " $$ " << "words_to_varchar_vector out->count " << out->count << std::endl << std::flush;
    #endif
    out->dataSize = in.chars.size();
    #ifdef DEBUG
        std::cout << utcnanotime().c_str() << " $$ " << "words_to_varchar_vector out->dataSize " << out->dataSize << std::endl << std::flush;
    #endif

    out->offsets = (int32_t *)malloc((in.starts.size() + 1) * sizeof(int32_t));
    if (!out->offsets) {
        std::cout << "Failed to malloc " << in.starts.size() + 1 << " * sizeof(int32_t)." << std::endl;
        return;
    }
    for (int i = 0; i < in.starts.size(); i++) {
        out->offsets[i] = in.starts[i];
    }

    #ifdef DEBUG
        std::cout << utcnanotime().c_str() << " $$ " << "here  " << out->dataSize << std::endl << std::flush;
        std::cout << utcnanotime().c_str() << " $$ " << "sze1  " << in.starts.size() << std::endl << std::flush;
        std::cout << utcnanotime().c_str() << " $$ " << "sze2  " << in.lens.size() << std::endl << std::flush;
    #endif

    int lastOffset;
    if (in.starts.size() == 0) {
        lastOffset = 0;
    } else {
        int lastEl = in.starts.size() - 1;
        lastOffset = in.starts[lastEl] + in.lens[lastEl];
    }

    #ifdef DEBUG
        std::cout << utcnanotime().c_str() << " $$ " << "here 2 " << out->dataSize << std::endl << std::flush;
    #endif

    out->offsets[in.starts.size()] = lastOffset;
    #ifdef DEBUG
        std::cout << utcnanotime().c_str() << " $$ " << "words_to_varchar_vector out->offsets[0] " << out->offsets[0] << std::endl << std::flush;
    #endif

    out->data = (char *)malloc(out->dataSize * sizeof(char));
    if (out->data == NULL) {
        std::cout << "Failed to malloc " << out->dataSize << " * sizeof(char)." << std::endl;
        return;
    }
    frovedis::int_to_char(in.chars.data(), in.chars.size(), out->data);
    #ifdef DEBUG
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

/* used in dfcolumn.hpp */
template <class K>
inline
size_t hash_func(const K& key, size_t size) {
  unsigned long ukey = reinterpret_cast<const unsigned long&>(key);
  return ukey % size;
}

// 32bit values are treated separately
template <>
inline
size_t hash_func(const int& key, size_t size) {
  unsigned int ukey = reinterpret_cast<const unsigned int&>(key);
  return ukey % size;
}

template <>
inline
size_t hash_func(const unsigned int& key, size_t size) {
  return key % size;
}

template <>
inline
size_t hash_func(const float& key, size_t size) {
  unsigned int ukey = reinterpret_cast<const unsigned int&>(key);
  return ukey % size;
}

template<class T>
void radix_partition2(const std::vector<T>& keys, std::vector<std::vector<T>>& partitions) {
    int num_partitions = 1 << 5;

    std::vector<size_t> hashes(keys.size());
    for (int i = 0; i < keys.size(); i++) {
        hashes[i] = hash_func(keys[i], num_partitions);
    }

    std::cout << "a" << std::endl;

    std::vector<size_t> counts(num_partitions);

    for (int i = 0; i < hashes.size(); i++) {
        size_t bucket = hashes[i] && 0xFF;
        counts[bucket]++;
    }

    std::cout << "b" << std::endl;

    partitions.resize(num_partitions);
    for (int i = 0; i < num_partitions; i++) {
        partitions[i].resize(counts[i]);
    }

    std::cout << "c" << std::endl;

    // This is the slow part.
    std::vector<size_t> last_pos(num_partitions);
    for (int i = 0; i < hashes.size(); i++) {
        size_t bucket = hashes[i] && 0xFF;
        std::vector<double> partition = partitions[bucket];
        partition[last_pos[bucket]] = keys[i];
        last_pos[bucket]++;
    }

    auto it = remove_if(partitions.begin(), partitions.end(), [](auto partition) {
        return partition.size() == 0;
    });

    partitions.erase(it, partitions.end());

    std::cout << "d:" << partitions.size() << std::endl;
}

template<class T>
void radix_partition(const T* keys, const size_t keys_size, std::vector<std::vector<T>>& partitions) {
    std::vector<T> keys_vector;
    keys_vector.insert(keys_vector.end(), &keys[0], &keys[keys_size]);
    radix_partition2(keys_vector, partitions);
}


/*
#if defined(_SX) || defined(__ve__) // might be used in x86
#define RADIX_SORT_VLEN 1024
#define RADIX_SORT_VLEN_EACH 256
#else
#define RADIX_SORT_VLEN 4
#define RADIX_SORT_VLEN_EACH 1
#define SWITCH_INSERTION_THR 64
#endif

#define RADIX_SORT_ALIGN_SIZE 128
#define CONTAIN_NEGATIVE_SIZE 65536

// supported K is int type, and only 0 or positive data
void radix_partition(const uint64_t* key_array, size_t size, size_t max_key_size, std::vector<uint64_t>& bucket_table) {
  int bucket_ldim = RADIX_SORT_VLEN + RADIX_SORT_ALIGN_SIZE/sizeof(size_t);
  int num_bucket = 1 << 8; // 8bit == 256
  // bucket_table is columnar (VLEN + 16) by num_bucket matrix
  // "16" is to avoid bank conflict/alignment, but reused for "rest" data
  std::vector<size_t> px_bucket_table(num_bucket * bucket_ldim);
  size_t* bucket_tablep = &bucket_table[0];
  size_t* px_bucket_tablep = &px_bucket_table[0];
  std::vector<size_t> pos(size);
  size_t* posp = &pos[0];
  size_t block_size = size / RADIX_SORT_VLEN;

  auto aligned_block_size = block_size * sizeof(K) / RADIX_SORT_ALIGN_SIZE;
  if(aligned_block_size % 2 == 0 && aligned_block_size != 0)
    aligned_block_size -= 1;
  block_size = aligned_block_size * RADIX_SORT_ALIGN_SIZE / sizeof(K);

  size_t rest = size - RADIX_SORT_VLEN * block_size;

  for(size_t d = 1; d <= max_key_size; d++) { // d: digit
    size_t to_shift = (d - 1) * 8;
    auto bucket_table_size = bucket_table.size();
    for(size_t i = 0; i < bucket_table_size; i++) bucket_tablep[i] = 0;

    for(size_t b = 0; b < block_size; b++) {
      #pragma cdir nodep
      #pragma _NEC ivdep
      // vector loop, loop raking
      for(int v = 0; v < RADIX_SORT_VLEN_EACH; v++) {
        auto v0 = v;
        auto v1 = v + RADIX_SORT_VLEN_EACH;
        auto v2 = v + RADIX_SORT_VLEN_EACH * 2;
        auto v3 = v + RADIX_SORT_VLEN_EACH * 3;
        auto key0 = key_src[block_size * v0 + b];
        auto key1 = key_src[block_size * v1 + b];
        auto key2 = key_src[block_size * v2 + b];
        auto key3 = key_src[block_size * v3 + b];
        int bucket0 = (key0 >> to_shift) & 0xFF;
        int bucket1 = (key1 >> to_shift) & 0xFF;
        int bucket2 = (key2 >> to_shift) & 0xFF;
        int bucket3 = (key3 >> to_shift) & 0xFF;
        auto bucket_table_tmp0 = bucket_tablep[bucket_ldim * bucket0 + v0];
        auto bucket_table_tmp1 = bucket_tablep[bucket_ldim * bucket1 + v1];
        auto bucket_table_tmp2 = bucket_tablep[bucket_ldim * bucket2 + v2];
        auto bucket_table_tmp3 = bucket_tablep[bucket_ldim * bucket3 + v3];
        bucket_tablep[bucket_ldim * bucket0 + v0] = bucket_table_tmp0 + 1;
        bucket_tablep[bucket_ldim * bucket1 + v1] = bucket_table_tmp1 + 1;
        bucket_tablep[bucket_ldim * bucket2 + v2] = bucket_table_tmp2 + 1;
        bucket_tablep[bucket_ldim * bucket3 + v3] = bucket_table_tmp3 + 1;
        posp[block_size * v0 + b] = bucket_table_tmp0;
        posp[block_size * v1 + b] = bucket_table_tmp1;
        posp[block_size * v2 + b] = bucket_table_tmp2;
        posp[block_size * v3 + b] = bucket_table_tmp3;
      }
    }
    int v = RADIX_SORT_VLEN;
    for(int b = 0; b < rest; b++) { // not vector loop
      auto key = key_src[block_size * v + b];
      int bucket = (key >> to_shift) & 0xFF;
      posp[block_size * v + b] = bucket_tablep[bucket_ldim * bucket + v];
      bucket_tablep[bucket_ldim * bucket + v]++;
    }
    // preparing for the copy
    prefix_sum(bucket_tablep, px_bucket_tablep + 1,
               num_bucket * bucket_ldim - 1);
    // now copy the data to the bucket
    if(block_size > 7) {
      #pragma _NEC vob
      for(int unroll = 0; unroll < 4; unroll++) {
        for(size_t b = 0; b < block_size-7; b+=8) { // b: block
          #pragma cdir nodep
          #pragma _NEC ivdep
          #pragma _NEC vovertake
          // vector loop, loop raking
          for(int v = 0; v < RADIX_SORT_VLEN_EACH; v++) {
            auto vv = v + RADIX_SORT_VLEN_EACH * unroll;
            auto key0 = key_src[block_size * vv + b];
            auto key1 = key_src[block_size * vv + b+1];
            auto key2 = key_src[block_size * vv + b+2];
            auto key3 = key_src[block_size * vv + b+3];
            auto key4 = key_src[block_size * vv + b+4];
            auto key5 = key_src[block_size * vv + b+5];
            auto key6 = key_src[block_size * vv + b+6];
            auto key7 = key_src[block_size * vv + b+7];
            int bucket0 = (key0 >> to_shift) & 0xFF;
            int bucket1 = (key1 >> to_shift) & 0xFF;
            int bucket2 = (key2 >> to_shift) & 0xFF;
            int bucket3 = (key3 >> to_shift) & 0xFF;
            int bucket4 = (key4 >> to_shift) & 0xFF;
            int bucket5 = (key5 >> to_shift) & 0xFF;
            int bucket6 = (key6 >> to_shift) & 0xFF;
            int bucket7 = (key7 >> to_shift) & 0xFF;
            auto px_bucket0 = px_bucket_tablep[bucket_ldim * bucket0 + vv];
            auto px_bucket1 = px_bucket_tablep[bucket_ldim * bucket1 + vv];
            auto px_bucket2 = px_bucket_tablep[bucket_ldim * bucket2 + vv];
            auto px_bucket3 = px_bucket_tablep[bucket_ldim * bucket3 + vv];
            auto px_bucket4 = px_bucket_tablep[bucket_ldim * bucket4 + vv];
            auto px_bucket5 = px_bucket_tablep[bucket_ldim * bucket5 + vv];
            auto px_bucket6 = px_bucket_tablep[bucket_ldim * bucket6 + vv];
            auto px_bucket7 = px_bucket_tablep[bucket_ldim * bucket7 + vv];
            auto posp0 = posp[block_size * vv + b];
            auto posp1 = posp[block_size * vv + b+1];
            auto posp2 = posp[block_size * vv + b+2];
            auto posp3 = posp[block_size * vv + b+3];
            auto posp4 = posp[block_size * vv + b+4];
            auto posp5 = posp[block_size * vv + b+5];
            auto posp6 = posp[block_size * vv + b+6];
            auto posp7 = posp[block_size * vv + b+7];
            auto to0 = px_bucket0 + posp0;
            auto to1 = px_bucket1 + posp1;
            auto to2 = px_bucket2 + posp2;
            auto to3 = px_bucket3 + posp3;
            auto to4 = px_bucket4 + posp4;
            auto to5 = px_bucket5 + posp5;
            auto to6 = px_bucket6 + posp6;
            auto to7 = px_bucket7 + posp7;
            key_dst[to0] = key0;
            key_dst[to1] = key1;
            key_dst[to2] = key2;
            key_dst[to3] = key3;
            key_dst[to4] = key4;
            key_dst[to5] = key5;
            key_dst[to6] = key6;
            key_dst[to7] = key7;
          }
        }
      }
    }
    #pragma _NEC vob
    for(int unroll = 0; unroll < 4; unroll++) {
      for(size_t b = block_size - (block_size % 8); b < block_size; b++) {
        #pragma cdir nodep
        #pragma _NEC ivdep
        #pragma _NEC vovertake
        // vector loop, loop raking
        for(int v = 0; v < RADIX_SORT_VLEN_EACH; v++) {
          auto vv = v + RADIX_SORT_VLEN_EACH * unroll;
          auto key = key_src[block_size * vv + b];
          int bucket = (key >> to_shift) & 0xFF;
          auto px_bucket = px_bucket_tablep[bucket_ldim * bucket + vv];
          auto posp0 = posp[block_size * vv + b];
          auto to = px_bucket + posp0;
          key_dst[to] = key;
        }
      }
    }
    v = RADIX_SORT_VLEN;
    #pragma cdir nodep
    #pragma _NEC ivdep
    #pragma _NEC vovertake
    #pragma _NEC vob
    for(size_t b = 0; b < rest; b++) {
      auto key = key_src[block_size * v + b];
      int bucket = (key >> to_shift) & 0xFF;
      size_t to = px_bucket_tablep[bucket_ldim * bucket + v] +
        posp[block_size * v + b];
      key_dst[to] = key;
    }

    next_is_tmp = 1 - next_is_tmp;
  }
}
*/
#define VE_TD_DEFS 1
#endif

