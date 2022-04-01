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

/*
#ifdef __ve__

 static uint64_t ve_register_mem_to_dmaatb_unaligned(void *vemva, size_t size) {
     uint64_t align = sysconf(_SC_PAGESIZE);
     uint64_t addr = (uint64_t)vemva;
     uint64_t offset = addr & (align - 1);
     void* addr_aligned = (void *)(addr & ~(align - 1));
     uint64_t size_aligned = (offset + size + align - 1) & ~(align - 1);
     uint64_t data_vehva = ve_register_mem_to_dmaatb(addr_aligned, size_aligned);
     if (data_vehva == (uint64_t)-1)
         return (uint64_t)-1;
     return data_vehva + offset;
 }

 static int ve_unregister_mem_from_dmaatb_unaligned(uint64_t vehva) {
     uint64_t align = sysconf(_SC_PAGESIZE);
     uint64_t vehva_aligned = vehva & ~(align - 1);
     return ve_unregister_mem_from_dmaatb(vehva_aligned);
 }

 extern "C" int attach_vh_shm(char *path, int32_t id, size_t size_mb, void **out_p, uint64_t *out_data_vehva) {
     int32_t key = ftok(path, id);
     size_t shm_size = size_mb * 1024 * 1024; // A multiple of 2M
     size_t data_size = 4; // A multiple of 4

     // Attach shm on VH to VE
     int shmid = vh_shmget(key, shm_size, SHM_HUGETLB);
     if (shmid == -1) {
         perror("vh_shmget");
         return 1;
     }

     uint64_t *shm_vehva = NULL;
     void *p = vh_shmat(shmid, NULL, 0, (void **)&shm_vehva);
     if (p == (void*)-1) {
         perror("vh_shmat");
         return 1;
     }

     if (ve_dma_init() != 0) {
         perror("ve_dma_init");
         return 1;
     }

     int *data = (int *)malloc(data_size);

     uint64_t data_vehva = ve_register_mem_to_dmaatb_unaligned(data, data_size);
     if (data_vehva == (uint64_t)-1) {
         perror("ve_register_mem_to_dmaatb_unaligned");
         return 1;
     }

     *out_p = p;
     *out_data_vehva = data_vehva;

     // read
     //int ret = ve_dma_post_wait(data_vehva, shm_vehva, data_size);
     //(*data)++;

     // write
     //ret = ve_dma_post_wait(shm_vehva, data_vehva, data_size);

     return 0;
 }

 extern "C" int dettach_vh_shm(void *p, uint64_t data_vehva) {
     vh_shmdt(p);

     int ret = ve_unregister_mem_from_dmaatb_unaligned(data_vehva);
     if (ret == -1) {
         perror("ve_unregister_mem_from_dmaatb_unaligned");
         return 1;
     }

     return 0;
 }

 #endif
 */