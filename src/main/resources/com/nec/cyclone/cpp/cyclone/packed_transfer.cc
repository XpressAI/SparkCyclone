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

#include "cyclone/packed_transfer.hpp"
#include "cyclone/transfer-definitions.hpp"
#include "cyclone/cyclone_utils.hpp"

#include "frovedis/core/utility.hpp"

#include <stddef.h>
#include <stdint.h>
#include <type_traits>
#include <iostream>
#include <cstring>

void merge_varchar_transfer(size_t batch_count, char* col_header, char* input_data, char* out_data, uint64_t* out_validity_buffer, char* out_lengths, char* out_offsets, uintptr_t* od, size_t &output_pos){
  //std::cout << "merge_varchar_transfer" << std::endl;
  size_t cur_col_pos = 0;
  size_t cur_data_pos = 0;
  size_t cur_out_data_pos = 0;
  size_t cur_out_lengths_pos = 0;
  size_t cur_out_offsets_pos = 0;
  size_t dangling_bits = 0;
  size_t processed_elements = 0;

  for(auto b = 0; b < batch_count; b++){
    cur_col_pos += sizeof(column_type); // Don't care about column type anymore - it has been asserted to be correct previously
    varchar_col_in* col_in = reinterpret_cast<varchar_col_in *>(&col_header[cur_col_pos]);
    cur_col_pos += sizeof(varchar_col_in);

    //std::cout << "merge_varchar_transfer: col_in->element_count = " <<  col_in->element_count << std::endl;
    //std::cout << "merge_varchar_transfer: col_in->data_size = " <<  col_in->data_size << std::endl;
    //std::cout << "merge_varchar_transfer: col_in->offsets_size = " <<  col_in->offsets_size << std::endl;
    //std::cout << "merge_varchar_transfer: col_in->lengths_size = " <<  col_in->lengths_size << std::endl;
    //std::cout << "merge_varchar_transfer: col_in->validity_buffer_size = " <<  col_in->validity_buffer_size << std::endl;

    //std::cout << "merge_varchar_transfer: copy data" << std::endl;

    size_t start_out_data_pos = cur_out_data_pos;
    std::memcpy(&out_data[cur_out_data_pos], &input_data[cur_data_pos], col_in->data_size);
    cur_data_pos += VECTOR_ALIGNED(col_in->data_size);
    cur_out_data_pos += col_in->data_size;

    //std::cout << "merge_varchar_transfer: copy offsets" << std::endl;
    // Offsets are relative to starting positions. After initially copying them, we need to correct them too.
    // Offsets specify where a string starts relative to the data buffer. When merging inputs they need to be offset
    // in order to be relative to the starting data position of the current batch.
    std::memcpy(&out_offsets[cur_out_offsets_pos], &input_data[cur_data_pos], col_in->offsets_size);
    if(b != 0){
      //std::cout << "merge_varchar_transfer: fix offsets" << std::endl;
      // offsets in the first batch are correct, no need to update them.
      int32_t* offsets_int = reinterpret_cast<int32_t *>(&out_offsets[cur_out_offsets_pos]);
      auto offset_correction = start_out_data_pos / sizeof(int32_t);
#pragma _NEC vector
      for(auto oi = 0; oi < col_in->element_count; oi++){
        offsets_int[oi] += offset_correction;
      }
    }
    cur_data_pos += VECTOR_ALIGNED(col_in->offsets_size);
    cur_out_offsets_pos += col_in->offsets_size;

    //std::cout << "merge_varchar_transfer: copy lengths" << std::endl;
    std::memcpy(&out_lengths[cur_out_lengths_pos], &input_data[cur_data_pos], col_in->lengths_size);
    cur_data_pos += VECTOR_ALIGNED(col_in->lengths_size);
    cur_out_lengths_pos += col_in->lengths_size;

    //std::cout << "merge_varchar_transfer: copy validity buffer" << std::endl;
    size_t cur_out_validity_data_pos = processed_elements / 64;
    //std::cout << "merge_varchar_transfer: cur_out_validity_data_pos=" << cur_out_validity_data_pos << std::endl;
    //std::cout << "merge_varchar_transfer: dangling_bits=" << dangling_bits << std::endl;

    uint64_t* batch_validity_buffer = reinterpret_cast<uint64_t *>(&input_data[cur_data_pos]);
    dangling_bits = cyclone::append_bitsets(
        &out_validity_buffer[cur_out_validity_data_pos], dangling_bits,
        batch_validity_buffer, col_in->element_count);

    processed_elements += col_in->element_count;
    cur_data_pos += VECTOR_ALIGNED(col_in->validity_buffer_size);
  }

  //std::cout << "merge_varchar_transfer: allocate vector" << std::endl;
  nullable_varchar_vector* result = nullable_varchar_vector::allocate();
  result->data = reinterpret_cast<int32_t *>(out_data);
  result->offsets = reinterpret_cast<int32_t *>(out_offsets);
  result->lengths = reinterpret_cast<int32_t *>(out_lengths);
  result->validityBuffer = out_validity_buffer;
  result->dataSize = cur_out_data_pos / sizeof(int32_t);
  result->count = processed_elements;

  //std::cout << "merge_varchar_transfer: set output pointers" << std::endl;
  od[output_pos++] = reinterpret_cast<uintptr_t>(result);
  od[output_pos++] = reinterpret_cast<uintptr_t>(result->data);
  od[output_pos++] = reinterpret_cast<uintptr_t>(result->offsets);
  od[output_pos++] = reinterpret_cast<uintptr_t>(result->lengths);
  od[output_pos++] = reinterpret_cast<uintptr_t>(result->validityBuffer);
}


template<typename T>
void merge_scalar_transfer(size_t batch_count, char* col_header, char* input_data, char* out_data, uint64_t* out_validity_buffer, uintptr_t* od, size_t &output_pos){
  //std::cout << "merge_scalar_transfer" << std::endl;

  size_t cur_col_pos = 0;
  size_t cur_data_pos = 0;
  size_t cur_out_data_pos = 0;
  size_t dangling_bits = 0;

  size_t processed_elements = 0;

  for(auto b = 0; b < batch_count; b++){
    cur_col_pos += sizeof(column_type); // Don't care about column type anymore - it has been asserted to be correct previously
    scalar_col_in* col_in = reinterpret_cast<scalar_col_in *>(&col_header[cur_col_pos]);
    cur_col_pos += sizeof(scalar_col_in);

    //std::cout << "merge_scalar_transfer: copy data" << std::endl;
    std::memcpy(&out_data[cur_out_data_pos], &input_data[cur_data_pos], col_in->data_size);
    cur_data_pos += VECTOR_ALIGNED(col_in->data_size);
    cur_out_data_pos += col_in->data_size;

    //std::cout << "merge_scalar_transfer: copy validity" << std::endl;
    //std::cout << "merge_scalar_transfer: cur_data_pos=" << cur_data_pos << std::endl;

    size_t cur_out_validity_data_pos = processed_elements / 64;
    //std::cout << "merge_scalar_transfer: cur_out_validity_data_pos=" << cur_out_validity_data_pos << std::endl;
    //std::cout << "merge_scalar_transfer: dangling_bits=" << dangling_bits << std::endl;

    uint64_t* batch_validity_buffer = reinterpret_cast<uint64_t *>(&input_data[cur_data_pos]);
    dangling_bits = cyclone::append_bitsets(
        &out_validity_buffer[cur_out_validity_data_pos], dangling_bits,
        batch_validity_buffer, col_in->element_count);

    processed_elements += col_in->element_count;
    cur_data_pos += VECTOR_ALIGNED(col_in->validity_buffer_size);
  }

  //std::cout << "merge_scalar_transfer: allocate vector" << std::endl;
  NullableScalarVec<T>* result = NullableScalarVec<T>::allocate();
  result->data = reinterpret_cast<T* >(out_data);
  result->validityBuffer = out_validity_buffer;
  result->count = processed_elements;

  //std::cout << "merge_scalar_transfer: set output pointers" << std::endl;
  //std::cout << "merge_scalar_transfer: set output pointers: result; pos: " << output_pos << std::endl;
  od[output_pos++] = reinterpret_cast<uintptr_t>(result);
  //std::cout << "merge_scalar_transfer: set output pointers: data; pos: " << output_pos << std::endl;
  od[output_pos++] = reinterpret_cast<uintptr_t>(result->data);
  //std::cout << "merge_scalar_transfer: set output pointers: validity; pos: " << output_pos << std::endl;
  od[output_pos++] = reinterpret_cast<uintptr_t>(result->validityBuffer);
  //std::cout << "merge_scalar_transfer: set output pointers: done" << std::endl;
}


/**
 * Allocate and copy memory according to transfer descriptor
 *
 * Small transfers incur an overhead which limits their total throughput
 * severely.
 *
 * In order to sort out the problem, we create a "transfer" structure on the VH
 * side and transfer the entire structure at once.
 *
 * However, the assumption that everything is allocated independently sits deep
 * within the existing code. For this reason, we need to unpack the transfer
 * on the VE side and do all individual allocations and memory transfers at this
 * point.
 *
 * As we often also receive many batches of data, this is a good place to merge
 * them into a single batch, as that is more efficient to process on the VE.
 *
 * We write an "output" descriptor with the information about the individual
 * memory addresses to create the appropriate structures on the VH side.
 *
 * The memory for the output descriptor is pre-allocated on call. We can
 * therefore directly write to it.
 *
 * # Transfer descriptor
 * The transfer descriptor has the following format:
 * ```
 * [header size][batch count][column count][column descriptor]...[data]
 * ```
 * - *header size* is the number of bytes the header uses. It can be used as
 *   the offset to index into the structure to get to the start of the data.
 * - *batch count* is the number of batches included in this transfer
 * - *column count* is the number of columns within each batch
 *
 * These 3 fields are then followed by `batch count * column count` column
 * descriptors. The column descriptors are ordered such that the first column
 * of all batches is described. Then the second column of all batches, etc...
 *
 * The column descriptor can take multiple possible shapes depending on the
 * type of the column. They have the following general format:
 * ```
 * [column type][element count][buffer size]...
 * ```
 * - *column type* specifies which type the buffer is
 * - *element count* specifies how many elements there are in this particular
 *   column
 * - *buffer size* specifies how large each buffer (including alignment padding!)
 *   for that column is
 *
 * The number of *buffer size* definitions that follow the first two fields
 * depends entirely on the column type.
 *
 * The following column types are supported:
 * - COL_TYPE_SHORT
 * - COL_TYPE_INT
 * - COL_TYPE_BIGINT
 * - COL_TYPE_FLOAT
 * - COL_TYPE_DOUBLE
 * - COL_TYPE_VARCHAR
 *
 * COL_TYPE_SHORT, COL_TYPE_INT, COL_TYPE_BIGINT, COL_TYPE_FLOAT and
 * COL_TYPE_DOUBLE all follow with 2 buffer sizes (see scalar_col_in struct)
 *
 * COL_TYPE_VARCHAR follows with 4 buffers (see varchar_col_in struct).
 *
 * # Output descriptor
 * The output descriptor has the following format:
 * ```
 * [column output descriptor]...
 * ```
 * As we are merging the entire input into a single batch, there will be exactly
 * `column count` output descriptors.
 *
 * Every column output descriptor follows the following general format:
 * ```
 * [buffer pointer]...
 * ```
 * Because the calling method exactly knows about the column order and its types
 * it knows exactly how much output to expect, and can therefore use the stack
 * based method of data transfer.
 *
 * @param td transfer structure
 * @param od output descriptor
 * @return 0 if successful
 */
extern "C" int handle_transfer(
    char** td,
    uintptr_t* od
) {
  //std::cout << "Reading header." << std::endl;
  char* transfer = td[0];
  transfer_header *header = reinterpret_cast<transfer_header *>(transfer);
  //std::cout << "header->header_size="  << header->header_size << std::endl;
  //std::cout << "header->batch_count="  << header->batch_count << std::endl;
  //std::cout << "header->column_count="  << header->column_count << std::endl;

  char* input_data = &transfer[header->header_size];

  size_t input_pos = sizeof(transfer_header);
  size_t input_data_pos = 0;
  size_t output_pos = 0;
  for(auto c = 0; c < header->column_count; c++){
    size_t col_type = (reinterpret_cast<column_type *>(&transfer[input_pos]))->type;
    //std::cout << "col_type="  << col_type << std::endl;

    size_t col_start = input_pos;

    switch (col_type) {
      case COL_TYPE_SHORT:
      case COL_TYPE_INT:
      case COL_TYPE_BIGINT:
      case COL_TYPE_FLOAT:
      case COL_TYPE_DOUBLE: {
        size_t total_element_count = 0;
        size_t total_data_size = 0;
        size_t total_validity_buffer_size = 0;
        size_t aligned_data_size = 0;
        for(auto b = 0; b < header->batch_count; b++){
          size_t cur_col_type = (reinterpret_cast<column_type *>(&transfer[input_pos]))->type;
          input_pos += sizeof(column_type);
          // Sanity Check: Ensure column type doesn't change while reading a
          // batch of input columns
          if(col_type != cur_col_type){
            return -2;
          }

          scalar_col_in* col_in = reinterpret_cast<scalar_col_in *>(&transfer[input_pos]);
          input_pos += sizeof(scalar_col_in);

          //std::cout << "col_in->element_count="  << col_in->element_count << std::endl;
          //std::cout << "col_in->data_size="  << col_in->data_size << std::endl;
          //std::cout << "col_in->validity_buffer_size="  << col_in->validity_buffer_size << std::endl;


          total_element_count += col_in->element_count;
          total_data_size += col_in->data_size;
          total_validity_buffer_size += col_in->validity_buffer_size;
          aligned_data_size += VECTOR_ALIGNED(col_in->data_size) + VECTOR_ALIGNED(col_in->validity_buffer_size);
        }

        char* data = static_cast<char *>(malloc(total_data_size));
        const auto vbytes = sizeof(uint64_t) * frovedis::ceil_div(total_element_count, size_t(64));
        uint64_t* validity_buffer = static_cast<uint64_t *>(calloc(vbytes, 1));
        //std::cout << "&validity_buffer[vbytes]="  << uintptr_t(validity_buffer) + vbytes << std::endl;


        switch (col_type) {
          case COL_TYPE_SHORT:
            merge_scalar_transfer<int32_t>(header->batch_count, &transfer[col_start], &input_data[input_data_pos], data, validity_buffer, od, output_pos);
            break;
          case COL_TYPE_INT:
            merge_scalar_transfer<int32_t>(header->batch_count, &transfer[col_start], &input_data[input_data_pos], data, validity_buffer, od, output_pos);
            break;
          case COL_TYPE_BIGINT:
            merge_scalar_transfer<int64_t>(header->batch_count, &transfer[col_start], &input_data[input_data_pos], data, validity_buffer , od, output_pos);
            break;
          case COL_TYPE_FLOAT:
            merge_scalar_transfer<float>(header->batch_count, &transfer[col_start], &input_data[input_data_pos], data, validity_buffer , od, output_pos);
            break;
          case COL_TYPE_DOUBLE:
            merge_scalar_transfer<double>(header->batch_count, &transfer[col_start], &input_data[input_data_pos], data, validity_buffer , od, output_pos);
            break;
          default:
            // Should be impossible to reach at this point!
            return -99;
        }
        input_data_pos += aligned_data_size;
      }
        break;
      case COL_TYPE_VARCHAR: {
        size_t total_element_count = 0;
        size_t total_data_size = 0;
        size_t total_offsets_size = 0;
        size_t total_lengths_size = 0;
        size_t total_validity_buffer_size = 0;
        size_t aligned_data_size = 0;

        for(auto b = 0; b < header->batch_count; b++){
          size_t cur_col_type = (reinterpret_cast<column_type *>(&transfer[input_pos]))->type;
          input_pos += sizeof(column_type);

          // Sanity Check: Ensure column type doesn't change while reading a
          // batch of input columns
          if(col_type != cur_col_type){
            return -3;
          }

          varchar_col_in* col_in = reinterpret_cast<varchar_col_in *>(&transfer[input_pos]);
          input_pos += sizeof(varchar_col_in);

          //std::cout << "col_in->element_count="  << col_in->element_count << std::endl;
          //std::cout << "col_in->data_size="  << col_in->data_size << std::endl;
          //std::cout << "col_in->offsets_size="  << col_in->offsets_size << std::endl;
          //std::cout << "col_in->lengths_size="  << col_in->lengths_size << std::endl;
          //std::cout << "col_in->validity_buffer_size="  << col_in->validity_buffer_size << std::endl;

          total_element_count += col_in->element_count;
          total_data_size += col_in->data_size;
          total_offsets_size += col_in->offsets_size;
          total_lengths_size += col_in->lengths_size;
          total_validity_buffer_size += col_in->validity_buffer_size;
          aligned_data_size += VECTOR_ALIGNED(col_in->data_size) + VECTOR_ALIGNED(col_in->offsets_size) + VECTOR_ALIGNED(col_in->lengths_size) + VECTOR_ALIGNED(col_in->validity_buffer_size);
        }

        char* data = static_cast<char *>(malloc(total_data_size));
        char* lengths = static_cast<char *>(malloc(total_lengths_size));
        char* offsets = static_cast<char *>(malloc(total_offsets_size));

        const auto vbytes = sizeof(uint64_t) * frovedis::ceil_div(total_element_count, size_t(64));
        uint64_t* validity_buffer = static_cast<uint64_t *>(calloc(vbytes, 1));

        merge_varchar_transfer(header->batch_count, &transfer[col_start], &input_data[input_data_pos], data, validity_buffer, lengths, offsets, od, output_pos);
        input_data_pos += aligned_data_size;
      }
        break;
      default:
        // Fail on unknown column types
        return -4;
        break;
    }
  }

  // Collect any data within transfer before freeing it
  size_t expected_read = header->header_size;

  // All done. All transferred data has been copied where it needs to go. We can
  // free it now.
  free(transfer);

  //std::cout << "[handle_transfer] done." << std::endl;

  // Sanity Check: ensure that reading the header resulted in getting to the
  // same number read as the header specifies
  if(expected_read == input_pos){
    return 0;
  }else{
    return -1;
  }
}
