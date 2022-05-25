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
#include "cyclone/cyclone.hpp"
#include "tests/doctest.h"
#include <stddef.h>

namespace cyclone::tests {
  TEST_SUITE("Packed Transfers") {
    template<typename T> const std::vector <T> raw{586, 951, 106, 318, 538, 620, 553, 605, 822, 941};

    template<typename T> void copy_scalar_vec_to_transfer_buffer(NullableScalarVec<T>* vec, char* header, char* data, size_t &header_pos, size_t &data_pos){
      column_type* col_type = reinterpret_cast<column_type *>(&header[header_pos]);
      if(std::is_same<T, int32_t>::value){
        col_type->type = COL_TYPE_INT;
      }else if(std::is_same<T, int64_t>::value){
        col_type->type = COL_TYPE_BIGINT;
      }else if(std::is_same<T, float>::value){
        col_type->type = COL_TYPE_FLOAT;
      }else if(std::is_same<T, double>::value){
        col_type->type = COL_TYPE_DOUBLE;
      }
      header_pos += sizeof(column_type);

      size_t element_count = static_cast<size_t>(vec->count);
      size_t data_size = sizeof(T) * element_count;
      size_t validity_buffer_size = frovedis::ceil_div(vec->count, int32_t(64)) * sizeof(uint64_t);

      scalar_col_in* col_in = reinterpret_cast<scalar_col_in *>(&header[header_pos]);
      col_in->element_count = element_count;
      col_in->data_size = data_size;
      col_in->validity_buffer_size = validity_buffer_size;
      header_pos += sizeof(scalar_col_in);

      std::memcpy(&data[data_pos], vec->data, data_size);
      data_pos += VECTOR_ALIGNED(data_size);

      std::memcpy(&data[data_pos], vec->validityBuffer, validity_buffer_size);
      data_pos += VECTOR_ALIGNED(validity_buffer_size);
    }

    void copy_varchar_vec_to_transfer_buffer(nullable_varchar_vector* vec, char* header, char* data, size_t &header_pos, size_t &data_pos){
      column_type* col_type = reinterpret_cast<column_type *>(&header[header_pos]);
      col_type->type = COL_TYPE_VARCHAR;
      header_pos += sizeof(column_type);

      size_t element_count = static_cast<size_t>(vec->count);
      size_t data_size = vec->dataSize * sizeof(int32_t);
      size_t offsets_size = sizeof(int32_t) * element_count;
      size_t lengths_size = sizeof(int32_t) * element_count;
      size_t validity_buffer_size = frovedis::ceil_div(vec->count, int32_t(64)) * sizeof(uint64_t);

      varchar_col_in* col_in = reinterpret_cast<varchar_col_in *>(&header[header_pos]);
      col_in->element_count = element_count;
      col_in->data_size = data_size;
      col_in->offsets_size = offsets_size;
      col_in->lengths_size = lengths_size;
      col_in->validity_buffer_size = validity_buffer_size;
      header_pos += sizeof(varchar_col_in);

      std::memcpy(&data[data_pos], vec->data, data_size);
      data_pos += VECTOR_ALIGNED(data_size);

      std::memcpy(&data[data_pos], vec->offsets, offsets_size);
      data_pos += VECTOR_ALIGNED(offsets_size);

      std::memcpy(&data[data_pos], vec->lengths, lengths_size);
      data_pos += VECTOR_ALIGNED(lengths_size);

      std::memcpy(&data[data_pos], vec->validityBuffer, validity_buffer_size);
      data_pos += VECTOR_ALIGNED(validity_buffer_size);
    }

    TEST_CASE_TEMPLATE("Unpacking works for single scalar vector of T=", T, int32_t, int64_t, float, double) {
      auto *vec1 = new NullableScalarVec(raw<T>);
      vec1->set_validity(1, 0);
      vec1->set_validity(4, 0);
      vec1->set_validity(8, 0);

      auto header_size = sizeof(transfer_header) + sizeof(size_t) + sizeof(scalar_col_in);

      size_t element_count = static_cast<size_t>(vec1->count);
      size_t data_size = VECTOR_ALIGNED(sizeof(T) * element_count);
      size_t validity_buffer_size = VECTOR_ALIGNED(frovedis::ceil_div(vec1->count, int32_t(64)) * sizeof(uint64_t));

      char* transfer = static_cast<char*>(malloc(header_size + data_size + validity_buffer_size));

      size_t pos = 0;
      transfer_header* header = reinterpret_cast<transfer_header *>(&transfer[pos]);
      header->header_size = header_size;
      header->batch_count = 1;
      header->column_count = 1;
      pos += sizeof(transfer_header);

      size_t data_pos = 0;
      size_t col_pos = 0;
      copy_scalar_vec_to_transfer_buffer(vec1, &transfer[pos], &transfer[header_size], col_pos, data_pos);

      uintptr_t* od = static_cast<uintptr_t*>(malloc(sizeof(uintptr_t) * 3));

      char* target[1] = {transfer};

      int res = handle_transfer(target, od);
      CHECK(res == 0);

      NullableScalarVec<T>* transferred = reinterpret_cast<NullableScalarVec<T>*>(od[0]);

      std::cout << "vec1 = " << std::endl;
      vec1->print();
      std::cout << "transferred = " << std::endl;
      transferred->print();

      CHECK(transferred->equals(vec1));
    }

    TEST_CASE("Unpacking works for single varchar vector"){
      std::vector<std::string> raw { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };
      auto *vec1 = new nullable_varchar_vector(raw);
      vec1->set_validity(1, 0);
      vec1->set_validity(4, 0);
      vec1->set_validity(10, 0);

      auto header_size = sizeof(transfer_header) + sizeof(size_t) + sizeof(varchar_col_in);

      size_t element_count = static_cast<size_t>(vec1->count);
      size_t data_size = VECTOR_ALIGNED(vec1->dataSize * sizeof(int32_t));
      size_t offsets_size = VECTOR_ALIGNED(element_count * sizeof(int32_t));
      size_t lengths_size = VECTOR_ALIGNED(element_count * sizeof(int32_t));
      size_t validity_buffer_size = VECTOR_ALIGNED(frovedis::ceil_div(vec1->count, int32_t(64)) * sizeof(uint64_t));

      char* transfer = static_cast<char*>(malloc(header_size + data_size + offsets_size + lengths_size + validity_buffer_size));

      size_t pos = 0;
      transfer_header* header = reinterpret_cast<transfer_header *>(&transfer[pos]);
      header->header_size = header_size;
      header->batch_count = 1;
      header->column_count = 1;
      pos += sizeof(transfer_header);

      size_t data_pos = 0;
      size_t col_pos = 0;
      copy_varchar_vec_to_transfer_buffer(vec1, &transfer[pos], &transfer[header_size], col_pos, data_pos);

      uintptr_t* od = static_cast<uintptr_t*>(malloc(sizeof(uintptr_t) * 5));

      char* target[1] = {transfer};

      int res = handle_transfer(target, od);
      CHECK(res == 0);

      nullable_varchar_vector* transferred = reinterpret_cast<nullable_varchar_vector*>(od[0]);

      vec1->print();
      transferred->print();

      CHECK(transferred->equals(vec1));
    }

    TEST_CASE("Unpacking works for two varchar vectors of an empty string each"){
      std::vector<std::string> raw { "" };
      auto *vec1 = new nullable_varchar_vector(raw);
      auto *vec2 = new nullable_varchar_vector(raw);

      auto header_size = sizeof(transfer_header) + 2*(sizeof(size_t) + sizeof(varchar_col_in));

      size_t element_count = static_cast<size_t>(vec1->count);
      size_t data_size = VECTOR_ALIGNED(vec1->dataSize * sizeof(int32_t));
      size_t offsets_size = VECTOR_ALIGNED(element_count * sizeof(int32_t));
      size_t lengths_size = VECTOR_ALIGNED(element_count * sizeof(int32_t));
      size_t validity_buffer_size = VECTOR_ALIGNED(frovedis::ceil_div(vec1->count, int32_t(64)) * sizeof(uint64_t));

      char* transfer = static_cast<char*>(malloc(header_size + 2*(data_size + offsets_size + lengths_size + validity_buffer_size)));

      size_t pos = 0;
      transfer_header* header = reinterpret_cast<transfer_header *>(&transfer[pos]);
      header->header_size = header_size;
      header->batch_count = 1;
      header->column_count = 2;
      pos += sizeof(transfer_header);

      size_t data_pos = 0;
      size_t col_pos = 0;
      copy_varchar_vec_to_transfer_buffer(vec1, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_varchar_vec_to_transfer_buffer(vec2, &transfer[pos], &transfer[header_size], col_pos, data_pos);

      uintptr_t* od = static_cast<uintptr_t*>(malloc(sizeof(uintptr_t) * 10));

      char* target[1] = {transfer};

      int res = handle_transfer(target, od);
      CHECK(res == 0);

      nullable_varchar_vector* transferred1 = reinterpret_cast<nullable_varchar_vector*>(od[0]);
      nullable_varchar_vector* transferred2 = reinterpret_cast<nullable_varchar_vector*>(od[5]);

      vec1->print();
      transferred1->print();
      vec2->print();
      transferred2->print();

      CHECK(transferred1->equals(vec1));
      CHECK(transferred2->equals(vec2));
    }

    TEST_CASE("Unpacking works for multiple vectors of different types"){
      std::vector<std::string> raw { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };
      auto *vec1 = new nullable_varchar_vector(raw);
      vec1->set_validity(1, 0);
      vec1->set_validity(4, 0);
      vec1->set_validity(10, 0);

      auto *vec2 = new NullableScalarVec<int32_t>({586, 951, 106, 318, 538, 620, 553, 605, 822, 941});
      vec2->set_validity(2, 0);
      vec2->set_validity(5, 0);
      vec2->set_validity(9, 0);

      auto *vec3 = new NullableScalarVec<double>({2.5, 3.141, 12, 4, 9, 2.2222, 3.333});
      vec3->set_validity(3, 0);
      vec3->set_validity(6, 0);

      auto header_size = sizeof(transfer_header) + (2 * (sizeof(size_t) + sizeof(scalar_col_in))) + sizeof(size_t) + sizeof(varchar_col_in);

      size_t element_count = static_cast<size_t>(vec1->count);
      size_t data_size = VECTOR_ALIGNED(vec1->dataSize * sizeof(int32_t)) + VECTOR_ALIGNED(vec2->count * sizeof(int32_t)) + VECTOR_ALIGNED(vec3->count * sizeof(double));
      size_t offsets_size = VECTOR_ALIGNED(element_count * sizeof(int32_t));
      size_t lengths_size = VECTOR_ALIGNED(element_count * sizeof(int32_t));
      size_t validity_buffer_size = VECTOR_ALIGNED(sizeof(uint64_t) * (frovedis::ceil_div(vec1->count, int32_t(64)) + frovedis::ceil_div(vec2->count, int32_t(64)) + frovedis::ceil_div(vec3->count, int32_t(64))));

      char* transfer = static_cast<char*>(malloc(header_size + data_size + offsets_size + lengths_size + validity_buffer_size));

      size_t pos = 0;
      transfer_header* header = reinterpret_cast<transfer_header *>(&transfer[pos]);
      header->header_size = header_size;
      header->batch_count = 1;
      header->column_count = 3;
      pos += sizeof(transfer_header);

      size_t data_pos = 0;
      size_t col_pos = 0;
      copy_varchar_vec_to_transfer_buffer(vec1, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_scalar_vec_to_transfer_buffer(vec2, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_scalar_vec_to_transfer_buffer(vec3, &transfer[pos], &transfer[header_size], col_pos, data_pos);

      uintptr_t* od = static_cast<uintptr_t*>(malloc(sizeof(uintptr_t) * (5 + 3 + 3)));

      char* target[1] = {transfer};

      std::cout << "before" << std::endl;

      int res = handle_transfer(target, od);

      std::cout << "after" << std::endl;

      CHECK(res == 0);

      nullable_varchar_vector* transferred1 = reinterpret_cast<nullable_varchar_vector*>(od[0]);

      std::cout << "vec1 = " << std::endl;
      vec1->print();
      std::cout << "transferred1 = " << std::endl;
      transferred1->print();
      CHECK(transferred1->equals(vec1));

      NullableScalarVec<int32_t>* transferred2 = reinterpret_cast<NullableScalarVec<int32_t>*>(od[5]);

      std::cout << "vec2 = " << std::endl;
      vec2->print();
      std::cout << "transferred2 = " << std::endl;
      transferred2->print();
      CHECK(transferred2->equals(vec2));

      NullableScalarVec<double>* transferred3 = reinterpret_cast<NullableScalarVec<double>*>(od[8]);

      std::cout << "vec3 = " << std::endl;
      vec3->print();
      std::cout << "transferred3 = " << std::endl;
      transferred3->print();
      CHECK(transferred3->equals(vec3));
    }

    TEST_CASE("Unpacking and merging two batches works for multiple vectors of different types"){
      std::vector<std::string> raw1 { "JAN", "FEB", "MAR", "APR", "MAY", "JUN"};
      std::vector<std::string> raw2 { "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };
      auto *vc_vec1 = new nullable_varchar_vector(raw1);
      vc_vec1->set_validity(1, 0);
      vc_vec1->set_validity(3, 0);
      vc_vec1->set_validity(5, 0);

      auto *vc_vec2 = new nullable_varchar_vector(raw2);
      vc_vec2->set_validity(0, 0);
      vc_vec2->set_validity(2, 0);
      vc_vec2->set_validity(4, 0);

      auto *sc_vec1 = new NullableScalarVec<int32_t>({586, 951, 106, 318, 538, 620});
      sc_vec1->set_validity(1, 0);
      sc_vec1->set_validity(3, 0);
      sc_vec1->set_validity(5, 0);

      auto *sc_vec2 = new NullableScalarVec<int32_t>({553, 605, 822, 941});
      sc_vec2->set_validity(2, 0);
      sc_vec2->set_validity(3, 0);

      auto header_size = sizeof(transfer_header) + (2 * (sizeof(size_t) + sizeof(scalar_col_in))) + (2* (sizeof(size_t) + sizeof(varchar_col_in)));

      size_t data_size = VECTOR_ALIGNED(vc_vec1->dataSize * sizeof(int32_t)) + VECTOR_ALIGNED(vc_vec2->dataSize * sizeof(int32_t)) + VECTOR_ALIGNED(sc_vec1->count * sizeof(int32_t)) + VECTOR_ALIGNED(sc_vec2->count * sizeof(int32_t));
      size_t offsets_size = VECTOR_ALIGNED(vc_vec1->count * sizeof(int32_t)) + VECTOR_ALIGNED(vc_vec2->count * sizeof(int32_t));
      size_t lengths_size = VECTOR_ALIGNED(vc_vec1->count * sizeof(int32_t)) + VECTOR_ALIGNED(vc_vec2->count * sizeof(int32_t));
      size_t validity_buffer_size = VECTOR_ALIGNED(sizeof(uint64_t) * ( frovedis::ceil_div(vc_vec1->count, int32_t(64))
                                                       + frovedis::ceil_div(vc_vec2->count, int32_t(64))
                                                       + frovedis::ceil_div(sc_vec1->count, int32_t(64))
                                                       + frovedis::ceil_div(sc_vec2->count, int32_t(64))));

      char* transfer = static_cast<char*>(malloc(header_size + data_size + offsets_size + lengths_size + validity_buffer_size));

      size_t pos = 0;
      transfer_header* header = reinterpret_cast<transfer_header *>(&transfer[pos]);
      header->header_size = header_size;
      header->batch_count = 2;
      header->column_count = 2;
      pos += sizeof(transfer_header);

      size_t data_pos = 0;
      size_t col_pos = 0;
      copy_varchar_vec_to_transfer_buffer(vc_vec1, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_varchar_vec_to_transfer_buffer(vc_vec2, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_scalar_vec_to_transfer_buffer(sc_vec1, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_scalar_vec_to_transfer_buffer(sc_vec2, &transfer[pos], &transfer[header_size], col_pos, data_pos);

      uintptr_t* od = static_cast<uintptr_t*>(malloc(sizeof(uintptr_t) * (5 + 3)));

      char* target[1] = {transfer};

      int res = handle_transfer(target, od);
      CHECK(res == 0);

      nullable_varchar_vector* varchars[2] = {vc_vec1, vc_vec2};
      NullableScalarVec<int32_t>* scalars[2] = {sc_vec1, sc_vec2};

      nullable_varchar_vector* vc_merged = nullable_varchar_vector::merge(varchars, 2);
      NullableScalarVec<int32_t>* sc_merged = NullableScalarVec<int32_t>::merge(scalars, 2);

      nullable_varchar_vector* transferred1 = reinterpret_cast<nullable_varchar_vector*>(od[0]);

      std::cout << "vc_merged = " << std::endl;
      vc_merged->print();
      std::cout << "transferred1 = " << std::endl;
      transferred1->print();
      CHECK(transferred1->equals(vc_merged));

      NullableScalarVec<int32_t>* transferred2 = reinterpret_cast<NullableScalarVec<int32_t>*>(od[5]);

      std::cout << "sc_merged = " << std::endl;
      sc_merged->print();
      std::cout << "transferred2 = " << std::endl;
      transferred2->print();
      CHECK(transferred2->equals(sc_merged));
    }

    TEST_CASE("Unpacking and merging three batches works for multiple vectors of different types"){
      std::vector<std::string> raw1 { "JAN", "FEB", "MAR", "APR", "MAY", "JUN"};
      std::vector<std::string> raw2 { "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };
      std::vector<std::string> raw3 { "A", "b", "C"};
      auto *vc_vec1 = new nullable_varchar_vector(raw1);
      vc_vec1->set_validity(1, 0);
      vc_vec1->set_validity(3, 0);
      vc_vec1->set_validity(5, 0);

      auto *vc_vec2 = new nullable_varchar_vector(raw2);
      vc_vec2->set_validity(0, 0);
      vc_vec2->set_validity(2, 0);
      vc_vec2->set_validity(4, 0);

      auto *vc_vec3 = new nullable_varchar_vector(raw3);
      vc_vec3->set_validity(1, 0);

      auto *sc_vec1 = new NullableScalarVec<int32_t>({586, 951, 106, 318, 538, 620});
      sc_vec1->set_validity(1, 0);
      sc_vec1->set_validity(3, 0);
      sc_vec1->set_validity(5, 0);

      auto *sc_vec2 = new NullableScalarVec<int32_t>({553, 605, 822, 941});
      sc_vec2->set_validity(2, 0);
      sc_vec2->set_validity(3, 0);

      auto *sc_vec3 = new NullableScalarVec<int32_t>({53, 5, 22, 94});
      sc_vec2->set_validity(0, 0);
      sc_vec2->set_validity(1, 0);

      auto header_size = sizeof(transfer_header) + (3 * (sizeof(size_t) + sizeof(scalar_col_in))) + (3 * (sizeof(size_t) + sizeof(varchar_col_in)));

      size_t data_size = (VECTOR_ALIGNED(vc_vec1->dataSize * sizeof(int32_t)) + VECTOR_ALIGNED(vc_vec2->dataSize * sizeof(int32_t)) + VECTOR_ALIGNED(vc_vec3->dataSize * sizeof(int32_t))
                         + VECTOR_ALIGNED(sc_vec1->count * sizeof(int32_t)) + VECTOR_ALIGNED(sc_vec2->count * sizeof(int32_t)) + VECTOR_ALIGNED(sc_vec3->count * sizeof(int32_t)));
      size_t offsets_size = VECTOR_ALIGNED(vc_vec1->count * sizeof(int32_t)) + VECTOR_ALIGNED(vc_vec2->count * sizeof(int32_t)) + VECTOR_ALIGNED(vc_vec3->count * sizeof(int32_t));
      size_t lengths_size = VECTOR_ALIGNED(vc_vec1->count * sizeof(int32_t)) + VECTOR_ALIGNED(vc_vec2->count * sizeof(int32_t)) + VECTOR_ALIGNED(vc_vec3->count * sizeof(int32_t));
      size_t validity_buffer_size = VECTOR_ALIGNED(sizeof(uint64_t) * ( frovedis::ceil_div(vc_vec1->count, int32_t(64))
                                                       + frovedis::ceil_div(vc_vec2->count, int32_t(64))
                                                       + frovedis::ceil_div(vc_vec3->count, int32_t(64))
                                                       + frovedis::ceil_div(sc_vec1->count, int32_t(64))
                                                       + frovedis::ceil_div(sc_vec2->count, int32_t(64))
                                                       + frovedis::ceil_div(sc_vec3->count, int32_t(64))));

      char* transfer = static_cast<char*>(malloc(header_size + data_size + offsets_size + lengths_size + validity_buffer_size));

      size_t pos = 0;
      transfer_header* header = reinterpret_cast<transfer_header *>(&transfer[pos]);
      header->header_size = header_size;
      header->batch_count = 3;
      header->column_count = 2;
      pos += sizeof(transfer_header);

      size_t data_pos = 0;
      size_t col_pos = 0;
      copy_varchar_vec_to_transfer_buffer(vc_vec1, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_varchar_vec_to_transfer_buffer(vc_vec2, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_varchar_vec_to_transfer_buffer(vc_vec3, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_scalar_vec_to_transfer_buffer(sc_vec1, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_scalar_vec_to_transfer_buffer(sc_vec2, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_scalar_vec_to_transfer_buffer(sc_vec3, &transfer[pos], &transfer[header_size], col_pos, data_pos);

      uintptr_t* od = static_cast<uintptr_t*>(malloc(sizeof(uintptr_t) * (5 + 3)));

      char* target[1] = {transfer};

      int res = handle_transfer(target, od);
      CHECK(res == 0);

      nullable_varchar_vector* varchars[3] = {vc_vec1, vc_vec2, vc_vec3};
      NullableScalarVec<int32_t>* scalars[3] = {sc_vec1, sc_vec2, sc_vec3};

      nullable_varchar_vector* vc_merged = nullable_varchar_vector::merge(varchars, 3);
      NullableScalarVec<int32_t>* sc_merged = NullableScalarVec<int32_t>::merge(scalars, 3);

      nullable_varchar_vector* transferred1 = reinterpret_cast<nullable_varchar_vector*>(od[0]);

      std::cout << "vc_merged = " << std::endl;
      vc_merged->print();
      std::cout << "transferred1 = " << std::endl;
      transferred1->print();
      CHECK(transferred1->equals(vc_merged));

      NullableScalarVec<int32_t>* transferred2 = reinterpret_cast<NullableScalarVec<int32_t>*>(od[5]);

      std::cout << "sc_merged = " << std::endl;
      sc_merged->print();
      std::cout << "transferred2 = " << std::endl;
      transferred2->print();
      CHECK(transferred2->equals(sc_merged));
    }

    TEST_CASE_TEMPLATE("Unpacking works for three scalar vector of T=", T, int32_t, int64_t, float, double) {
        uint64_t validity_buffers[3] = {
          0b0000000000000000000000000000000000000000000000000110011000110010,
    //                                                      ^ valid until here
          0b0000000011001001011001001000100101001000011011101000001100101000,
    //              ^ valid until here
          0b0000011010100101101110010010101111000011010000000001100100111001
    //           ^ valid until here
        };
      const std::vector <T> a{0, 4436, 0, 0, 9586, 2142, 0, 0, 0, 2149, 4297, 0, 0, 3278, 6668, 0};
      auto *vec1 = new NullableScalarVec(a);
      vec1->validityBuffer = &validity_buffers[0];
      
      const std::vector <T> b{
        0, 0, 0, 8051, 0, 1383, 0, 0, 2256, 5785, 0, 0, 0, 0, 0, 4693, 0, 1849, 3790, 8995, 0, 6961, 7132, 0, 0, 0, 0, 6968, 0, 0, 3763, 0, 3558, 0, 0, 2011, 0, 0, 0, 3273, 0, 0, 9428, 0, 0, 6408, 7940, 0, 9521, 0, 0, 5832, 0, 0, 5817, 5949
      };
      auto *vec2 = new NullableScalarVec(b);
      vec2->validityBuffer = &validity_buffers[1];
      
      const std::vector <T> c{
        7319, 0, 0, 4859, 524, 406, 0, 0, 1154, 0, 0, 1650, 8040, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1146, 0, 7268, 8197, 0, 0, 0, 0, 81, 2053, 6571, 4600, 0, 3699, 0, 8404, 0, 0, 8401, 0, 0, 6234, 6281, 7367, 0, 4688, 7490, 0, 5412, 0, 0, 871, 0, 9086, 0, 5362, 6516
      };
      auto *vec3 = new NullableScalarVec(c);
      vec3->validityBuffer = &validity_buffers[2];
      


      auto header_size = sizeof(transfer_header) + 3 * (sizeof(size_t) + sizeof(scalar_col_in));

      size_t data_size = VECTOR_ALIGNED(sizeof(T) * vec1->count) + VECTOR_ALIGNED(sizeof(T) * vec2->count) + VECTOR_ALIGNED(sizeof(T) * vec3->count);
      size_t validity_buffer_size = VECTOR_ALIGNED(frovedis::ceil_div(vec1->count, int32_t(64)) * sizeof(uint64_t))
                                  + VECTOR_ALIGNED(frovedis::ceil_div(vec2->count, int32_t(64)) * sizeof(uint64_t))
                                  + VECTOR_ALIGNED(frovedis::ceil_div(vec3->count, int32_t(64)) * sizeof(uint64_t));

      char* transfer = static_cast<char*>(malloc(header_size + data_size + validity_buffer_size));

      size_t pos = 0;
      transfer_header* header = reinterpret_cast<transfer_header *>(&transfer[pos]);
      header->header_size = header_size;
      header->batch_count = 3;
      header->column_count = 1;
      pos += sizeof(transfer_header);

      size_t data_pos = 0;
      size_t col_pos = 0;
      copy_scalar_vec_to_transfer_buffer(vec1, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_scalar_vec_to_transfer_buffer(vec2, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_scalar_vec_to_transfer_buffer(vec3, &transfer[pos], &transfer[header_size], col_pos, data_pos);

      uintptr_t* od = static_cast<uintptr_t*>(malloc(sizeof(uintptr_t) * 3));

      char* target[1] = {transfer};

      int res = handle_transfer(target, od);
      CHECK(res == 0);

      NullableScalarVec<T>* transferred = reinterpret_cast<NullableScalarVec<T>*>(od[0]);

      NullableScalarVec<T>* scalars[3] = {vec1, vec2, vec3};
      NullableScalarVec<T>* merged = NullableScalarVec<T>::merge(scalars, 3);

      std::cout << "merged = " << std::endl;
      merged->print();
      std::cout << "transferred = " << std::endl;
      transferred->print();

      CHECK(transferred->equals(merged));
    }
    
    TEST_CASE("Unpacking works for three varchar vectors") {
        uint64_t validity_buffers[3] = {
          0b0000000000000000000000000000000000000000000000000110011000110010,
//                                                      ^ valid until here
          0b0000000011001001011001001000100101001000011011101000001100101000,
//              ^ valid until here
          0b0000011010100101101110010010101111000011010000000001100100111001
//           ^ valid until here
        };
      const std::vector <std::string> a{"0", "4436", "0", "0", "9586", "2142", "0", "0", "0", "2149", "4297", "0", "0", "3278", "6668", "0"};
      auto *vec1 = new nullable_varchar_vector(a);
      vec1->validityBuffer = &validity_buffers[0];
      
      const std::vector <std::string> b{
        "0", "0", "0", "8051", "0", "1383", "0", "0", "2256", "5785", "0", "0", "0", "0", "0", "4693", "0", "1849", "3790", "8995", "0", "6961", "7132", "0", "0", "0", "0", "6968", "0", "0", "3763", "0", "3558", "0", "0", "2011", "0", "0", "0", "3273", "0", "0", "9428", "0", "0", "6408", "7940", "0", "9521", "0", "0", "5832", "0", "0", "5817", "5949"
      };
      auto *vec2 = new nullable_varchar_vector(b);
      vec2->validityBuffer = &validity_buffers[1];
      
      const std::vector <std::string> c{
        "7319", "0", "0", "4859", "524", "406", "0", "0", "1154", "0", "0", "1650", "8040", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1146", "0", "7268", "8197", "0", "0", "0", "0", "81", "2053", "6571", "4600", "0", "3699", "0", "8404", "0", "0", "8401", "0", "0", "6234", "6281", "7367", "0", "4688", "7490", "0", "5412", "0", "0", "871", "0", "9086", "0", "5362", "6516"
      };
      auto *vec3 = new nullable_varchar_vector(c);
      vec3->validityBuffer = &validity_buffers[2];
      


      auto header_size = sizeof(transfer_header) + 3 * (sizeof(size_t) + sizeof(varchar_col_in));

      size_t data_size = VECTOR_ALIGNED(vec1->dataSize * sizeof(int32_t)) + VECTOR_ALIGNED(vec2->dataSize * sizeof(int32_t)) + VECTOR_ALIGNED(vec3->dataSize * sizeof(int32_t));
      size_t validity_buffer_size = VECTOR_ALIGNED(frovedis::ceil_div(vec1->count, int32_t(64)) * sizeof(uint64_t))
                                  + VECTOR_ALIGNED(frovedis::ceil_div(vec2->count, int32_t(64)) * sizeof(uint64_t))
                                  + VECTOR_ALIGNED(frovedis::ceil_div(vec3->count, int32_t(64)) * sizeof(uint64_t));
      size_t offsets_size = VECTOR_ALIGNED(vec1->count * sizeof(int32_t)) + VECTOR_ALIGNED(vec2->count * sizeof(int32_t)) + VECTOR_ALIGNED(vec3->count * sizeof(int32_t));
      size_t lengths_size = VECTOR_ALIGNED(vec1->count * sizeof(int32_t)) + VECTOR_ALIGNED(vec2->count * sizeof(int32_t)) + VECTOR_ALIGNED(vec3->count * sizeof(int32_t));

      char* transfer = static_cast<char*>(malloc(header_size + data_size + validity_buffer_size + offsets_size + lengths_size));

      size_t pos = 0;
      transfer_header* header = reinterpret_cast<transfer_header *>(&transfer[pos]);
      header->header_size = header_size;
      header->batch_count = 3;
      header->column_count = 1;
      pos += sizeof(transfer_header);

      size_t data_pos = 0;
      size_t col_pos = 0;
      copy_varchar_vec_to_transfer_buffer(vec1, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_varchar_vec_to_transfer_buffer(vec2, &transfer[pos], &transfer[header_size], col_pos, data_pos);
      copy_varchar_vec_to_transfer_buffer(vec3, &transfer[pos], &transfer[header_size], col_pos, data_pos);

      uintptr_t* od = static_cast<uintptr_t*>(malloc(sizeof(uintptr_t) * 5));

      char* target[1] = {transfer};

      int res = handle_transfer(target, od);
      CHECK(res == 0);

      nullable_varchar_vector* transferred = reinterpret_cast<nullable_varchar_vector*>(od[0]);

      nullable_varchar_vector* varchars[3] = {vec1, vec2, vec3};
      nullable_varchar_vector* merged = nullable_varchar_vector::merge(varchars, 3);

      std::cout << "merged = " << std::endl;
      merged->print();
      std::cout << "transferred = " << std::endl;
      transferred->print();

      CHECK(transferred->equals(merged));
    }
  }

  std::vector<std::string> gen_words(size_t word_len, size_t word_cnt, char c = '*') {

    std::string a_str = std::string(word_len, c);

    std::vector<std::string> words(word_cnt);
    for (int i=0; i<word_cnt; i++) {
        words.push_back(std::string(a_str));
    }
    return words;
  }

   TEST_CASE("transfer very big nullable_varchar_vec") {

        // at least 64 mb
        std::vector<std::string> strs_a = gen_words(2048, 32768, 'a');
        std::vector<std::string> strs_b = gen_words(2048, 32768, 'b');

        // copy generated words into nullable_varchar_vec
        nullable_varchar_vector* nvv_a = new nullable_varchar_vector(strs_a);
        nullable_varchar_vector* nvv_b = new nullable_varchar_vector(strs_b);

        // src words are not needed anymore
        strs_a.clear();
        strs_a.shrink_to_fit();
        strs_b.clear();
        strs_b.shrink_to_fit();

        // create output descriptor for handle_transfer
        uintptr_t od[3];

        // prepare transfer buf
        size_t header_size = sizeof(transfer_header) + sizeof(size_t) + sizeof(varchar_col_in);
        size_t data_size = nvv_a->dataSize * sizeof(int32_t);
        size_t offsets_size = nvv_a->count * sizeof(int32_t);
        size_t lengths_size = nvv_a->count * sizeof(int32_t);
        size_t validity_buffer_size = frovedis::ceil_div(nvv_a->count, int32_t(64)) * sizeof(uint64_t);

        std::cout << "header_size = " << header_size << std::endl;

        size_t total_size = VECTOR_ALIGNED(header_size) 
                          + VECTOR_ALIGNED(data_size) 
                          + VECTOR_ALIGNED(validity_buffer_size)
                          + VECTOR_ALIGNED(offsets_size)
                          + VECTOR_ALIGNED(lengths_size);

        std::cout << "total_size = " << total_size << std::endl;

        char *transfer = new char[total_size];
        size_t pos = 0;

        std::cout << "transfer buffer has been allocated" << std::endl;

        // use the first bytes of the transfer as header
        transfer_header* header = reinterpret_cast<transfer_header*>(&transfer[0]);
        header->header_size = 0;
        header->batch_count = 1;
        header->column_count = 1;
        pos += sizeof(transfer_header);

        std::cout << "header at 0x" << std::hex << header << " has been written, pos is now " << pos << std::endl << std::flush;

        size_t* column_type = reinterpret_cast<size_t *>(&transfer[pos]);
        *column_type = COL_TYPE_VARCHAR;
        pos += sizeof(size_t);

        std::cout << "column_type has been written, pos is now " << pos << std::endl;

        varchar_col_in* col_header = reinterpret_cast<varchar_col_in *>(&transfer[pos]);

        col_header->element_count = nvv_a->count;
        col_header->data_size = nvv_a->dataSize * sizeof(int32_t);
        col_header->offsets_size = nvv_a->count * sizeof(int32_t);
        col_header->lengths_size = nvv_a->count * sizeof(int32_t);
        col_header->validity_buffer_size = frovedis::ceil_div(nvv_a->count, int32_t(64)) * sizeof(uint64_t);
        pos += sizeof(varchar_col_in);

        std::cout << "col_header has been written, pos is now " << pos << std::endl;

        // align pos for data
        pos = VECTOR_ALIGNED(pos);

        std::cout << "pos is now vector aligned at " << pos << std::endl << std::flush;
	std::cout << "header at 0x" << std::hex << header << " will be corrected " << std::endl << std::flush;

        // set header size
        header->header_size = pos; // this SIGSEGVs

        std::cout << "Will write " << data_size << " bytes starting from position " << pos << std::endl << std::flush;

        // copy data (aligned)
        std::memcpy(&transfer[pos], nvv_a->data, data_size);
        pos += VECTOR_ALIGNED(data_size);

        std::cout << "data has been written, pos is now " << pos << std::endl << std::flush;

        // copy offsets (aligned)
        std::memcpy(&transfer[pos], nvv_a->offsets, offsets_size);
        pos += VECTOR_ALIGNED(offsets_size);

        std::cout << "offsets has been written, pos is now " << pos << std::endl << std::flush;

         // copy lengths (alinged)
        std::memcpy(&transfer[pos], nvv_a->lengths, lengths_size);
        pos += VECTOR_ALIGNED(lengths_size);

        std::cout << "lengths has been written, pos is now " << pos << std::endl << std::flush;

        // copy validity buffer
        std::memcpy(&transfer[pos], nvv_a->lengths, validity_buffer_size);
        pos += VECTOR_ALIGNED(validity_buffer_size);

        std::cout << "validity buffer has been written, pos is now " << pos << std::endl << std::flush;

        // transfer
        char* target[1] = {transfer};

        int res = handle_transfer(target, od);
        CHECK(res == 0);

        // cleanup
        delete [] transfer;

        nvv_b->reset();
        delete nvv_b;

        nvv_a->reset();
        delete nvv_a;
   }
}
