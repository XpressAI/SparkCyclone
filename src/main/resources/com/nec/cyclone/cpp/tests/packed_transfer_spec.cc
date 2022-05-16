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

      int res = handle_transfer(target, od);
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
  }
}
