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
#include <tuple>

namespace cyclone::tests {
  TEST_CASE("bitmask_to_matching_ids() works") {
    std::vector<size_t> bitmask = { 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1 };
    std::vector<size_t> expected = { 2, 7, 8, 9, 10, 13, 14 };
    CHECK(cyclone::bitmask_to_matching_ids(bitmask) == expected);
  }

  TEST_CASE("std::vector print works") {
    std::vector<size_t> vec = { 2, 7, 8, 9, 10, 13, 14 };
    std::cout << vec << std::endl;
  }

  TEST_CASE("std::tuple print works") {
    auto tup = std::make_tuple(5, "Hello", -0.1);
    std::cout << tup << std::endl;
  }

  TEST_CASE("separate_to_groups() works") {
      // std::vector<size_t> grouping = { 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1 };
      std::vector<size_t> grouping = { 10, 10, 11, 10, 10, 10, 10, 11, 11, 11, 11, 10, 10, 11, 11 };
      std::vector<size_t> expected_1 = { 2, 7, 8, 9, 10, 13, 14 };
      std::vector<size_t> expected_0 = { 0, 1, 3, 4, 5, 6, 11, 12 };
      std::vector<size_t> expected_keys = {10, 11};
      std::vector<size_t> keys;
      std::vector<std::vector<size_t>> groups = cyclone::separate_to_groups(grouping, keys);

      cyclone::print_vec("groups[0]", groups[0]);
      cyclone::print_vec("expect_0:", expected_0);
      cyclone::print_vec("groups[1]", groups[1]);
      cyclone::print_vec("expect_1:", expected_1);

      CHECK(groups[0] == expected_0);
      CHECK(groups[1] == expected_1);
      CHECK(keys == expected_keys);
  }

  TEST_CASE("old separate_to_groups() works") {
    std::vector<size_t> grouping = { 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1 };
    std::vector<size_t> expected_1 = { 2, 7, 8, 9, 10, 13, 14 };
    std::vector<size_t> expected_0 = { 0, 1, 3, 4, 5, 6, 11, 12 };
    std::vector<size_t> expected_keys = {0, 1};
    std::vector<size_t> keys;
    std::vector<std::vector<size_t>> groups = cyclone::separate_to_groups(grouping, keys);

    cyclone::print_vec("groups[0]", groups[0]);
    cyclone::print_vec("expect_0:", expected_0);
    cyclone::print_vec("groups[1]", groups[1]);
    cyclone::print_vec("expect_1:", expected_1);

    CHECK(groups[0] == expected_0);
    CHECK(groups[1] == expected_1);
    CHECK(keys == expected_keys);
  }

  TEST_CASE("empty separate_to_groups() is still ok.") {
    std::vector<size_t> grouping = {};
    std::vector<size_t> keys;
    std::vector<size_t> empty_keys = {};
    std::vector<std::vector<size_t>> groups = cyclone::separate_to_groups(grouping, keys);

    CHECK(groups.size() == 0);
    CHECK(keys == empty_keys);
  }

  TEST_CASE("joining works on fully matched pairs") {
    std::vector<size_t> left = {1, 2, 3, 4, 5, 6};
    std::vector<size_t> right = {6, 3, 2, 4, 5, 1};
    std::vector<size_t> out_left;
    std::vector<size_t> out_right;

    std::vector<size_t> expected_left = {0, 1, 2, 3, 4, 5};
    std::vector<size_t> expected_right = {5, 2, 1, 3, 4, 0};

    cyclone::equi_join_indices(left, right, out_left, out_right);

    cyclone::print_vec("out_left", out_left);
    cyclone::print_vec("exp_left", expected_left);
    cyclone::print_vec("out_right", out_right);
    cyclone::print_vec("exp_right", expected_right);

    CHECK(out_left.size() == left.size());
    CHECK(out_right.size() == right.size());
    CHECK(out_left == expected_left);
    CHECK(out_right == expected_right);
  }

  TEST_CASE("joining works on partially unmatched pairs") {
    std::vector<size_t> left = {1, 2, 3, 4, 5, 6};
    std::vector<size_t> right = {12, 3, 1, 69, 0};
    std::vector<size_t> out_left;
    std::vector<size_t> out_right;

    std::vector<size_t> expected_left = {0, 2};
    std::vector<size_t> expected_right = {2, 1};

    cyclone::equi_join_indices(left, right, out_left, out_right);

    cyclone::print_vec("out_left", out_left);
    cyclone::print_vec("exp_left", expected_left);
    cyclone::print_vec("out_right", out_right);
    cyclone::print_vec("exp_right", expected_right);

    CHECK(out_left.size() == 2);
    CHECK(out_right.size() == 2);
    CHECK(out_left == expected_left);
    CHECK(out_right == expected_right);
  }

  TEST_CASE("Appending bitset to empty memory works") {
    uint64_t b[3] = {32, 32 + 64, 128};

    uint64_t* output = static_cast<uint64_t *>(malloc(3 * sizeof(uint64_t)));

    cyclone::append_bitsets(output, 0, b, 8 * 3 * sizeof(uint64_t));

    CHECK(output[0] == b[0]);
    CHECK(output[1] == b[1]);
    CHECK(output[2] == b[2]);
    free(output);
  }

  TEST_CASE("Merging two uint64_t aligned bitsets works") {
     uint64_t a[2] = {128, 255};
     uint64_t b[3] = {32, 32 + 64, 128};

     uint64_t* output = static_cast<uint64_t *>(malloc(5 * sizeof(uint64_t)));

     cyclone::append_bitsets(output, 0, a, 8 * 2 * sizeof(uint64_t));
     cyclone::append_bitsets(&output[2], 0, b, 8 * 3 * sizeof(uint64_t));

     CHECK(output[0] == a[0]);
     CHECK(output[1] == a[1]);
     CHECK(output[2] == b[0]);
     CHECK(output[3] == b[1]);
     CHECK(output[4] == b[2]);
     free(output);
  }

  TEST_CASE("Merging two byte aligned bitsets works") {
    char a[2] = {char(128), char(255)};
    char b[3] = {32, 32 + 64, char(128)};

    char* output = static_cast<char *>(malloc(5 * sizeof(char)));

    cyclone::append_bitsets(output, 0, a, 8 * 2 * sizeof(char));
    cyclone::append_bitsets(&output[2], 0, b, 8 * 3 * sizeof(char));

    CHECK(output[0] == a[0]);
    CHECK(output[1] == a[1]);
    CHECK(output[2] == b[0]);
    CHECK(output[3] == b[1]);
    CHECK(output[4] == b[2]);
    free(output);
  }

  TEST_CASE("Merging two unaligned bitsets works") {
    char a[9] = {char(128), char(255), 8, 16, 15, 7, 9, 0, 1};
    uint64_t b[3] = {32, 32 + 64, 128};

    size_t expected_dangling = 8;
    size_t expected_free_tail = 64 - 8;

    char expected_0[8]   = {char(128), char(255), 8, 16, 15, 7, 9, 0};
    char expected_1_0[8] = {1, 0, 0,  0, 0, 0, 0, 0};
    uint64_t * expected_0_uint64 = reinterpret_cast<uint64_t *>(expected_0);
    uint64_t * expected_1_1_uint64 = reinterpret_cast<uint64_t *>(expected_1_0);
    uint64_t expected_1 = expected_1_1_uint64[0] | (b[0] << expected_dangling);
    uint64_t expected_2 = (b[0] >> expected_free_tail) | (b[1] << expected_dangling);
    uint64_t expected_3 = (b[1] >> expected_free_tail) | (b[2] << expected_dangling);
    uint64_t expected_4 = (b[2] >> expected_free_tail);


    uint64_t* output = reinterpret_cast<uint64_t *>(calloc(5 * sizeof(uint64_t), 1));

    uint64_t* a_step_T = reinterpret_cast<uint64_t *>(a);

    auto dangling = cyclone::append_bitsets(output, 0, a_step_T, 8 * 9 * sizeof(char));
    auto final_dangling = cyclone::append_bitsets(&output[1], dangling, b, 8 * 3 * sizeof(uint64_t));

    CHECK(dangling == expected_dangling);
    CHECK(final_dangling == 8);
    CHECK(output[0] == expected_0_uint64[0]);
    CHECK(output[1] == expected_1);
    CHECK(output[2] == expected_2);
    CHECK(output[3] == expected_3);
    CHECK(output[4] == expected_4);
    free(output);
  }

  TEST_CASE("Merging fully into tail works") {
    char a[9] = {char(128), char(255), 8, 16, 15, 7, 9, 0, 1};
    char b[2] = {31, char(128)};

    size_t expected_dangling = 8;
    size_t expected_free_tail = 64 - 8;

    char expected_0[8] = {char(128), char(255), 8, 16, 15, 7, 9, 0};
    char expected_1[8] = {1  ,  31, char(128),  0,  0, 0, 0, 0};
    uint64_t * expected_0_uint64 = reinterpret_cast<uint64_t *>(expected_0);
    uint64_t * expected_1_uint64 = reinterpret_cast<uint64_t *>(expected_1);

    uint64_t* output = reinterpret_cast<uint64_t *>(calloc(2 * sizeof(uint64_t), 1));

    uint64_t* a_step_T = reinterpret_cast<uint64_t *>(a);
    uint64_t* b_step_T = reinterpret_cast<uint64_t *>(b);

    auto dangling = cyclone::append_bitsets(output, 0, a_step_T, 8 * 9 * sizeof(char));
    auto final_dangling = cyclone::append_bitsets(&output[1], dangling, b_step_T, 8 * 2 * sizeof(char));

    CHECK(dangling == expected_dangling);
    CHECK(final_dangling == 24);
    CHECK(output[0] == expected_0_uint64[0]);
    CHECK(output[1] == expected_1_uint64[0]);
    free(output);
  }

  TEST_CASE("Merging fully into tail works even when unaligned") {
    char a[9] = {char(128), char(255), 8, 16, 15, 7, 9, 0, 1};
    char b[2] = {31, 1};

    size_t expected_dangling = 1;
    size_t expected_free_tail = 64 - 8;

    char expected_0[8] = {char(128), char(255), 8, 16, 15, 7, 9, 0};
    // 63 = 31 << 1 + 1; 2 = 1 << 1
    char expected_1[8] = {63,  2,  0,  0, 0, 0, 0};
    uint64_t * expected_0_uint64 = reinterpret_cast<uint64_t *>(expected_0);
    uint64_t * expected_1_uint64 = reinterpret_cast<uint64_t *>(expected_1);

    uint64_t* output = reinterpret_cast<uint64_t *>(calloc(2 * sizeof(uint64_t), 1));

    uint64_t* a_step_T = reinterpret_cast<uint64_t *>(a);
    uint64_t* b_step_T = reinterpret_cast<uint64_t *>(b);

    auto dangling = cyclone::append_bitsets(output, 0, a_step_T, 8 * 8 * sizeof(char) + 1);
    auto final_dangling = cyclone::append_bitsets(&output[1], dangling, b_step_T, 8 * 1 * sizeof(char) + 1);

    std::cout << "output[0]  = " << std::bitset<64>(output[0]) << std::endl;
    std::cout << "expected_0 = " << std::bitset<64>(expected_0_uint64[0]) << std::endl;
    std::cout << "output[1]  = " << std::bitset<64>(output[1]) << std::endl;
    std::cout << "expected_1 = " << std::bitset<64>(expected_1_uint64[0]) << std::endl;

    CHECK(dangling == expected_dangling);
    CHECK(final_dangling == 10);
    CHECK(output[0] == expected_0_uint64[0]);
    CHECK(output[1] == expected_1_uint64[0]);
    free(output);
  }

  TEST_CASE("Merging into tail works after several big steps landing on full byte") {
    uint64_t a[3] = {
      0b0000000000000000000000000000000000000000000000000000000001000000,
      0b0000000000000000000000000000000000000000000000000000000010000000,
      0b0000000000000000000000000000000000000000000000000000000011111111
//              ^ valid until here
    };

    uint64_t b[3] = {
        0b1110000000000000000000000000000000000000000000000000000001100001,
        0b0000000000000000000000000000000000000000000000000000000011100001,
        0b0000000000000000000000000000000000000000000000000000000000111101
//                                                                  ^ valid until here
    };

    uint64_t expected[5] = {
        0b0000000000000000000000000000000000000000000000000000000001000000,
        0b0000000000000000000000000000000000000000000000000000000010000000,
        0b1000010000000000000000000000000000000000000000000000000011111111,
//    from b[0] ^ from a[3]
        0b1000011110000000000000000000000000000000000000000000000000000001,
//    from b[1] ^ from b[0]
        0b1111010000000000000000000000000000000000000000000000000000000011
//    from b[2] ^ from b[1]
  };

    uint64_t* output = static_cast<uint64_t *>(malloc(5 * sizeof(uint64_t)));

    auto dangling = cyclone::append_bitsets(output, 0, a, 2 * 64 + 58);
    auto final_dangling = cyclone::append_bitsets(&output[2], dangling, b, 134);

    //std::cout << "output[0]  = " << std::bitset<64>(output[0]) << std::endl;
    //std::cout << "expected_0 = " << std::bitset<64>(expected[0]) << std::endl;
    //std::cout << "output[1]  = " << std::bitset<64>(output[1]) << std::endl;
    //std::cout << "expected_1 = " << std::bitset<64>(expected[1]) << std::endl;
    //std::cout << "output[2]  = " << std::bitset<64>(output[2]) << std::endl;
    //std::cout << "expected_2 = " << std::bitset<64>(expected[2]) << std::endl;
    //std::cout << "output[3]  = " << std::bitset<64>(output[3]) << std::endl;
    //std::cout << "expected_3 = " << std::bitset<64>(expected[3]) << std::endl;
    //std::cout << "output[4]  = " << std::bitset<64>(output[4]) << std::endl;
    //std::cout << "expected_4 = " << std::bitset<64>(expected[4]) << std::endl;

    CHECK(dangling == 58);
    CHECK(final_dangling == 0);
    CHECK(output[0] == expected[0]);
    CHECK(output[1] == expected[1]);
    CHECK(output[2] == expected[2]);
    CHECK(output[3] == expected[3]);
    CHECK(output[4] == expected[4]);
    free(output);

    // TODO: Add actual checks on the last elements
  }

  TEST_CASE("Merging into tail works after several big steps landing on non-full byte") {
    uint64_t a[3] = {
        0b0000000000000000000000000000000000000000000000000000000001000000,
        0b0000000000000000000000000000000000000000000000000000000010000000,
        0b0000000000000000000000000000000000000000000000000000000011111111
//              ^ valid until here
    };
    uint64_t b[3] = {
        0b1110000000000000000000000000000000000000000000000000000001100001,
        0b0000000000000000000000000000000000000000000000000000000011100001,
        0b0000000000000000000000000000000000000000000000000000000000001101
//                                                                    ^ valid until here
    };

    uint64_t expected[5] = {
        0b0000000000000000000000000000000000000000000000000000000001000000,
        0b0000000000000000000000000000000000000000000000000000000010000000,
        0b1000010000000000000000000000000000000000000000000000000011111111,
//    from b[0] ^ from a[3]
        0b1000011110000000000000000000000000000000000000000000000000000001,
//    from b[1] ^ from b[0]
//        vv empty
        0b0011010000000000000000000000000000000000000000000000000000000011
//    from b[2] ^ from b[1]
    };

    uint64_t* output = static_cast<uint64_t *>(malloc(5 * sizeof(uint64_t)));

    auto dangling = cyclone::append_bitsets(output, 0, a, 2 * 64 + 58);
    auto final_dangling = cyclone::append_bitsets(&output[2], dangling, b, 132);

    //std::cout << "output[0]  = " << std::bitset<64>(output[0]) << std::endl;
    //std::cout << "expected_0 = " << std::bitset<64>(expected[0]) << std::endl;
    //std::cout << "output[1]  = " << std::bitset<64>(output[1]) << std::endl;
    //std::cout << "expected_1 = " << std::bitset<64>(expected[1]) << std::endl;
    //std::cout << "output[2]  = " << std::bitset<64>(output[2]) << std::endl;
    //std::cout << "expected_2 = " << std::bitset<64>(expected[2]) << std::endl;
    //std::cout << "output[3]  = " << std::bitset<64>(output[3]) << std::endl;
    //std::cout << "expected_3 = " << std::bitset<64>(expected[3]) << std::endl;
    //std::cout << "output[4]  = " << std::bitset<64>(output[4]) << std::endl;
    //std::cout << "expected_4 = " << std::bitset<64>(expected[4]) << std::endl;

    CHECK(dangling == 58);
    CHECK(final_dangling == 62);
    CHECK(output[0] == expected[0]);
    CHECK(output[1] == expected[1]);
    CHECK(output[2] == expected[2]);
    CHECK(output[3] == expected[3]);
    CHECK(output[4] == expected[4]);
    free(output);
  }

  TEST_CASE("Merging into tail works after several small steps landing on full byte"){
    uint64_t inputs[3] = {
      0b0000000000000000000000000000000000000000000000000110011000110010,
//                                                      ^ valid until here
      0b0000000011001001011001001000100101001000011011101000001100101000,
//              ^ valid until here
      0b0000011010100101101110010010101111000011010000000001100100111001
//           ^ valid until here
    };

    uint64_t lengths[3] = {16, 56, 59};

    const auto vbytes = sizeof(uint64_t) * frovedis::ceil_div(lengths[0] + lengths[1] + lengths[2], size_t(64));
    uint64_t* output = static_cast<uint64_t *>(calloc(vbytes, 1));

    auto d1 = cyclone::append_bitsets(&output[0],  0, &inputs[0], lengths[0]);
    auto d2 = cyclone::append_bitsets(&output[0], d1, &inputs[1], lengths[1]);
    auto d3 = cyclone::append_bitsets(&output[1], d2, &inputs[2], lengths[2]);

    auto expected_d1 = lengths[0] % 64;
    auto expected_d2 = (lengths[0] + lengths[1]) % 64;
    auto expected_d3 = (lengths[0] + lengths[1] + lengths[2]) % 64;

    uint64_t expected[3] = {
      0b0110010010001001010010000110111010000011001010000110011000110010,
//                                 part of inputs[1]    ^ inputs[0]
      0b1010010110111001001010111100001101000000000110010011100111001001,
//                                 part of inputs[2]    ^ part of inputs[1]
      0b0000000000000000000000000000000000000000000000000000000000000110
//                                              empty                ^ part of inputs[2]
    };

    CHECK(d1 == expected_d1);
    CHECK(d2 == expected_d2);
    CHECK(d3 == expected_d3);

    CHECK(output[0] == expected[0]);
    CHECK(output[1] == expected[1]);
    CHECK(output[2] == expected[2]);
  }
}
