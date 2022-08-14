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

namespace cyclone::tests {
  TEST_CASE("bitmask_to_matching_ids() works") {
    std::vector<size_t> bitmask = { 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1 };
    std::vector<size_t> expected = { 2, 7, 8, 9, 10, 13, 14 };
    CHECK(cyclone::bitmask_to_matching_ids(bitmask) == expected);
  }

  TEST_CASE("joining works on fully matched pairs") {
    std::vector<size_t> left = {1, 2, 3, 4, 5, 6};
    std::vector<size_t> right = {6, 3, 2, 4, 5, 1};
    std::vector<size_t> out_left;
    std::vector<size_t> out_right;

    std::vector<size_t> expected_left = {0, 1, 2, 3, 4, 5};
    std::vector<size_t> expected_right = {5, 2, 1, 3, 4, 0};

    cyclone::equi_join_indices(left, right, out_left, out_right);

    cyclone::io::print_vec("out_left", out_left);
    cyclone::io::print_vec("exp_left", expected_left);
    cyclone::io::print_vec("out_right", out_right);
    cyclone::io::print_vec("exp_right", expected_right);

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

    cyclone::io::print_vec("out_left", out_left);
    cyclone::io::print_vec("exp_left", expected_left);
    cyclone::io::print_vec("out_right", out_right);
    cyclone::io::print_vec("exp_right", expected_right);

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
     cyclone::append_bitsets(output, 8 * 2 * sizeof(uint64_t), b, 8 * 3 * sizeof(uint64_t));

     CHECK(output[0] == a[0]);
     CHECK(output[1] == a[1]);
     CHECK(output[2] == b[0]);
     CHECK(output[3] == b[1]);
     CHECK(output[4] == b[2]);
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

    cyclone::append_bitsets(output, 0, a, 2 * 64 + 58);
    cyclone::append_bitsets(output, 2 * 64 + 58, b, 134);

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

    CHECK(output[0] == expected[0]);
    CHECK(output[1] == expected[1]);
    CHECK(output[2] == expected[2]);
    CHECK(output[3] == expected[3]);
    CHECK(output[4] == expected[4]);
    free(output);
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

    cyclone::append_bitsets(output, 0, a, 2 * 64 + 58);
    cyclone::append_bitsets(output, 2 * 64 + 58, b, 132);

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

    const auto vbytes = sizeof(uint64_t) * frovedis::ceil_div(lengths[0] + lengths[1] + lengths[2], uint64_t(64));
    uint64_t* output = static_cast<uint64_t *>(calloc(vbytes, 1));

    cyclone::append_bitsets(output,  0, &inputs[0], lengths[0]);
    cyclone::append_bitsets(output, lengths[0], &inputs[1], lengths[1]);
    cyclone::append_bitsets(output, lengths[0] + lengths[1], &inputs[2], lengths[2]);

    uint64_t expected[3] = {
      0b0110010010001001010010000110111010000011001010000110011000110010,
//                                 part of inputs[1]    ^ inputs[0]
      0b1010010110111001001010111100001101000000000110010011100111001001,
//                                 part of inputs[2]    ^ part of inputs[1]
      0b0000000000000000000000000000000000000000000000000000000000000110
//                                              empty                ^ part of inputs[2]
    };

    CHECK(output[0] == expected[0]);
    CHECK(output[1] == expected[1]);
    CHECK(output[2] == expected[2]);
  }
}
