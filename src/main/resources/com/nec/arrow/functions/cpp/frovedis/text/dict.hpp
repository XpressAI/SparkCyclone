#ifndef DICT_HPP
#define DICT_HPP

#if defined(__ve__) || defined(_SX)
#define DICT_VLEN 256
#else
//#define DICT_VLEN 1
#define DICT_VLEN 4
#endif

#include "../core/radix_sort.hpp"
#include "../core/set_operations.hpp"
#include "words.hpp"
#include <limits>

namespace frovedis {

void unique_words(const std::vector<uint64_t>& compressed,
                  const std::vector<size_t>& compressed_lens,
                  const std::vector<size_t>& compressed_lens_num,
                  std::vector<uint64_t>& unique_compressed,
                  std::vector<size_t>& unique_compressed_lens,
                  std::vector<size_t>& unique_compressed_lens_num);

void compress_words(const std::vector<int>& v,
                    const std::vector<size_t>& starts,
                    const std::vector<size_t>& lens,
                    std::vector<uint64_t>& compressed,
                    std::vector<size_t>& compressed_lens,
                    std::vector<size_t>& compressed_lens_num,
                    std::vector<size_t>& order);

void print_compressed_words(const std::vector<uint64_t>& compressed,
                            const std::vector<size_t>& compressed_lens,
                            const std::vector<size_t>& compressed_lens_num,
                            const std::vector<size_t>& order);

void print_compressed_words(const std::vector<uint64_t>& compressed,
                            const std::vector<size_t>& compressed_lens,
                            const std::vector<size_t>& compressed_lens_num,
                            bool print_idx = false);

words decompress_compressed_words(const std::vector<uint64_t>& cwords,
                                  const std::vector<size_t>& lens,
                                  const std::vector<size_t>& lens_num,
                                  const std::vector<size_t>& order);

void lexical_sort_compressed_words(const std::vector<uint64_t>& cwords,
                                   const std::vector<size_t>& lens,
                                   const std::vector<size_t>& lens_num,
                                   std::vector<size_t>& order);

struct compressed_words {
  std::vector<uint64_t> cwords; // compressed
  std::vector<size_t> lens;
  std::vector<size_t> lens_num;
  std::vector<size_t> order;

  void clear() { // to free memory
    std::vector<uint64_t> cwords_tmp; cwords.swap(cwords_tmp);
    std::vector<size_t> lens_tmp; lens.swap(lens_tmp);
    std::vector<size_t> lens_num_tmp; lens_num.swap(lens_num_tmp);
    std::vector<size_t> order_tmp; order.swap(order_tmp);
  }
  struct words decompress() const;
  void lexical_sort();
  compressed_words extract(const std::vector<size_t>& idx) const;
  void print() const {print_compressed_words(cwords, lens, lens_num, order);}
  size_t num_words() const {return order.size();}

};

compressed_words merge_compressed_words(const compressed_words& a,
                                        const compressed_words& b);

compressed_words
merge_multi_compressed_words(std::vector<compressed_words>&);

compressed_words make_compressed_words(const words&);

struct dict {
  std::vector<uint64_t> cwords; // compressed
  std::vector<size_t> lens;
  std::vector<size_t> lens_num;

  void print() const {print_compressed_words(cwords, lens, lens_num, true);}
  std::vector<size_t> lookup(const compressed_words& cw) const;
  struct words index_to_words(const std::vector<size_t>& idx) const;
  struct words decompress() const;

  size_t num_words() const;

};

dict make_dict(const words& ws);
dict make_dict(const compressed_words& comp);
// same as above: to use from map
// pointer to overloaded function becomes ambiguous
dict make_dict_from_words(const words& ws);
dict make_dict_from_compressed(const compressed_words& comp);

dict merge_dict(const dict& a, const dict& b);

}
#endif
