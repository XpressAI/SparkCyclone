#include "cyclone/util/memory.hpp"

#include <cstdlib>

extern "C" int cyclone_alloc(size_t size, uintptr_t* out) {
  out[0] = reinterpret_cast<uintptr_t>(std::malloc(size));
  return 0;
}

extern "C" int cyclone_free(uintptr_t* addresses, size_t count) {
  for (size_t i = 0; i < count; i++) {
    std::free(reinterpret_cast<void*>(addresses[i]));
  }
  return 0;
}
