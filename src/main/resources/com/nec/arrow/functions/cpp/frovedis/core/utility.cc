#include "utility.hpp"
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <stdio.h>

namespace frovedis {

bool is_bigendian() {
  int i = 1;
  if(*((char*)&i)) return false;
  else return true;
}

}