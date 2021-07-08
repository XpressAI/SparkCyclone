#include "utility.hpp"
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <stdio.h>

namespace frovedis {

#ifndef __UTILITY__
bool is_bigendian() {
  int i = 1;
  if(*((char*)&i)) return false;
  else return true;
}

#define __UTILITY__ 1
#endif

}