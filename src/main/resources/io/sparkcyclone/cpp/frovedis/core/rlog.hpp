#ifndef RLOG_HPP
#define RLOG_HPP

#include "log.hpp"

using frovedis::TRACE;
using frovedis::DEBUG;
using frovedis::INFO;
using frovedis::WARNING;
using frovedis::ERROR;
using frovedis::FATAL;

#define RLOG(level) LOG(level)

#endif
