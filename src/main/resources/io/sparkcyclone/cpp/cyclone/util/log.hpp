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

#include "cyclone/util/io.hpp"
#include "cyclone/util/time.hpp"
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <pthread.h>
#include <unistd.h>

namespace cyclone::log {
  /*
    Define enum with string values mapping using X macros:
      https://stackoverflow.com/questions/11714325/how-to-get-enum-item-name-from-its-value
  */
  #define LOG_LEVELS  \
  X(TRACE,  "TRACE")  \
  X(DEBUG,  "DEBUG")  \
  X(INFO,   "INFO")   \
  X(WARN,   "WARN")   \
  X(ERROR,  "ERROR")  \
  X(FATAL,  "FATAL")

  #define X(level, name) level,
  enum LogLevel : size_t {
    LOG_LEVELS
  };
  #undef X

  #define X(level, name) name,
  static char const *LogLevelName[] = {
    LOG_LEVELS
  };
  #undef X

  // Fetch the log level from the system environment
  inline LogLevel log_level() {
    static LogLevel level = ({
      const char* level_p = std::getenv("LIBCYCLONE_LOG_LEVEL");
      const auto  level_s = level_p ? std::string(level_p) : "";

      // Set log level to INFO by default
      auto _level = LogLevel::INFO;
      for (auto i = 0; i < size_t(LogLevel::FATAL); i++) {
        if (level_s == LogLevelName[i]) {
          _level = static_cast<LogLevel>(i);
          break;
        }
      }
      _level;
    });

    return level;
  }

  class NullStream : public std::ostream {
  public:
    NullStream() : std::ostream(nullptr) {}
    NullStream(const NullStream &) : std::ostream(nullptr) {}
  };

  template <class T>
  const NullStream &operator<<(NullStream &&os, const T &value) {
    return os;
  }

  /*
    Declare a singleton mutex to control the logging.  Mark as inline since it
    is declared in a header file:
      https://stackoverflow.com/questions/56876624/inline-stdmutex-in-header-file
  */
  inline std::mutex log_mutex;

  // Declare a singleton null stream
  inline NullStream null_stream;

  template<typename ... T>
  inline void log(const LogLevel level, const char *file, const int32_t line, const std::string_view &fmt, T const & ...args) {
    if (level >= log_level()) {
      std::lock_guard<std::mutex> lock(log_mutex);
      std::cout
        << cyclone::io::format("[%s] [%10d] [%10d] [%5s] [%s:%d] ", cyclone::time::utc(), getpid(), pthread_self(), LogLevelName[level], file, line)
        << cyclone::io::format(fmt, args...)
        << std::endl;
    }
  }

  #define trace(fmt, ...) log(cyclone::log::LogLevel::TRACE,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)
  #define debug(fmt, ...) log(cyclone::log::LogLevel::DEBUG,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)
  #define info(fmt, ...)  log(cyclone::log::LogLevel::INFO,   __FILE__, __LINE__, fmt, ##__VA_ARGS__)
  #define warn(fmt, ...)  log(cyclone::log::LogLevel::WARN,   __FILE__, __LINE__, fmt, ##__VA_ARGS__)
  #define error(fmt, ...) log(cyclone::log::LogLevel::ERROR,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)
  #define fatal(fmt, ...) log(cyclone::log::LogLevel::FATAL,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)

  inline std::ostream& slog(const LogLevel level, const char *file, const int32_t line) {
    return ((level < log_level()) ? null_stream : std::cout)
      << cyclone::io::format("[%s] [%10d] [%10d] [%5s] [%s:%d] ", cyclone::time::utc(), getpid(), pthread_self(), LogLevelName[level], file, line);
  }

  #define strace slog(cyclone::log::LogLevel::TRACE,  __FILE__, __LINE__)
  #define sdebug slog(cyclone::log::LogLevel::DEBUG,  __FILE__, __LINE__)
  #define sinfo  slog(cyclone::log::LogLevel::INFO,   __FILE__, __LINE__)
  #define swarn  slog(cyclone::log::LogLevel::WARN,   __FILE__, __LINE__)
  #define serror slog(cyclone::log::LogLevel::ERROR,  __FILE__, __LINE__)
  #define sfatal slog(cyclone::log::LogLevel::FATAL,  __FILE__, __LINE__)
}
