cmake_minimum_required(VERSION 3.6)
project(HelloWorld C)
find_package(OpenMP)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
# disabling: it's crashing on Windows; on Linux the program is returning wrong result
# the same code on VE works just fine though.
# set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
add_library(aurora4spark SHARED aurora4spark.c)
set_property(TARGET aurora4spark PROPERTY C_STANDARD 99)
