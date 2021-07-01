cmake_minimum_required(VERSION 3.6)
project(HelloWorld C)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
add_library(aurora4spark SHARED aurora4spark.cpp)
