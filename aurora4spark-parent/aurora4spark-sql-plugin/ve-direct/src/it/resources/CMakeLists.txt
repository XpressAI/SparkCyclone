cmake_minimum_required(VERSION 3.6)

project(HelloWorld)

set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)

add_library(sortstuff SHARED sort-stuff-lib.c)
