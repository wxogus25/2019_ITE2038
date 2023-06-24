cmake_minimum_required(VERSION 3.15)
project(join)

set(CMAKE_CXX_STANDARD 14)

add_executable(join main.cpp bpt.h bpt.cpp file_manager.h file_manager.cpp buffer_manager.h buffer_manager.cpp)