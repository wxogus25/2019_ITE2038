cmake_minimum_required(VERSION 3.15)
project(untitled2 C)

set(CMAKE_C_STANDARD 11)

add_executable(untitled2 main.c bpt.h bpt.c file_manager.h file_manager.c buffer_manager.h buffer_manager.c)