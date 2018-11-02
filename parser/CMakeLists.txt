cmake_minimum_required(VERSION 3.12)

project(hustle_parser C)

include_directories(${PROJECT_SOURCE_DIR})

add_library(linked_list linked_list.h linked_list.c)
add_library(parse_node parse_node.h parse_node.c)

add_executable(main main.c)
target_link_libraries(main parse_node linked_list)