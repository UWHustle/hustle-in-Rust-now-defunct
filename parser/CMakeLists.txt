cmake_minimum_required(VERSION 3.12)

project(hustle_parser C)

include_directories(${PROJECT_SOURCE_DIR})

add_library(dynamic_array dynamic_array.h dynamic_array.c)
add_library(parse_node parse_node.h parse_node.c)
add_library(string_builder string_builder.h string_builder.c)
add_library(stringify stringify.h stringify.c)

add_executable(main main.c)
target_link_libraries(main dynamic_array parse_node string_builder stringify)