cmake_minimum_required(VERSION 3.12)

project(hustle_parser C)

include_directories(${PROJECT_SOURCE_DIR})

add_library(dynamic_array dynamic_array.h dynamic_array.c)
add_library(parse_node parse_node.h parse_node.c)
add_library(string_builder string_builder.h string_builder.c)
add_library(stringify stringify.h stringify.c)

find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)
bison_target(hustle_bison parser.y ${PROJECT_SOURCE_DIR}/parser.c)
flex_target(hustle_flex lexer.l ${PROJECT_SOURCE_DIR}/lexer.c)
add_flex_bison_dependency(hustle_flex hustle_bison)

add_library(parser_wrapper parser_wrapper.h parser_wrapper.c ${BISON_hustle_bison_OUTPUTS} ${FLEX_hustle_flex_OUTPUTS})

add_executable(main main.c ${BISON_hustle_bison_OUTPUTS} ${FLEX_hustle_flex_OUTPUTS})
target_link_libraries(main dynamic_array parse_node string_builder stringify parser_wrapper)