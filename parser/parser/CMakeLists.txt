cmake_minimum_required(VERSION 3.5)

project(hustle_parser C)

include_directories(${PROJECT_SOURCE_DIR})

find_package(BISON 3.0 REQUIRED)
find_package(FLEX REQUIRED)

bison_target(
        hustle_bison
        ${CMAKE_CURRENT_SOURCE_DIR}/parser.y
        ${CMAKE_CURRENT_SOURCE_DIR}/parser.c)

flex_target(
        hustle_flex
        ${CMAKE_CURRENT_SOURCE_DIR}/lexer.l
        ${CMAKE_CURRENT_SOURCE_DIR}/lexer.c)

add_flex_bison_dependency(hustle_flex hustle_bison)

add_library(
        hustle_parser STATIC
        parser_wrapper.h parser_wrapper.c
        parse_node.h parse_node.c
        ${BISON_hustle_bison_OUTPUTS}
        ${FLEX_hustle_flex_OUTPUTS}
        util/dynamic_array.h util/dynamic_array.c
        util/string_builder.h util/string_builder.c
        util/stringify.h util/stringify.c
)

install(TARGETS hustle_parser DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
