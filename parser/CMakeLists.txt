find_package(BISON 3.0 REQUIRED)
find_package(FLEX REQUIRED)
bison_target(hustle_bison parser.y ${CMAKE_CURRENT_SOURCE_DIR}/parser.c)
flex_target(hustle_flex lexer.l ${CMAKE_CURRENT_SOURCE_DIR}/lexer.c)
add_flex_bison_dependency(hustle_flex hustle_bison)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99")

# Library target (build with --target parser)
add_library(
        parser
        parse_node.h parse_node.c
        parser_wrapper.h parser_wrapper.c
        ${BISON_hustle_bison_OUTPUTS}
        ${FLEX_hustle_flex_OUTPUTS}
        utility/dynamic_array.h utility/dynamic_array.c
        utility/string_builder.h utility/string_builder.c
        utility/stringify.h utility/stringify.c
)

# Testing main target (build with --target parser_test)
# add_executable(parser_test test/parser_test.c)
# target_link_libraries(parser_test parser)
