find_package(BISON 3.2.0 REQUIRED)
find_package(FLEX REQUIRED)
bison_target(hustle_bison parser.y ${CMAKE_CURRENT_SOURCE_DIR}/parser.c)
flex_target(hustle_flex lexer.l ${CMAKE_CURRENT_SOURCE_DIR}/lexer.c)
add_flex_bison_dependency(hustle_flex hustle_bison)

add_library(
        parser
        dynamic_array.h dynamic_array.c
        parse_node.h parse_node.c
        string_builder.h string_builder.c
        stringify.h stringify.c
        parser_wrapper.h parser_wrapper.c
        ${BISON_hustle_bison_OUTPUTS}
        ${FLEX_hustle_flex_OUTPUTS}
)