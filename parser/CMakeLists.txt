find_package(BISON 3.0 REQUIRED)
find_package(FLEX REQUIRED)
bison_target(PARSER parser.y ${CMAKE_CURRENT_SOURCE_DIR}/parser.cpp)
flex_target(LEXER lexer.l ${CMAKE_CURRENT_SOURCE_DIR}/lexer.cpp)
add_flex_bison_dependency(LEXER PARSER)

add_library(
        parser
        ParserDriver.h ParserDriver.cpp
        ParseNode.h ParseNode.cpp
        SelectNode.h SelectNode.cpp
        ReferenceNode.h ReferenceNode.cpp
        FunctionNode.h FunctionNode.cpp
        OperatorNode.h OperatorNode.cpp
        ${BISON_PARSER_OUTPUTS}
        ${FLEX_LEXER_OUTPUTS}
)

target_link_libraries(parser optimizer)

#add_subdirectory(test)
