add_executable(parser_test parser_test.cpp)
target_link_libraries(parser_test parser)
add_test(parser_test parser_test)
