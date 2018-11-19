add_executable(parser_test parser_test.c)
target_link_libraries(parser_test parser)
add_test(parser_test parser_test)