add_executable(select select.cpp)
target_link_libraries(select parser)
add_test(parser_test_select select)

add_executable(create create.cpp)
target_link_libraries(create parser)
add_test(parser_test_create create)
