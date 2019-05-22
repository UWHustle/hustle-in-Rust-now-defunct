cmake_minimum_required(VERSION 3.12)

set(CMAKE_CXX_STANDARD 14)

add_subdirectory(quickstep)
add_executable(optimizer optimizer_wrapper.cpp optimizer_wrapper.h)

target_link_libraries(
        optimizer
        hustle_optimizer
        quickstep_queryoptimizer_tests_TestDatabaseLoader
        quickstep_parser_SqlParserWrapper)

add_dependencies(optimizer hustle_optimizer)

install(TARGETS optimizer DESTINATION ${CMAKE_BINARY_DIR})
