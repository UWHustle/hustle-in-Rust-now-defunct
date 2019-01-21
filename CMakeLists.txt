cmake_minimum_required(VERSION 3.5)

project(hustle)

set(CMAKE_CXX_STANDARD 14)

execute_process(COMMAND init.sh)

include_directories(${PROJECT_SOURCE_DIR})

file(COPY ${CMAKE_SOURCE_DIR}/test-data DESTINATION ${CMAKE_BINARY_DIR})

enable_testing()

add_subdirectory(execution)
add_subdirectory(optimizer)
add_subdirectory(parser)
add_subdirectory(cli)

set_target_properties(cli PROPERTIES OUTPUT_NAME ${PROJECT_NAME} RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
