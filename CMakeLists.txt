cmake_minimum_required(VERSION 3.5)

project(hustle)

include_directories(${PROJECT_SOURCE_DIR})

file(COPY ${CMAKE_SOURCE_DIR}/test-data DESTINATION ${CMAKE_BINARY_DIR})

add_subdirectory(cli)
add_subdirectory(parser)
add_subdirectory(optimizer)
add_subdirectory(execution)

add_executable(hustle cli/cli.c)

target_link_libraries(hustle parser optimizer ${EXECUTION})
add_dependencies(hustle execution)
