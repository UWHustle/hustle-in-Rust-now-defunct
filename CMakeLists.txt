cmake_minimum_required (VERSION 2.8.6)
project (HUSTLE)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")

include_directories(${PROJECT_SOURCE_DIR})

add_subdirectory(cli)
add_subdirectory(parser)
#add_subdirectory(resolver)
#add_subdirectory(optimizer)
#add_subdirectory(execution)
#add_subdirectory(catalog)

add_executable(hustle cli/cli.c)

#target_link_libraries(hustle resolver)
target_link_libraries(hustle parser)