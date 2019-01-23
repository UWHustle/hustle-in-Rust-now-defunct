cmake_minimum_required(VERSION 3.5)

project(hustle)

set(CMAKE_CXX_STANDARD 14)

# Update git submodules
find_package(Git QUIET)
if(GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
    message(STATUS "Updating git submodules")
    execute_process(
            COMMAND ${GIT_EXECUTABLE} submodule update --init
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            RESULT_VARIABLE GIT_SUBMOD_RESULT)
    if(NOT GIT_SUBMOD_RESULT EQUAL "0")
        message(FATAL_ERROR "git submodule update --init failed with ${GIT_SUBMOD_RESULT}")
    endif()
endif()

include_directories(${PROJECT_SOURCE_DIR})

file(COPY ${CMAKE_SOURCE_DIR}/test-data DESTINATION ${CMAKE_BINARY_DIR})

enable_testing()

add_subdirectory(execution)
add_subdirectory(optimizer)
add_subdirectory(parser)
add_subdirectory(cli)

set_target_properties(cli PROPERTIES OUTPUT_NAME ${PROJECT_NAME} RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
