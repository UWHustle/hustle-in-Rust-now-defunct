include(ExternalProject)

ExternalProject_Add(
        execution
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
        BINARY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/target/debug
        BUILD_COMMAND cargo build
        INSTALL_COMMAND ""
)

find_library(
        EXECUTION
        execution
        PATHS ${CMAKE_CURRENT_SOURCE_DIR}/target/debug
)