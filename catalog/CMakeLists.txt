# Generate the library: optimizer
add_library(catalog Catalog.cpp Catalog.hpp)

add_test(NAME catalog_tests
         COMMAND cargo test
         WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})