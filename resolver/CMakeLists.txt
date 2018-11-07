# Generate the library: optimizer
add_library(resolver Resolver.cpp Resolver.hpp)
target_link_libraries(resolver optimizer)
target_link_libraries(resolver catalog)