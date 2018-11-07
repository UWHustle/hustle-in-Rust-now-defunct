# Generate the library: optimizer
add_library(optimizer Optimizer.cpp Optimizer.hpp)
target_link_libraries(optimizer execution)
target_link_libraries(optimizer catalog)