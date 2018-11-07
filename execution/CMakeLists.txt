# Generate the library: optimizer
add_library(execution Execution.cpp Execution.hpp)
target_link_libraries(execution catalog)