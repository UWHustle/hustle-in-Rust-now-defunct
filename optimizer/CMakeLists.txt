add_library(
        optimizer
        example.hpp example.cpp
)

target_link_libraries(optimizer ${EXECUTION})
add_dependencies(optimizer execution)
