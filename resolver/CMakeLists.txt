set (CMAKE_CXX_STANDARD 11)

add_library(
        resolver
        resolver.h resolver.cpp
)

target_link_libraries(resolver optimizer)