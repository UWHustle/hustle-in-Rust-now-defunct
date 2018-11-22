add_subdirectory(quickstep)

add_library(hopt example.hpp example.cpp)

target_link_libraries(hopt ${EXECUTION} hustle_optimizer)
add_dependencies(hopt execution hustle_optimizer)



