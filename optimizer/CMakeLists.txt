add_subdirectory(quickstep)
add_library(optimizer optimizer_wrapper.hpp optimizer_wrapper.cpp)

target_link_libraries(optimizer
        ${EXECUTION}
        hustle_optimizer
        resolver
        quickstep_queryoptimizer_tests_TestDatabaseLoader
        quickstep_parser_SqlParserWrapper
        )

add_dependencies(optimizer execution hustle_optimizer)
