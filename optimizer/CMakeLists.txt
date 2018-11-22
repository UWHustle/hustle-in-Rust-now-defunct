add_subdirectory(quickstep)

add_library(hopt example.hpp example.cpp)

target_link_libraries(hopt ${EXECUTION} hustle_optimizer
        quickstep_queryoptimizer_QueryHandle
        quickstep_queryoptimizer_ExecutionGenerator
        quickstep_queryoptimizer_LIPFilterGenerator
        quickstep_queryoptimizer_LogicalGenerator
        quickstep_queryoptimizer_LogicalToPhysicalMapper
        quickstep_queryoptimizer_Optimizer
        quickstep_queryoptimizer_OptimizerContext
        quickstep_queryoptimizer_OptimizerTree
        quickstep_queryoptimizer_PhysicalGenerator
        quickstep_queryoptimizer_QueryPlan
        quickstep_queryoptimizer_QueryProcessor
        quickstep_queryoptimizer_Validator
        quickstep_parser_SqlParserWrapper
        quickstep_queryoptimizer_costmodel
        quickstep_queryoptimizer_expressions
        quickstep_queryoptimizer_tests_TestDatabaseLoader

        quickstep_queryoptimizer_logical
        quickstep_queryoptimizer_physical
        quickstep_queryoptimizer_resolver
        quickstep_queryoptimizer_rules
        quickstep_queryoptimizer_strategy)

add_dependencies(hopt execution hustle_optimizer)



