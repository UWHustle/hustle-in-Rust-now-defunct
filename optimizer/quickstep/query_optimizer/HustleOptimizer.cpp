#include "query_optimizer/HustleOptimizer.hpp"
#include "query_optimizer/LogicalGenerator.hpp"
#include "query_optimizer/OptimizerContext.hpp"
#include "query_optimizer/PhysicalGenerator.hpp"
#include "utility/Macros.hpp"
#include "parser/SqlParserWrapper.hpp"
#include "query_optimizer/tests/TestDatabaseLoader.hpp"


quickstep::optimizer::physical::PhysicalPtr hustle_getPhysicalPlan(const quickstep::ParseStatement &parse_statement,
                                   quickstep::CatalogDatabase *catalog_database,
                                   quickstep::optimizer::OptimizerContext *optimizer_context) {
  quickstep::optimizer::LogicalGenerator logical_generator(optimizer_context);
  quickstep::optimizer::PhysicalGenerator physical_generator(optimizer_context);

  quickstep::optimizer::physical::PhysicalPtr physical_plan =
      physical_generator.generatePlan(
          logical_generator.generatePlan(*catalog_database, parse_statement),
          catalog_database);

  return physical_plan;
}

std::string hustle_optimize(char *input) {
  quickstep::SqlParserWrapper sql_parser_;
  std::string* query = new std::string(input);

  sql_parser_.feedNextBuffer(query);
  quickstep::ParseResult result = sql_parser_.getNextStatement();

  quickstep::optimizer::OptimizerContext optimizer_context;
  const quickstep::ParseStatement &parse_statement = *result.parsed_statement;

  quickstep::optimizer::TestDatabaseLoader test_database_loader_;

  test_database_loader_.createHustleTestRelation(false /* allow_vchar */);
  test_database_loader_.loadHustleTestRelation();
  test_database_loader_.createHustleJoinRelations();

  quickstep::optimizer::physical::PhysicalPtr pplan =
      hustle_getPhysicalPlan(parse_statement,
                             test_database_loader_.catalog_database(),
                             &optimizer_context);

  return pplan->jsonString();
}
