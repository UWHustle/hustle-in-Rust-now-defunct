#include "query_optimizer/HustleOptimizer.hpp"
#include "query_optimizer/Optimizer.hpp"
#include "query_optimizer/ExecutionGenerator.hpp"
#include "query_optimizer/LogicalGenerator.hpp"
#include "query_optimizer/OptimizerContext.hpp"
#include "query_optimizer/resolver/Resolver.hpp"
#include "query_optimizer/PhysicalGenerator.hpp"
#include "utility/Macros.hpp"
#include "parser/SqlParserWrapper.hpp"
#include "query_optimizer/tests/TestDatabaseLoader.hpp"


//namespace quickstep {
//namespace optimizer {
////using namespace quickstep;
////using namespace quickstep::optimizer;
//
//
//}
//}
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

std::string hustle_optimize() {
  std::cout << "1 \n";
  quickstep::SqlParserWrapper sql_parser_;
  quickstep::optimizer::Optimizer optimizer_;
  std::string* query = new std::string("select a from t;");

  sql_parser_.feedNextBuffer(query);
  quickstep::ParseResult result = sql_parser_.getNextStatement();

  std::cout << "2 \n";
  quickstep::optimizer::OptimizerContext optimizer_context;
  const quickstep::ParseStatement &parse_statement = *result.parsed_statement;

  std::cout << "3 \n";

  quickstep::optimizer::TestDatabaseLoader test_database_loader_;

  std::cout << "3.1 \n";
  test_database_loader_.createTestRelation(false /* allow_vchar */);
  std::cout << "3.2 \n";
  test_database_loader_.loadTestRelation();

  std::cout << "4 \n";
  quickstep::optimizer::physical::PhysicalPtr pplan =
      hustle_getPhysicalPlan(parse_statement,
                             test_database_loader_.catalog_database(),
                             &optimizer_context);
  std::cout << "5: " << pplan->toString() << std::endl;


  return pplan->toString();
}

//}  // namespace optimizer
//}  // namespace quickstep

//
