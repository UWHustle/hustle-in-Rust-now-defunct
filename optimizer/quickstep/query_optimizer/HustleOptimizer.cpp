#include "query_optimizer/HustleOptimizer.hpp"
#include "query_optimizer/LogicalGenerator.hpp"
#include "query_optimizer/OptimizerContext.hpp"
#include "query_optimizer/PhysicalGenerator.hpp"
#include "utility/Macros.hpp"
#include "parser/SqlParserWrapper.hpp"
#include "query_optimizer/tests/TestDatabaseLoader.hpp"
#include "utility/SqlError.hpp"

std::string hustle_optimize(const std::shared_ptr<ParseNode> &syntax_tree,
                            const std::string &sql) {

  quickstep::optimizer::OptimizerContext optimizer_context;

  quickstep::optimizer::TestDatabaseLoader test_database_loader_;
  test_database_loader_.createHustleTestRelation(false /* allow_vchar */);
  test_database_loader_.loadHustleTestRelation();
  test_database_loader_.createHustleJoinRelations();

  quickstep::optimizer::LogicalGenerator logical_generator(&optimizer_context);
  quickstep::optimizer::PhysicalGenerator
      physical_generator(&optimizer_context);

  quickstep::optimizer::physical::PhysicalPtr pplan;
  try {
    quickstep::optimizer::logical::LogicalPtr lplan;
    // If the sql is empty the parser/resolver of Hustle is used.
    if (sql.empty()) {
      lplan = logical_generator.hustleGeneratePlan(
          *test_database_loader_.catalog_database(), syntax_tree);
    } else {  // Parse the query using the quickstep parser
      quickstep::SqlParserWrapper sql_parser_;
      auto query = new std::string(sql);
      sql_parser_.feedNextBuffer(query);
      quickstep::ParseResult result = sql_parser_.getNextStatement();
      if (result.condition == quickstep::ParseResult::kError) {
        printf( "%s", result.error_message.c_str());
        return "";
      }
      const quickstep::ParseStatement
          &parse_statement = *result.parsed_statement;
      // Convert the query to the logical plan using quickstep's optimizer
      lplan = logical_generator.generatePlan(
          *test_database_loader_.catalog_database(), parse_statement);
    }

    std::cout<< "Logical Plan: " << lplan->jsonString() << std::endl;
    std::cout << " --------------------- " << std::endl;
    pplan =
        physical_generator.generatePlan(
            lplan, test_database_loader_.catalog_database());

    std::cout<< "Physical Plan: " << pplan->jsonString() << std::endl;
  } catch (const quickstep::SqlError &sql_error) {
    printf("%s", sql_error.formatMessage(sql).c_str());
    return "";
  }

  return pplan->jsonString();
}