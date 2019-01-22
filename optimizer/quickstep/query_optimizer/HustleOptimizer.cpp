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
  if (sql.empty()) {
    pplan =
        physical_generator.generatePlan(
            logical_generator.hustleGeneratePlan(*test_database_loader_.catalog_database(),
                                                 syntax_tree),
            test_database_loader_.catalog_database());
  } else {
    quickstep::SqlParserWrapper sql_parser_;
    auto query = new std::string(sql);
    sql_parser_.feedNextBuffer(query);
    quickstep::ParseResult result = sql_parser_.getNextStatement();
    if (result.condition == quickstep::ParseResult::kError) {
      printf("%s", result.error_message.c_str());
      return "";
    }
    const quickstep::ParseStatement
        &parse_statement = *result.parsed_statement;
    try {
      pplan =
          physical_generator.generatePlan(
              logical_generator.generatePlan(*test_database_loader_.catalog_database(),
                                             parse_statement),
              test_database_loader_.catalog_database());
    } catch (const quickstep::SqlError &sql_error) {
      printf("%s", sql_error.formatMessage(sql).c_str());
      return "";
    }
  }
  return pplan->jsonString();
}
