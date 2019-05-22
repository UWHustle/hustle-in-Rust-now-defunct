#include "query_optimizer/HustleOptimizer.hpp"
#include "query_optimizer/LogicalGenerator.hpp"
#include "query_optimizer/OptimizerContext.hpp"
#include "query_optimizer/PhysicalGenerator.hpp"
#include "utility/Macros.hpp"
#include "parser/SqlParserWrapper.hpp"
#include "query_optimizer/tests/TestDatabaseLoader.hpp"
#include "utility/SqlError.hpp"
#include "types/TypeID.hpp"
#include "catalog/Catalog.hpp"
#include "catalog/Catalog.pb.h"

#include <fstream>
#include <memory>
#include <string>
#include <tuple>

namespace quickstep {

void OptimizerWrapper::readCatalog() {
  quickstep::serialization::Catalog catalog_proto;
  std::ifstream catalog_file(std::string(kCatalogPath).c_str());
  if (!catalog_file.good()) {
    throw UnableToReadCatalogE(kCatalogPath);
  }

  if (!catalog_proto.ParseFromIstream(&catalog_file)) {
    throw CatalogNotProtoE(kCatalogPath);
  }
  catalog_file.close();
  catalog_ = std::make_shared<Catalog>(catalog_proto);
}

void OptimizerWrapper::saveCatalog() {
  std::ofstream catalog_file(std::string(kCatalogPath).c_str());

  if (!catalog_->getProto().SerializeToOstream(&catalog_file)) {
    throw UnableToWriteCatalogE(kCatalogPath);
  }

  catalog_file.close();
}

void OptimizerWrapper::CreateDefaultCatalog() {

  catalog_->addDatabase(new quickstep::CatalogDatabase(nullptr /* parent */,
                                                      "default_db" /* name */,
                                                      0 /* id */));

//  quickstep::CatalogDatabase *catalog_database =
//      catalog_->getDatabaseByNameMutable("default_db");
//
//  std::vector<std::string> rel_names = {"t", "s"};
//  std::vector<std::vector<std::tuple<std::string, quickstep::TypeID, std::size_t>>>
//      rel_columns = {
//      {std::make_tuple("a", quickstep::kInt, 0), std::make_tuple("b", quickstep::kInt, 0)},
//      {std::make_tuple("c", quickstep::kInt, 0), std::make_tuple("d", quickstep::kVarChar, 10)}
//  };
//
//  for (std::size_t rel_idx = 0; rel_idx < rel_names.size(); ++rel_idx) {
//    std::unique_ptr<quickstep::CatalogRelation> relation(
//        new quickstep::CatalogRelation(catalog_database,
//                                       rel_names[rel_idx],
//                                       -1 /* id */,
//                                       true /* temporary */));
//
//    const std::vector<std::tuple<std::string, quickstep::TypeID, std::size_t>>
//        &columns = rel_columns[rel_idx];
//    int attr_id = -1;
//    for (std::size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
//        if (std::get<2>(columns[col_idx]) == 0) {
//            relation->addAttribute(new quickstep::CatalogAttribute(
//                    relation.get(),
//                    std::get<0>(columns[col_idx]),
//                    quickstep::TypeFactory::GetType(std::get<1>(columns[col_idx])),
//                    ++attr_id));
//        }
//        else {
//            relation->addAttribute(new quickstep::CatalogAttribute(
//                    relation.get(),
//                    std::get<0>(columns[col_idx]),
//                    quickstep::TypeFactory::GetType(std::get<1>(columns[col_idx]), std::get<2>(columns[col_idx])),
//                    ++attr_id));
//        }
//
//    }
//    catalog_database->addRelation(relation.release());
//  }
}

std::string OptimizerWrapper::hustle_optimize(const std::string &sql) {
  quickstep::optimizer::OptimizerContext optimizer_context;
  catalog_.reset();

  try {
    readCatalog();
  } catch (UnableToReadCatalogE &read_exception) {
    catalog_ = std::make_shared<Catalog>();
    CreateDefaultCatalog();
  }

  quickstep::CatalogDatabase *catalog_database =
      catalog_->getDatabaseByNameMutable("default_db");

  quickstep::optimizer::LogicalGenerator logical_generator(&optimizer_context);
  quickstep::optimizer::PhysicalGenerator
      physical_generator(&optimizer_context);

  quickstep::optimizer::physical::PhysicalPtr pplan;
  try {
        quickstep::optimizer::logical::LogicalPtr lplan;
        // Parse the query using the quickstep parser
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
      // Convert the query to the logical plan using quickstep's optimizer
      lplan = logical_generator.generatePlan(
          catalog_database, parse_statement, true /* husteMode */);

//    std::cout<< "Logical Plan: " << lplan->jsonString() << std::endl;
//    std::cout << " --------------------- " << std::endl;
    pplan =
        physical_generator.generatePlan(
            lplan, catalog_database);

//    std::cout<< "Physical Plan: " << pplan->jsonString() << std::endl;
  } catch (const quickstep::SqlError &sql_error) {
    printf("%s", sql_error.formatMessage(sql).c_str());
    return "";
  }

  saveCatalog();

  return pplan->jsonString();
}
}