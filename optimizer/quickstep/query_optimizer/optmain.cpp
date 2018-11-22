/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/

#include "query_optimizer/Optimizer.hpp"
#include "parser/SqlParserWrapper.hpp"
#include "HustleOptimizer.hpp"

#include "query_optimizer/LogicalGenerator.hpp"
#include "query_optimizer/OptimizerContext.hpp"
#include "query_optimizer/resolver/Resolver.hpp"
#include "gflags/gflags.h"
#include "query_optimizer/tests/TestDatabaseLoader.hpp"
#include "glog/logging.h"

namespace quickstep {

class ParseStatement;
class CatalogRelation;

namespace optimizer {

}  // namespace optimizer
}  // namespace quickstep

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::cout << "1 \n";
//  quickstep::SqlParserWrapper sql_parser_;
////  quickstep::optimizer::TestDatabaseLoader test_database_loader_;
//  quickstep::optimizer::Optimizer optimizer_;
//  std::string* query = new std::string("select int_col from test;");
//
//  sql_parser_.feedNextBuffer(query);
//  quickstep::ParseResult result = sql_parser_.getNextStatement();
//
//  std::cout << "2 \n";
//  quickstep::optimizer::OptimizerContext optimizer_context;
//  const quickstep::ParseStatement &parse_statement = *result.parsed_statement;
//
//  std::cout << "3 \n";
//
//  quickstep::optimizer::TestDatabaseLoader test_database_loader_;
//
//  test_database_loader_.createTestRelation(false /* allow_vchar */);
//  test_database_loader_.loadTestRelation();
//
//  std::cout << "4 \n";
//  const char * pplan =
//      hustle_getPhysicalPlan(parse_statement,
//                             test_database_loader_.catalog_database(),
//                             &optimizer_context);
//  std::cout << "5 \n";
//  std::cout << pplan << std::endl;
//
////  std::cout<< "AAAA :" << result.parsed_statement->toString() << std::endl;


//  const ParseStatement &parse_statement = *result.parsed_statement;
//  const CatalogRelation *query_result_relation = nullptr;


  return 0;

}
