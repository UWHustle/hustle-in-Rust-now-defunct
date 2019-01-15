#ifndef QUICKSTEP_HUSTLEOPTIMIZER_H
#define QUICKSTEP_HUSTLEOPTIMIZER_H
#include <iostream>
#include <memory>
#include "parser/ParseNode.h"

//namespace quickstep {
//
//class CatalogDatabase;
//class ParseStatement;
//class QueryHandle;
//
//namespace optimizer {
//
//class OptimizerContext;
//
//const char* hustle_getPhysicalPlan(const ParseStatement &parse_statement,
//                              CatalogDatabase *catalog_database,
//                              OptimizerContext *optimizer_context);
//

std::string hustle_optimize(const std::shared_ptr<ParseNode> &syntax_tree, const std::string &sql = std::string());
//
//
//}  // namespace optimizer
//}  // namespace quickstep


#endif //QUICKSTEP_HUSTLEOPTIMIZER_H
