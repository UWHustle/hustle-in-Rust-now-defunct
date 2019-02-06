#ifndef HUSTLE_HUSTLERESOLVER_H
#define HUSTLE_HUSTLERESOLVER_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "parser/ParseNode.h"
#include "parser/SelectNode.h"
#include "catalog/CatalogDatabase.hpp"
#include "query_optimizer/OptimizerContext.hpp"
#include "query_optimizer/expressions/Alias.hpp"
#include "query_optimizer/expressions/Predicate.hpp"
#include "query_optimizer/logical/Logical.hpp"
#include "query_optimizer/logical/TableReference.hpp"

class HustleResolver {
public:
    HustleResolver(const quickstep::CatalogDatabase &catalog_database, quickstep::optimizer::OptimizerContext *context);
    quickstep::optimizer::logical::LogicalPtr resolve(std::shared_ptr<ParseNode> syntax_tree);
private:

    struct WithQueriesInfo {
        std::vector<quickstep::optimizer::logical::LogicalPtr> with_query_plans;
        std::unordered_map<std::string, int> with_query_name_to_vector_position;
        std::unordered_set<int> unreferenced_query_indexes;
    };

    const quickstep::CatalogDatabase &catalog_database_;
    quickstep::optimizer::OptimizerContext *context_;
    WithQueriesInfo with_queries_info_;
    quickstep::optimizer::logical::LogicalPtr logical_plan_;

    quickstep::optimizer::logical::LogicalPtr resolve_select(std::shared_ptr<SelectNode> select_node);

    quickstep::optimizer::logical::LogicalPtr resolve_from(std::shared_ptr<ParseNode> parse_node,
            std::vector<quickstep::optimizer::expressions::AttributeReferencePtr> *attribute_references);
    quickstep::optimizer::expressions::ScalarPtr resolve_expression(std::shared_ptr<ParseNode> parse_node,
            std::vector<quickstep::optimizer::expressions::AttributeReferencePtr> *attribute_references,
            std::vector<quickstep::optimizer::expressions::AliasPtr> *aggregate_expressions);
    quickstep::optimizer::expressions::PredicatePtr resolve_predicate(std::shared_ptr<ParseNode> parse_node,
            std::vector<quickstep::optimizer::expressions::AttributeReferencePtr> *attribute_references);
};


#endif //HUSTLE_HUSTLERESOLVER_H
