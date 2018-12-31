#ifndef HUSTLE_HUSTLERESOLVER_H
#define HUSTLE_HUSTLERESOLVER_H

#include <memory>
#include <string>
#include <unordered_map>
#include "parser/ParseNode.h"
#include "parser/SelectNode.h"
#include "parser/ReferenceNode.h"
#include "catalog/CatalogDatabase.hpp"
#include "query_optimizer/OptimizerContext.hpp"
#include "query_optimizer/logical/Logical.hpp"

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
    quickstep::optimizer::logical::LogicalPtr resolve_reference(std::shared_ptr<ReferenceNode> reference_node);
};


#endif //HUSTLE_HUSTLERESOLVER_H
