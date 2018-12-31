#include "HustleResolver.h"

#include "query_optimizer/expressions/NamedExpression.hpp"
#include "query_optimizer/logical/MultiwayCartesianJoin.hpp"
#include "query_optimizer/logical/Project.hpp"
#include "query_optimizer/logical/TableReference.hpp"
#include "query_optimizer/logical/TopLevelPlan.hpp"

namespace logical = ::quickstep::optimizer::logical;
namespace expressions = ::quickstep::optimizer::expressions;

using namespace std;

HustleResolver::HustleResolver(const quickstep::CatalogDatabase &catalog_database,
                               quickstep::optimizer::OptimizerContext *context)
                               : catalog_database_(catalog_database), context_(context) { }

quickstep::optimizer::logical::LogicalPtr HustleResolver::resolve(shared_ptr<ParseNode> syntax_tree) {
    switch (syntax_tree->type) {
        case SELECT:
            logical_plan_ = resolve_select(dynamic_pointer_cast<SelectNode>(syntax_tree));
        default:
            cerr << "Unsupported top level node type: " << syntax_tree->type << endl;
            break;
    }

    logical_plan_ = logical::TopLevelPlan::Create(logical_plan_, with_queries_info_.with_query_plans);
    return logical_plan_;
}

quickstep::optimizer::logical::LogicalPtr HustleResolver::resolve_select(shared_ptr<SelectNode> select_node) {
    logical::LogicalPtr logical_plan;

    // resolve FROM
    vector<logical::LogicalPtr> from_logical;
    for (const auto &from_parse : select_node->from) {
        from_logical.emplace_back(resolve_reference(dynamic_pointer_cast<ReferenceNode>(from_parse)));
    }

    if (from_logical.size() > 1) {
        logical_plan = logical::MultiwayCartesianJoin::Create(from_logical);
    } else {
        logical_plan = from_logical[0];
    }

    // resolve SELECT
    vector<expressions::NamedExpressionPtr> select_list_expressions;
    vector<bool> has_aggregate_per_expression;
    // TODO: build these vectors

    // resolve GROUP BY
    vector<expressions::NamedExpressionPtr> group_by_expressions;
    // TODO: build this vector

    logical_plan = logical::Project::Create(logical_plan, select_list_expressions);

    return logical_plan;
}

quickstep::optimizer::logical::LogicalPtr
HustleResolver::resolve_reference(shared_ptr<ReferenceNode> reference_node) {
    logical::LogicalPtr logical_plan;
    const quickstep::CatalogRelation *relation = catalog_database_.getRelationByName(reference_node->reference);
    return logical::TableReference::Create(relation, reference_node->reference, context_);
}
