
#include <optimizer/quickstep/query_optimizer/expressions/PatternMatcher.hpp>
#include "HustleResolver.h"

#include "query_optimizer/expressions/Alias.hpp"
#include "query_optimizer/expressions/NamedExpression.hpp"
#include "query_optimizer/expressions/AggregateFunction.hpp"
#include "query_optimizer/logical/MultiwayCartesianJoin.hpp"
#include "query_optimizer/logical/Project.hpp"
#include "query_optimizer/logical/TableReference.hpp"
#include "query_optimizer/logical/TopLevelPlan.hpp"
#include "expressions/aggregation/AggregateFunctionSum.hpp"

namespace logical = ::quickstep::optimizer::logical;
namespace expressions = ::quickstep::optimizer::expressions;

using namespace std;

HustleResolver::HustleResolver(const quickstep::CatalogDatabase &catalog_database,
                               quickstep::optimizer::OptimizerContext *context)
                               : catalog_database_(catalog_database), context_(context) { }

logical::LogicalPtr HustleResolver::resolve(shared_ptr<ParseNode> syntax_tree) {
    switch (syntax_tree->type) {
        case SELECT:
            logical_plan_ = resolve_select(static_pointer_cast<SelectNode>(syntax_tree));
            break;
        default:
            ostringstream msg_stream;
            msg_stream << "Resolver: unsupported top-level node type: " << syntax_tree->type;
            throw msg_stream.str();
    }

    logical_plan_ = logical::TopLevelPlan::Create(logical_plan_, with_queries_info_.with_query_plans);
    return logical_plan_;
}

logical::LogicalPtr HustleResolver::resolve_select(shared_ptr<SelectNode> select_node) {
    logical::LogicalPtr logical_plan;
    vector<expressions::AttributeReferencePtr> attribute_references;

    // resolve FROM
    vector<logical::LogicalPtr> from_logical;
    for (const auto &from_parse : select_node->from) {
        logical::TableReferencePtr table_reference = resolve_reference(static_pointer_cast<ReferenceNode>(from_parse));
        from_logical.emplace_back(table_reference);
        for (const auto &referenced_attribute : table_reference->getReferencedAttributes()) {
            attribute_references.emplace_back(referenced_attribute);
        }
    }

    if (from_logical.size() > 1) {
        logical_plan = logical::MultiwayCartesianJoin::Create(from_logical);
    } else {
        logical_plan = from_logical[0];
    }

    // resolve SELECT
    vector<expressions::NamedExpressionPtr> select_list_expressions;

    for (const auto &project_parse : select_node->target) {

        expressions::ScalarPtr project_expression = resolve_expression(project_parse, attribute_references);

        expressions::ExprId project_expression_id;
        expressions::NamedExpressionPtr project_named_expression;
        if (expressions::SomeNamedExpression::MatchesWithConditionalCast(project_expression,
                &project_named_expression)) {
            project_expression_id = project_named_expression->id();
        } else {
            project_expression_id = context_->nextExprId();
        }

        if (project_named_expression == nullptr) {
            project_named_expression = expressions::Alias::Create(project_expression_id, project_expression, "", "");
        }
        select_list_expressions.emplace_back(project_named_expression);
    }
    

    // resolve GROUP BY
    vector<expressions::NamedExpressionPtr> group_by_expressions;
    // TODO: build this vector

    logical_plan = logical::Project::Create(logical_plan, select_list_expressions);

    return logical_plan;
}

logical::TableReferencePtr HustleResolver::resolve_reference(shared_ptr<ReferenceNode> reference_node) {
    const quickstep::CatalogRelation *relation = catalog_database_.getRelationByName(reference_node->reference);
    return logical::TableReference::Create(relation, reference_node->reference, context_);
}

expressions::ScalarPtr HustleResolver::resolve_expression(shared_ptr<ParseNode> parse_node,
        vector<expressions::AttributeReferencePtr> attribute_references) {
    switch (parse_node->type) {
        case REFERENCE: {
            auto project_reference_parse = static_pointer_cast<ReferenceNode>(parse_node);

            auto found_attribute_reference = find_if(attribute_references.begin(), attribute_references.end(),
                    [&project_reference_parse](const expressions::AttributeReferencePtr attribute_reference) {
                        return attribute_reference->attribute_name() == project_reference_parse->reference;
                    });
            return found_attribute_reference[0];
        }
        default:
            ostringstream msg_stream;
            msg_stream << "Resolver: unsupported expression node type: " << parse_node->type;
            throw msg_stream.str();
    }
}
