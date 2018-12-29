#include "HustleResolver.h"

#include "query_optimizer/logical/MultiwayCartesianJoin.hpp"

namespace logical = ::quickstep::optimizer::logical;

using namespace std;

HustleResolver::HustleResolver(const quickstep::CatalogDatabase &catalog_database,
                               quickstep::optimizer::OptimizerContext *context)
                               : catalog_database_(catalog_database), context_(context) { }

quickstep::optimizer::logical::LogicalPtr HustleResolver::resolve(shared_ptr<ParseNode> syntax_tree) {
    switch (syntax_tree->type) {
        case SELECT:
            return resolve_select(dynamic_pointer_cast<SelectNode>(syntax_tree));
        default:
            cerr << "Unsupported top level node type: " << syntax_tree->type << endl;
            break;
    }
}

quickstep::optimizer::logical::LogicalPtr HustleResolver::resolve_select(shared_ptr<SelectNode> select_node) {
    logical::LogicalPtr logical_plan;

    // resolve FROM clause
    vector<logical::LogicalPtr> from_logical;
    for (const auto &from_parse : select_node->from) {
        from_logical.emplace_back(resolve_reference(dynamic_pointer_cast<ReferenceNode>(from_parse)));
    }

    if (from_logical.size() > 1) {
        logical_plan = logical::MultiwayCartesianJoin::Create(from_logical);
    } else {
        logical_plan = from_logical[0];
    }

    return logical_plan;
}

quickstep::optimizer::logical::LogicalPtr
HustleResolver::resolve_reference(shared_ptr<ReferenceNode> reference_node) {
    logical::LogicalPtr logical_plan;
//        return logical::TableReference::Create(nullptr, reference_node->reference, nullptr);
    return logical_plan;
}
