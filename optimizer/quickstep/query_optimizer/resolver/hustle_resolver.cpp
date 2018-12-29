#include "hustle_resolver.h"

#include <iostream>
#include "optimizer/optimizer_wrapper.hpp"
#include "catalog/CatalogRelation.hpp"
#include "query_optimizer/logical/Logical.hpp"
#include "query_optimizer/logical/MultiwayCartesianJoin.hpp"
#include "query_optimizer/logical/TableReference.hpp"

using namespace std;
using namespace resolver;

namespace logical = quickstep::optimizer::logical;

namespace {
    logical::LogicalPtr resolve_select(shared_ptr<SelectNode> select_node);
    logical::LogicalPtr resolve_reference(shared_ptr<ReferenceNode> reference_node);
}

void resolver::resolve(shared_ptr<ParseNode> syntax_tree, string input) {
    switch (syntax_tree->type) {
        case SELECT:
            resolve_select(dynamic_pointer_cast<SelectNode>(syntax_tree));
        default:
            cerr << "Unsupported top level node type: " << syntax_tree->type << endl;
            break;
    }
//    int res = optimizer(input);
}

namespace {
    logical::LogicalPtr resolve_select(shared_ptr<SelectNode> select_node) {
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

    logical::LogicalPtr resolve_reference(shared_ptr<ReferenceNode> reference_node) {
        logical::LogicalPtr logical_plan;
        return logical::TableReference::Create(nullptr, reference_node->reference, nullptr);
    }
}
