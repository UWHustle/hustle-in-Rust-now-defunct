#include "SelectNode.h"

#include <iostream>

using namespace std;

SelectNode::SelectNode(vector<ParseNode*> target, vector<ParseNode*> from,
                       vector<ParseNode*> group_by) : ParseNode(SELECT) {
    this->target = target;
    this->from = from;
    this->group_by = group_by;
}

void SelectNode::json_stringify() {
    cout << "type: SELECT" << endl;

    for (const auto &node : target) {
        node->json_stringify();
    }

    for (const auto &node : from) {
        node->json_stringify();
    }

    for (const auto &node : group_by) {
        node->json_stringify();
    }
}
