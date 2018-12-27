#include "ParseNode.h"

#include <iostream>

using namespace std;

ParseNode::ParseNode() {
    type = NONE;
}

ParseNode::ParseNode(NodeType type) {
    this->type = type;
}

void ParseNode::json_stringify() {
    auto attributes = get_attributes();
    auto children = get_children();
    auto children_lists = get_children_lists();

    size_t num_keys = attributes.size() + children.size() + children_lists.size();
    size_t key_index = 0;

    cout << "{";

    for (const auto &attribute : attributes) {
        cout << attribute.first << ":" << attribute.second;
        if (key_index < num_keys - 1) {
            cout << ",";
        }
        key_index++;
    }

    for (const auto &child : children_lists) {
        cout << child.first << ":[";
        for (size_t i = 0; i < child.second.size(); ++i) {
            if (i) {
                cout << ",";
            }
            child.second[i]->json_stringify();
        }
        cout << "]";
        if (key_index < num_keys - 1) {
            cout << ",";
        }
        key_index++;
    }

    cout << "}";
}

unordered_map<string, string> ParseNode::get_attributes() {
    return {
            {"type", to_string(type)}
    };
}

unordered_map<string, shared_ptr<ParseNode>> ParseNode::get_children() {
    return {};
}

unordered_map<string, vector<shared_ptr<ParseNode>>> ParseNode::get_children_lists() {
    return {};
}
