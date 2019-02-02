#include "ParseNode.h"

#include <iostream>
#include <sstream>

using namespace std;

ParseNode::ParseNode() {
    type = NodeType::NONE;
}

ParseNode::ParseNode(NodeType type) {
    this->type = type;
}

bool ParseNode::operator==(const ParseNode& other) {
    auto attributes = get_attributes();
    auto children = get_children();
    auto children_lists = get_children_lists();

    auto other_children = other.get_children();
    auto other_attributes = other.get_attributes();
    auto other_children_lists = other.get_children_lists();

    auto size_equal = attributes.size() == other_attributes.size()
            && children.size() == other_children.size()
            && other_children_lists.size() == other_children_lists.size();

    auto attributes_equal = attributes == other_attributes;

    auto children_equal = all_of(children.begin(), children.end(),
            [&other_children](const pair<string, shared_ptr<ParseNode>> &child) {
        if (other_children.find(child.first) == other_children.end()) {
            return false;
        }
        auto other_child = other_children[child.first];
        if (child.second && other_child) {
            return *child.second == *other_child;
        }
        return child.second == other_child;
    });

    auto children_lists_equal = all_of(children_lists.begin(), children_lists.end(),
            [&other_children_lists](const pair<string, vector<shared_ptr<ParseNode>>> &children_list) {
        if (other_children_lists.find(children_list.first) == other_children_lists.end()) {
            return false;
        }
        auto other_children_list = other_children_lists[children_list.first];
        return equal(children_list.second.begin(), children_list.second.end(), other_children_list.begin(),
                [](const shared_ptr<ParseNode> &left, const shared_ptr<ParseNode> &right) {
            if (left && right) {
                return *left == *right;
            }
            return left == right;
        });
    });

    return size_equal && attributes_equal && children_equal && children_lists_equal;
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

    for (const auto &child : children) {
        if (child.second) {
            cout << child.first << ":";
            child.second->json_stringify();
            if (key_index < num_keys - 1) {
                cout << ",";
            }
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

unordered_map<string, string> ParseNode::get_attributes() const {
    return {
            {"type", to_string(type)}
    };
}

unordered_map<string, shared_ptr<ParseNode>> ParseNode::get_children() const {
    return {};
}

unordered_map<string, vector<shared_ptr<ParseNode>>> ParseNode::get_children_lists() const {
    return {};
}

string ParseNode::to_sql_string() {
    return string();
}

string ParseNode::to_sql_string(vector<shared_ptr<ParseNode>> nodes) {
    stringstream sql_stream;
    for (size_t i = 0; i < nodes.size(); ++i) {
        if (i != 0) {
            sql_stream << ", ";
        }
        sql_stream << nodes[i]->to_sql_string();
    }
    return sql_stream.str();
}
