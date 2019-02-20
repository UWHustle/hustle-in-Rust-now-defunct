#include "CreateTableNode.h"

using namespace std;

CreateTableNode::CreateTableNode(string name, vector<shared_ptr<ParseNode>> attributes)
        : name(move(name)), attributes(move(attributes)) {}

unordered_map<string, string> CreateTableNode::get_attributes() const {
    auto attributes = ParseNode::get_attributes();
    attributes.insert({"name", name});
    return attributes;
}

unordered_map<string, vector<shared_ptr<ParseNode>>> CreateTableNode::get_children_lists() const {
    return {
            {"attributes", attributes}
    };
}
