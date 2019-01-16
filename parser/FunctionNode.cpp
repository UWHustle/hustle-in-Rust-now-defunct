#include "FunctionNode.h"

using namespace std;

FunctionNode::FunctionNode(const string name, vector<shared_ptr<ParseNode>> arguments) : ParseNode(FUNCTION), arguments(move(arguments)) {
    this->name = name;
}

unordered_map<string, string> FunctionNode::get_attributes() {
    auto attributes = ParseNode::get_attributes();
    attributes.insert({"name", name});
    return attributes;
}

unordered_map<string, vector<shared_ptr<ParseNode>>> FunctionNode::get_children_lists() {
    return {
            {"arguments", arguments}
    };
}
