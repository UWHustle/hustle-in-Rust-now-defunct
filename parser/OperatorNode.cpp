#include <utility>
#include <string>

#include "OperatorNode.h"

using namespace std;

OperatorNode::OperatorNode(OperatorType operator_type, vector<shared_ptr<ParseNode>> operands)
        : operator_type(operator_type), operands(move(operands)) { }

unordered_map<string, string> OperatorNode::get_attributes() {
    auto attributes = ParseNode::get_attributes();
    attributes.insert({"operator", to_string(operator_type)});
    return attributes;
}

unordered_map<string, vector<shared_ptr<ParseNode>>> OperatorNode::get_children_lists() {
    return {
            {"operands", operands}
    };
}
