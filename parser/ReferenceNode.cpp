#include "ReferenceNode.h"

using namespace std;

ReferenceNode::ReferenceNode(const string attribute, const string relation) : ParseNode(REFERENCE) {
    this->attribute = attribute;
    this->relation = relation;
}

unordered_map<string, string> ReferenceNode::get_attributes() const {
    auto attributes = ParseNode::get_attributes();
    attributes.insert({
        {"attribute", attribute},
        {"relation", relation}
    });
    return attributes;
}

string ReferenceNode::to_sql_string() {
    if (relation.empty()) {
        return attribute;
    }
    return relation + "." + attribute;
}
