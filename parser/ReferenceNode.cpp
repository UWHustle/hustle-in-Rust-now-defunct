#include "ReferenceNode.h"

using namespace std;

ReferenceNode::ReferenceNode(const string reference) : ParseNode(REFERENCE) {
    this->reference = reference;
}

unordered_map<string, string> ReferenceNode::get_attributes() {
    auto attributes = ParseNode::get_attributes();
    attributes.insert({"reference", reference});
    return attributes;
}
