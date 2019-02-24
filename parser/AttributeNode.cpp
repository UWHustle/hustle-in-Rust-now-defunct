#include "AttributeNode.h"

#include <utility>

using namespace std;

AttributeNode::AttributeNode(string name, string type) : name(move(name)), type(move(type)) {}

unordered_map<string, string> AttributeNode::get_attributes() const {
    auto attributes = ParseNode::get_attributes();
    attributes.insert({"name", name});
    attributes.insert({"type", type});
    return attributes;
}
