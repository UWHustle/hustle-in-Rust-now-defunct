#include "ParseNode.h"

#include <iostream>

#define ATTRIBUTE_NAME(x) #x

using namespace std;

ParseNode::ParseNode() {
    this->type = NONE;
}

ParseNode::ParseNode(NodeType type) {
    this->type = type;
}

void ParseNode::json_stringify() {
    cout << ATTRIBUTE_NAME(this->type) << this->type << endl;
}
