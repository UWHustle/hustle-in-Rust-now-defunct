#include "ParseNode.h"

#include <iostream>

using namespace std;

ParseNode::ParseNode() {
    this->type = NONE;
}

ParseNode::ParseNode(NodeType type) {
    this->type = type;
}

void ParseNode::json_stringify() {
    cout << "type: " << this->type << endl;
}
