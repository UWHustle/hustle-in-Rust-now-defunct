#include "ParseNode.h"

ParseNode::ParseNode() {
    this->type = NONE;
}

ParseNode::ParseNode(NodeType type) {
    this->type = type;
}
