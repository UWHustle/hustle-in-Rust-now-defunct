#include "ReferenceNode.h"

ReferenceNode::ReferenceNode(string reference) : ParseNode(REFERENCE) {
    this->reference = reference;
}
