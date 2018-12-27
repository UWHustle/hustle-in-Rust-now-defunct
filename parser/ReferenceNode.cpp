#include "ReferenceNode.h"

#include <iostream>

using namespace std;

ReferenceNode::ReferenceNode(const string reference) : ParseNode(REFERENCE) {
    this->reference = reference;
}

void ReferenceNode::json_stringify() {
    cout << "type: REFERENCE" << endl;
}