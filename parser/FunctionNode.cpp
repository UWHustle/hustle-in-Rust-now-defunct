#include "FunctionNode.h"

#include <iostream>

using namespace std;

FunctionNode::FunctionNode(const string name, vector<ParseNode*> arguments) : ParseNode(FUNCTION) {
    this->name = name;
    this->arguments = arguments;
}

void FunctionNode::json_stringify() {
    cout << "type: FUNCTION" << endl;

    for (const auto &node : arguments) {
        node->json_stringify();
    }
}