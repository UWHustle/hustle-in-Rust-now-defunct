#include "FunctionNode.h"

#include <iostream>

using namespace std;

FunctionNode::FunctionNode(const string name, vector<unique_ptr<ParseNode> > arguments) : ParseNode(FUNCTION), arguments(std::move(arguments)) {
    this->name = name;
}

void FunctionNode::json_stringify() {
    cout << "type: FUNCTION" << endl;

    for (const auto &node : arguments) {
        node->json_stringify();
    }
}