#include "FunctionNode.h"

FunctionNode::FunctionNode(const string name, const vector<ParseNode> *arguments) : ParseNode(FUNCTION) {
    this->name = name;
    this->arguments = arguments;
}
