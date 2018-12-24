#ifndef HUSTLE_FUNCTIONNODE_H
#define HUSTLE_FUNCTIONNODE_H

#include <string>
#include "ParseNode.h"

using namespace std;

class FunctionNode: public ParseNode {
public:
    FunctionNode(string name, const vector<ParseNode> *arguments);

private:
    string name;
    const vector<ParseNode> *arguments;
};


#endif //HUSTLE_FUNCTIONNODE_H
