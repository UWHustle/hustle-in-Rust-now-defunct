#ifndef HUSTLE_FUNCTIONNODE_H
#define HUSTLE_FUNCTIONNODE_H

#include <string>
#include "ParseNode.h"

using namespace std;

class FunctionNode: public ParseNode {
public:
    FunctionNode(string name, vector<ParseNode*> arguments);
    void json_stringify();

private:
    string name;
    vector<ParseNode*> arguments;
};


#endif //HUSTLE_FUNCTIONNODE_H
