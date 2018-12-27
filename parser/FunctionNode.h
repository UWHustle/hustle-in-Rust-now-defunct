#ifndef HUSTLE_FUNCTIONNODE_H
#define HUSTLE_FUNCTIONNODE_H

#include <string>
#include "ParseNode.h"

class FunctionNode: public ParseNode {
public:
    FunctionNode(std::string name, std::vector<std::unique_ptr<ParseNode> > arguments);
    void json_stringify();

private:
    std::string name;
    std::vector<std::unique_ptr<ParseNode> > arguments;
};


#endif //HUSTLE_FUNCTIONNODE_H
