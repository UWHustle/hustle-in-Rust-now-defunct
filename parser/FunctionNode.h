#ifndef HUSTLE_FUNCTIONNODE_H
#define HUSTLE_FUNCTIONNODE_H

#include <memory>
#include <string>
#include <vector>
#include "ParseNode.h"

class FunctionNode: public ParseNode {
public:
    FunctionNode(std::string name, std::vector<std::shared_ptr<ParseNode>> arguments);
    std::unordered_map<std::string, std::string> get_attributes() override;
    std::unordered_map<std::string, std::vector<std::shared_ptr<ParseNode>>> get_children_lists() override;
private:
    std::string name;
    std::vector<std::shared_ptr<ParseNode> > arguments;
};

#endif //HUSTLE_FUNCTIONNODE_H
