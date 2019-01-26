#ifndef HUSTLE_FUNCTIONNODE_H
#define HUSTLE_FUNCTIONNODE_H

#include "ParseNode.h"

enum Function {NAMED, EQ};

class FunctionNode: public ParseNode {
public:
    FunctionNode(Function function, std::vector<std::shared_ptr<ParseNode>> arguments, std::string name = "");
    std::unordered_map<std::string, std::string> get_attributes() override;
    std::unordered_map<std::string, std::vector<std::shared_ptr<ParseNode>>> get_children_lists() override;
    std::string to_sql_string() override;
    Function function;
    std::vector<std::shared_ptr<ParseNode> > arguments;
    std::string name;
};

#endif //HUSTLE_FUNCTIONNODE_H
