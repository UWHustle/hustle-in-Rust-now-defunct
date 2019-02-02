#ifndef HUSTLE_FUNCTIONNODE_H
#define HUSTLE_FUNCTIONNODE_H

#include "ParseNode.h"

class FunctionNode: public ParseNode {
public:
    enum FunctionType {NAMED, EQ};
    FunctionNode(FunctionType function_type, std::vector<std::shared_ptr<ParseNode>> arguments, std::string name = "");
    std::unordered_map<std::string, std::string> get_attributes() const override;
    std::unordered_map<std::string, std::vector<std::shared_ptr<ParseNode>>> get_children_lists() const override;
    std::string to_sql_string() override;
    FunctionType function_type;
    std::vector<std::shared_ptr<ParseNode> > arguments;
    std::string name;
};

#endif //HUSTLE_FUNCTIONNODE_H
