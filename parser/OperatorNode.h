#ifndef HUSTLE_OPERATORNODE_H
#define HUSTLE_OPERATORNODE_H

#include "ParseNode.h"

enum OperatorType {EQ};

class OperatorNode: public ParseNode {
public:
    OperatorNode(OperatorType operator_type, std::vector<std::shared_ptr<ParseNode>> operands);
    OperatorType operator_type;
    std::vector<std::shared_ptr<ParseNode>> operands;
    std::unordered_map<std::string, std::string> get_attributes() override;
    std::unordered_map<std::string, std::vector<std::shared_ptr<ParseNode>>> get_children_lists() override;
};


#endif //HUSTLE_OPERATORNODE_H
