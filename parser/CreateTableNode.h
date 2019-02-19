#ifndef HUSTLE_CREATENODE_H
#define HUSTLE_CREATENODE_H

#include "ParseNode.h"
#include "AttributeNode.h"

class CreateTableNode: public ParseNode {
public:
    CreateTableNode(std::string name, std::vector<std::shared_ptr<ParseNode>> attributes);
    std::unordered_map<std::string, std::string> get_attributes() const override;
    std::unordered_map<std::string, std::vector<std::shared_ptr<ParseNode>>> get_children_lists() const override;
    std::string name;
    std::vector<std::shared_ptr<ParseNode>> attributes;
};


#endif //HUSTLE_CREATENODE_H
