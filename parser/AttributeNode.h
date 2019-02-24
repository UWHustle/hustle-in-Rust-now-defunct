#ifndef HUSTLE_ATTRIBUTENODE_H
#define HUSTLE_ATTRIBUTENODE_H

#include "ParseNode.h"

class AttributeNode: public ParseNode {
public:
    AttributeNode(std::string name, std::string type);
    std::unordered_map<std::string, std::string> get_attributes() const override;
    std::string name;
    std::string type;
};


#endif //HUSTLE_ATTRIBUTENODE_H
