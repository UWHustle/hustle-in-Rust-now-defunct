#ifndef HUSTLE_REFERENCENODE_H
#define HUSTLE_REFERENCENODE_H

#include <string>
#include <unordered_map>
#include "ParseNode.h"

class ReferenceNode: public ParseNode {
public:
    explicit ReferenceNode(std::string attribute, std::string relation = "");
    std::unordered_map<std::string, std::string> get_attributes() override;
    std::string to_sql_string() override;
    std::string attribute;
    std::string relation;
};


#endif //HUSTLE_REFERENCENODE_H
