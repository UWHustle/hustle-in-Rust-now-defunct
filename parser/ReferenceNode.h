#ifndef HUSTLE_REFERENCENODE_H
#define HUSTLE_REFERENCENODE_H

#include <string>
#include <unordered_map>
#include "ParseNode.h"

class ReferenceNode: public ParseNode {
public:
    explicit ReferenceNode(std::string reference);
    std::unordered_map<std::string, std::string> get_attributes() override;
private:
    std::string reference;
};


#endif //HUSTLE_REFERENCENODE_H
