#ifndef HUSTLE_SELECT_NODE_H
#define HUSTLE_SELECT_NODE_H

#include <memory>
#include <vector>
#include <unordered_map>
#include <string>
#include "ParseNode.h"
#include "ReferenceNode.h"

class SelectNode: public ParseNode {
public:
    SelectNode(std::vector<std::shared_ptr<ParseNode>> target, std::vector<std::shared_ptr<ParseNode>> from,
            std::vector<std::shared_ptr<ParseNode>> group_by);
    std::unordered_map<std::string, std::vector<std::shared_ptr<ParseNode>>> get_children_lists() override;
    std::vector<std::shared_ptr<ParseNode>> target;
    std::vector<std::shared_ptr<ParseNode>> from;
    std::vector<std::shared_ptr<ParseNode>> group_by;
};

#endif //HUSTLE_SELECT_NODE_H
