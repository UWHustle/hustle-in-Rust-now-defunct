#ifndef HUSTLE_SELECT_NODE_H
#define HUSTLE_SELECT_NODE_H

#include <vector>
#include <memory>
#include "ParseNode.h"

class SelectNode: public ParseNode {
public:
    SelectNode(std::vector<std::shared_ptr<ParseNode> > target, std::vector<std::shared_ptr<ParseNode> > from,
            std::vector<std::shared_ptr<ParseNode> > group_by);
//    void json_stringify();
private:
    std::vector<std::shared_ptr<ParseNode> > target;
    std::vector<std::shared_ptr<ParseNode> > from;
    std::vector<std::shared_ptr<ParseNode> > group_by;
};

#endif //HUSTLE_SELECT_NODE_H
