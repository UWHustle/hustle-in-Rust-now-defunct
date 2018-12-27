#ifndef HUSTLE_SELECT_NODE_H
#define HUSTLE_SELECT_NODE_H

#include <vector>
#include "ParseNode.h"

class SelectNode: public ParseNode {
public:
    SelectNode(std::vector<std::unique_ptr<ParseNode> > target, std::vector<std::unique_ptr<ParseNode> > from,
            std::vector<std::unique_ptr<ParseNode> > group_by);
    void json_stringify();
private:
    std::vector<std::unique_ptr<ParseNode> > target;
    std::vector<std::unique_ptr<ParseNode> > from;
    std::vector<std::unique_ptr<ParseNode> > group_by;
};

#endif //HUSTLE_SELECT_NODE_H
