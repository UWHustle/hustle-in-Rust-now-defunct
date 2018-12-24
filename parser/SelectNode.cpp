#include "SelectNode.h"

SelectNode::SelectNode(const vector<ParseNode> *target, const vector<ParseNode> *from,
                       const vector<ParseNode> *group_by) : ParseNode(SELECT) {
    this->target = target;
    this->from = from;
    this->group_by = group_by;
}

