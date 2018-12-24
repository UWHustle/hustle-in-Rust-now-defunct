#ifndef HUSTLE_SELECT_NODE_H
#define HUSTLE_SELECT_NODE_H

#include <vector>
#include "ParseNode.h"

using namespace std;

class SelectNode: public ParseNode {
public:
    SelectNode(const vector<ParseNode> *target, const vector<ParseNode> *from, const vector<ParseNode> *group_by);

private:
    const vector<ParseNode> *target;
    const vector<ParseNode> *from;
    const vector<ParseNode> *group_by;
};


#endif //HUSTLE_SELECT_NODE_H
