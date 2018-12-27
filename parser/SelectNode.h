#ifndef HUSTLE_SELECT_NODE_H
#define HUSTLE_SELECT_NODE_H

#include <vector>
#include "ParseNode.h"

using namespace std;

class SelectNode: public ParseNode {
public:
    SelectNode(vector<ParseNode*> target, vector<ParseNode*> from, vector<ParseNode*> group_by);
    void json_stringify();
private:
    vector<ParseNode*> target;
    vector<ParseNode*> from;
    vector<ParseNode*> group_by;
};


#endif //HUSTLE_SELECT_NODE_H
