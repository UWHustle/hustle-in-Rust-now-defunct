#ifndef HUSTLE_REFERENCENODE_H
#define HUSTLE_REFERENCENODE_H

#include <string>
#include "ParseNode.h"

using namespace std;

class ReferenceNode: public ParseNode {
public:
    explicit ReferenceNode(string reference);
private:
    string reference;
};


#endif //HUSTLE_REFERENCENODE_H
