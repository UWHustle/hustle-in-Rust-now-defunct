#ifndef HUSTLE_PARSE_NODE_H
#define HUSTLE_PARSE_NODE_H

#include <vector>

enum NodeType {NONE, SELECT, REFERENCE, FUNCTION};

class ParseNode {
public:
    ParseNode();
    explicit ParseNode(NodeType type);
    virtual void json_stringify();
private:
    NodeType type;
};


#endif //HUSTLE_PARSE_NODE_H
