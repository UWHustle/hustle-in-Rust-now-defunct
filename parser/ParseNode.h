#ifndef HUSTLE_PARSE_NODE_H
#define HUSTLE_PARSE_NODE_H

enum NodeType {NONE, SELECT, REFERENCE, FUNCTION};

class ParseNode {
public:
    ParseNode();
    explicit ParseNode(NodeType type);
private:
    NodeType type;
};


#endif //HUSTLE_PARSE_NODE_H
