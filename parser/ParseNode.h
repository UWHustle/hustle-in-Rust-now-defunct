#ifndef HUSTLE_PARSE_NODE_H
#define HUSTLE_PARSE_NODE_H

#include <memory>
#include <unordered_map>
#include <vector>

enum NodeType {NONE, SELECT, REFERENCE, FUNCTION};

class ParseNode {
public:
    ParseNode();
    explicit ParseNode(NodeType type);
    void json_stringify();
    virtual std::unordered_map<std::string, std::string> get_attributes();
    virtual std::unordered_map<std::string, std::shared_ptr<ParseNode>> get_children();
    virtual std::unordered_map<std::string, std::vector<std::shared_ptr<ParseNode>>> get_children_lists();
    virtual std::string to_sql_string();
    NodeType type;
protected:
    static std::string to_sql_string(std::vector<std::shared_ptr<ParseNode>> nodes);
};

#endif //HUSTLE_PARSE_NODE_H