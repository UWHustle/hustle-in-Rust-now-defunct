#ifndef HUSTLE_PARSE_NODE_H
#define HUSTLE_PARSE_NODE_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

class ParseNode {
public:
    enum NodeType {NONE, SELECT, REFERENCE, FUNCTION, JOIN};
    ParseNode();
    explicit ParseNode(NodeType type);
    bool operator==(const ParseNode& other);
    void json_stringify();
    virtual std::unordered_map<std::string, std::string> get_attributes() const;
    virtual std::unordered_map<std::string, std::shared_ptr<ParseNode>> get_children() const;
    virtual std::unordered_map<std::string, std::vector<std::shared_ptr<ParseNode>>> get_children_lists() const;
    virtual std::string to_sql_string();
    NodeType type;
protected:
    static std::string to_sql_string(std::vector<std::shared_ptr<ParseNode>> nodes);
};

#endif //HUSTLE_PARSE_NODE_H
