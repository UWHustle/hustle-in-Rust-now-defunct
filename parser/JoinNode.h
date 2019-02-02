#ifndef HUSTLE_JOINNODE_H
#define HUSTLE_JOINNODE_H

#include "ParseNode.h"

class JoinNode: public ParseNode {
public:
    enum JoinType {NONE, NATURAL, LEFT, OUTER, RIGHT, INNER, CROSS};
    JoinNode(JoinType join_type, std::shared_ptr<ParseNode> left, std::shared_ptr<ParseNode> right,
             std::shared_ptr<ParseNode> predicate);
    std::unordered_map<std::string, std::string> get_attributes() const override;
    std::unordered_map<std::string, std::shared_ptr<ParseNode>> get_children() const override;
    JoinType join_type;
    std::shared_ptr<ParseNode> left;
    std::shared_ptr<ParseNode> right;
    std::shared_ptr<ParseNode> predicate;
    static JoinType parse_join_type(std::string join_keyword);
};


#endif //HUSTLE_JOINNODE_H
