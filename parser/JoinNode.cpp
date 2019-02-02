#include "JoinNode.h"

#include <algorithm>
#include <string>
#include <utility>

using namespace std;

JoinNode::JoinNode(JoinType join_type, shared_ptr<ParseNode> left, shared_ptr<ParseNode> right,
                   shared_ptr<ParseNode> predicate) : ParseNode(JOIN), join_type(join_type), left(move(left)),
                                                      right(move(right)), predicate(move(predicate)) {

}

unordered_map<string, string> JoinNode::get_attributes() const {
    auto attributes = ParseNode::get_attributes();
    attributes.insert({"join_type", to_string(join_type)});
    return attributes;
}

unordered_map<string, shared_ptr<ParseNode>> JoinNode::get_children() const {
    return {
            {"left", left},
            {"right", right},
            {"predicate", predicate}
    };
}

JoinNode::JoinType JoinNode::parse_join_type(string join_keyword) {
    transform(join_keyword.begin(), join_keyword.end(), join_keyword.begin(), ::tolower);
    if (join_keyword == "natural") {
        return NATURAL;
    } else if (join_keyword == "left" || join_keyword == "leftouter") {
        return LEFT;
    } else if (join_keyword == "right" || join_keyword == "rightouter") {
        return RIGHT;
    } else if (join_keyword == "outer" || join_keyword == "fullouter") {
        return OUTER;
    } else if (join_keyword == "inner") {
        return INNER;
    } else if (join_keyword == "cross" || join_keyword == "innercross") {
        return CROSS;
    }
    return NONE;
}


