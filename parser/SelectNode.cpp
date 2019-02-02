#include <utility>

#include "SelectNode.h"

using namespace std;

SelectNode::SelectNode(vector<shared_ptr<ParseNode>> target, shared_ptr<ParseNode> from,
        shared_ptr<ParseNode> where, vector<shared_ptr<ParseNode>> group_by) : ParseNode(SELECT),
        target(move(target)), from(move(from)), where(move(where)), group_by(move(group_by)) { }


unordered_map<string, shared_ptr<ParseNode>> SelectNode::get_children() const {
    return {
            {"from", from},
            {"where", where}
    };
}

unordered_map<string, vector<shared_ptr<ParseNode>>> SelectNode::get_children_lists() const {
    return {
            {"target", target},
            {"group_by", group_by}
    };
}


