#include "SelectNode.h"

using namespace std;

SelectNode::SelectNode(vector<shared_ptr<ParseNode>> target, vector<shared_ptr<ParseNode>> from,
                       vector<shared_ptr<ParseNode>> group_by) : ParseNode(SELECT),
                       target(move(target)), from(move(from)), group_by(move(group_by)) { }

unordered_map<string, vector<shared_ptr<ParseNode>>> SelectNode::get_children_lists() {
    return {
            {"target", target},
            {"from", from},
            {"group_by", group_by}
    };
}
