#include <utility>

#include <utility>

#include "SelectNode.h"

#include <iostream>

using namespace std;

SelectNode::SelectNode(vector<shared_ptr<ParseNode>> target, vector<shared_ptr<ParseNode>> from,
                       vector<shared_ptr<ParseNode>> group_by) : ParseNode(SELECT),
                       target(std::move(target)), from(std::move(from)), group_by(std::move(group_by)) { }

//void SelectNode::json_stringify() {
//    cout << "type: SELECT" << endl;
//
//    for (const auto &node : target) {
//        node->json_stringify();
//    }
//
//    for (const auto &node : from) {
//        node->json_stringify();
//    }
//
//    for (const auto &node : group_by) {
//        node->json_stringify();
//    }
//}
