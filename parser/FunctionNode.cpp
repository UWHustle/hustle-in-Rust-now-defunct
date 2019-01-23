#include "FunctionNode.h"

#include <sstream>

using namespace std;

FunctionNode::FunctionNode(const string name, vector<shared_ptr<ParseNode>> arguments)
        : ParseNode(FUNCTION), arguments(arguments) {
    this->name = name;
}

unordered_map<string, string> FunctionNode::get_attributes() {
    auto attributes = ParseNode::get_attributes();
    attributes.insert({"name", name});
    return attributes;
}

unordered_map<string, vector<shared_ptr<ParseNode>>> FunctionNode::get_children_lists() {
    return {
            {"arguments", arguments}
    };
}

string FunctionNode::to_sql_string() {
    stringstream sql_stream;
    sql_stream << name << "(" << ParseNode::to_sql_string(arguments) << ")";
    return sql_stream.str();
}
