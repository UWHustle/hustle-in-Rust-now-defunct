#include "FunctionNode.h"

#include <sstream>
#include <utility>

using namespace std;

FunctionNode::FunctionNode(FunctionType function_type, vector<shared_ptr<ParseNode>> arguments, const string name)
        : ParseNode(FUNCTION), function_type(function_type), arguments(move(arguments)), name(name) { }

unordered_map<string, string> FunctionNode::get_attributes() {
    auto attributes = ParseNode::get_attributes();
    attributes.insert({"function", to_string(function_type)});
    if (function_type == NAMED) {
        attributes.insert({"name", name});
    }
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
