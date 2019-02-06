#include <iostream>
#include <memory>
#include "parser/ParserDriver.h"

using namespace std;

void test_parse_select() {
    ParserDriver parser_driver;
    shared_ptr<ParseNode> expected;

    parser_driver.parse("SELECT a FROM t;");
    expected = static_pointer_cast<ParseNode>(make_shared<SelectNode>(
            vector<shared_ptr<ParseNode>>{
                    make_shared<ReferenceNode>("a")},
            make_shared<ReferenceNode>("", "t"),
            nullptr,
            vector<shared_ptr<ParseNode>>()));
    assert(*expected == *parser_driver.syntax_tree);

    parser_driver.parse("SELECT MIN(a) FROM t;");
    expected = static_pointer_cast<ParseNode>(make_shared<SelectNode>(
            vector<shared_ptr<ParseNode>>{
                    make_shared<FunctionNode>(
                            FunctionNode::NAMED,
                            vector<shared_ptr<ParseNode>>{
                                    make_shared<ReferenceNode>("a")},
                            "MIN")},
            make_shared<ReferenceNode>("", "t"),
            nullptr,
            vector<shared_ptr<ParseNode>>()));
    assert(*expected == *parser_driver.syntax_tree);

    parser_driver.parse("SELECT a FROM t GROUP BY a;");
    expected = static_pointer_cast<ParseNode>(make_shared<SelectNode>(
            vector<shared_ptr<ParseNode>>{
                    make_shared<ReferenceNode>("a")},
            make_shared<ReferenceNode>("", "t"),
            nullptr,
            vector<shared_ptr<ParseNode>>{
                    make_shared<ReferenceNode>("a")}));
    assert(*expected == *parser_driver.syntax_tree);

    parser_driver.parse("SELECT t.a, u.b FROM t, u;");
    expected = static_pointer_cast<ParseNode>(make_shared<SelectNode>(
            vector<shared_ptr<ParseNode>>{
                    make_shared<ReferenceNode>("a", "t"),
                    make_shared<ReferenceNode>("b", "u")},
            make_shared<JoinNode>(
                    JoinNode::CROSS,
                    make_shared<ReferenceNode>("", "t"),
                    make_shared<ReferenceNode>("", "u"),
                    nullptr),
            nullptr,
            vector<shared_ptr<ParseNode>>()));
    assert(*expected == *parser_driver.syntax_tree);

    parser_driver.parse("SELECT t.a, u.b FROM t, u WHERE t.a = u.b;");
    expected = static_pointer_cast<ParseNode>(make_shared<SelectNode>(
            vector<shared_ptr<ParseNode>>{
                    make_shared<ReferenceNode>("a", "t"),
                    make_shared<ReferenceNode>("b", "u")},
            make_shared<JoinNode>(
                    JoinNode::CROSS,
                    make_shared<ReferenceNode>("", "t"),
                    make_shared<ReferenceNode>("", "u"),
                    nullptr),
            make_shared<FunctionNode>(
                    FunctionNode::EQ, vector<shared_ptr<ParseNode>>{
                            make_shared<ReferenceNode>("a", "t"),
                            make_shared<ReferenceNode>("b", "u")
                    }),
            vector<shared_ptr<ParseNode>>()));
    assert(*expected == *parser_driver.syntax_tree);

    parser_driver.parse("SELECT t.a, u.b FROM t JOIN u ON t.a = u.b;");
    expected = static_pointer_cast<ParseNode>(make_shared<SelectNode>(
            vector<shared_ptr<ParseNode>>{
                    make_shared<ReferenceNode>("a", "t"),
                    make_shared<ReferenceNode>("b", "u")},
            make_shared<JoinNode>(
                    JoinNode::INNER,
                    make_shared<ReferenceNode>("", "t"),
                    make_shared<ReferenceNode>("", "u"),
                    make_shared<FunctionNode>(
                            FunctionNode::EQ, vector<shared_ptr<ParseNode>>{
                                    make_shared<ReferenceNode>("a", "t"),
                                    make_shared<ReferenceNode>("b", "u")
                            })),
            nullptr,
            vector<shared_ptr<ParseNode>>()));
    assert(*expected == *parser_driver.syntax_tree);
}

void test_parse_join_type() {
    assert(JoinNode::parse_join_type("NATURAL") == JoinNode::NATURAL);
    assert(JoinNode::parse_join_type("LEFT") == JoinNode::LEFT);
    assert(JoinNode::parse_join_type("LEFTOUTER") == JoinNode::LEFT);
    assert(JoinNode::parse_join_type("RIGHT") == JoinNode::RIGHT);
    assert(JoinNode::parse_join_type("RIGHTOUTER") == JoinNode::RIGHT);
    assert(JoinNode::parse_join_type("OUTER") == JoinNode::OUTER);
    assert(JoinNode::parse_join_type("FULLOUTER") == JoinNode::OUTER);
    assert(JoinNode::parse_join_type("INNER") == JoinNode::INNER);
    assert(JoinNode::parse_join_type("CROSS") == JoinNode::CROSS);
}

int main() {
    test_parse_select();
    test_parse_join_type();
}
