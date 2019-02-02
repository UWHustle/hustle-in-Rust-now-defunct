#include <iostream>
#include <memory>
#include "parser/ParserDriver.h"

using namespace std;

void select() {
    ParserDriver parser_driver;

    parser_driver.parse("SELECT a FROM t;");
    auto expected = static_pointer_cast<ParseNode>(make_shared<SelectNode>(
            vector<shared_ptr<ParseNode>>{make_shared<ReferenceNode>("a")},
            make_shared<ReferenceNode>("", "t"),
            nullptr,
            vector<shared_ptr<ParseNode>>()));
    assert(*expected == *parser_driver.syntax_tree);
}

int main() {
    select();
}
