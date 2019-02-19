#include <memory>
#include "parser/ParserDriver.h"

using namespace std;

void test_parse_create() {
    ParserDriver parser_driver;
    shared_ptr<ParseNode> expected;

    parser_driver.parse("CREATE TABLE t (a INT);");
    expected = static_pointer_cast<ParseNode>(make_shared<CreateTableNode>(
            "t",
            vector<shared_ptr<ParseNode>>{
                    make_shared<AttributeNode>("a", "INT")
            }));
    assert(*expected == *parser_driver.syntax_tree);

    parser_driver.parse("CREATE TABLE t (a INT, b TEXT, c CUSTOM_TYPE);");
    expected = static_pointer_cast<ParseNode>(make_shared<CreateTableNode>(
            "t",
            vector<shared_ptr<ParseNode>>{
                    make_shared<AttributeNode>("a", "INT"),
                    make_shared<AttributeNode>("b", "TEXT"),
                    make_shared<AttributeNode>("c", "CUSTOM_TYPE")
            }));
    assert(*expected == *parser_driver.syntax_tree);
}

int main() {
    test_parse_create();
}
