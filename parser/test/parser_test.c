#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "parser/parser_wrapper.h"
#include "parser/parse_node.h"
#include "parser/utility/stringify.h"

void test_parse_tree(char *query, char *expected_output) {
    parse_node *node = get_parse_tree(query);
    char *json_output = json_stringify(node);
    assert(!strcmp(json_output, expected_output));
    free(json_output);
    free_tree(node);
}

int main(void) {

    char *query1 = "SELECT Count(a), b FROM integer_table GROUP BY b;";
    char *expected1 = "{\n"
                      "  \"type\": \"ParseStatementSetOperation\",\n"
                      "  \"set_operation_query\": {\n"
                      "    \"type\": \"ParseSetOperation\",\n"
                      "    \"set_operation_type\": \"Select\",\n"
                      "    \"operands\": [\n"
                      "      {\n"
                      "        \"type\": \"ParseSelect\",\n"
                      "        \"select\": [\n"
                      "          {\n"
                      "            \"type\": \"FunctionCall\",\n"
                      "            \"name\": \"Count\",\n"
                      "            \"arguments\": [\n"
                      "              {\n"
                      "                \"type\": \"AttributeReference\",\n"
                      "                \"attribute_name\": \"a\"\n"
                      "              }\n"
                      "            ]\n"
                      "          },\n"
                      "          {\n"
                      "            \"type\": \"AttributeReference\",\n"
                      "            \"attribute_name\": \"b\"\n"
                      "          }\n"
                      "        ],\n"
                      "        \"from\": [\n"
                      "          {\n"
                      "            \"type\": \"ParseSimpleTableReference\",\n"
                      "            \"table_name\": \"integer_table\"\n"
                      "          }\n"
                      "        ],\n"
                      "        \"group_by\": [\n"
                      "          {\n"
                      "            \"type\": \"AttributeReference\",\n"
                      "            \"attribute_name\": \"b\"\n"
                      "          }\n"
                      "        ]\n"
                      "      }\n"
                      "    ]\n"
                      "  }\n"
                      "}";

    char *query2 = "SELECT CommonPrefix(ip) FROM ip_table;";
    char *expected2 = "{\n"
                      "  \"type\": \"ParseStatementSetOperation\",\n"
                      "  \"set_operation_query\": {\n"
                      "    \"type\": \"ParseSetOperation\",\n"
                      "    \"set_operation_type\": \"Select\",\n"
                      "    \"operands\": [\n"
                      "      {\n"
                      "        \"type\": \"ParseSelect\",\n"
                      "        \"select\": [\n"
                      "          {\n"
                      "            \"type\": \"FunctionCall\",\n"
                      "            \"name\": \"CommonPrefix\",\n"
                      "            \"arguments\": [\n"
                      "              {\n"
                      "                \"type\": \"AttributeReference\",\n"
                      "                \"attribute_name\": \"ip\"\n"
                      "              }\n"
                      "            ]\n"
                      "          }\n"
                      "        ],\n"
                      "        \"from\": [\n"
                      "          {\n"
                      "            \"type\": \"ParseSimpleTableReference\",\n"
                      "            \"table_name\": \"ip_table\"\n"
                      "          }\n"
                      "        ]\n"
                      "      }\n"
                      "    ]\n"
                      "  }\n"
                      "}";

    test_parse_tree(query1, expected1);
    test_parse_tree(query2, expected2);
    return 0;
}
