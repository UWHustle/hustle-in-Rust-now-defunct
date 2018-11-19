#include <stdio.h>

#include "parser/parser_wrapper.h"
#include "parser/parse_node.h"
#include "parser/utility/stringify.h"

void test_parse_tree(char *query) {
    parse_node *node = get_parse_tree(query);
    char *json_output = json_stringify(node);
    printf("%s\n", json_output);
    free(json_output);
    free_tree(node);
}

int main(void) {
    test_parse_tree("SELECT Count(a), b FROM integer_table GROUP BY b;");
    test_parse_tree("SELECT CommonPrefix(ip) FROM ip_table;");
    return 0;
}