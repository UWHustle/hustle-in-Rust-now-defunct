#include <stdio.h>

#include "parser/parser_wrapper.h"
#include "parser/parse_node.h"
#include "parser/utility/stringify.h"

int main(void) {
    char *test = "SELECT c FROM test;";
    parse_node *node = get_parse_tree(test);

    char *quickstep_output = quickstep_stringify(node);
    printf("%s\n", quickstep_output);
    free(quickstep_output);

    char *json_output = json_stringify(node);
    printf("%s", json_output);
    free(json_output);

    free_tree(node);
    return 0;
}