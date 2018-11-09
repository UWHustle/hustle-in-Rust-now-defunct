#include <stdio.h>

#include "parser_wrapper.h"
#include "parse_node.h"
#include "stringify.h"

int main(void) {
    char test[] = "SELECT c FROM test;";
    parse_node *node = parse(test);
    char *quickstep_output = quickstep_stringify(node);
    printf("%s\n", quickstep_output);
    free(quickstep_output);

    char *json_output = json_stringify(node);
    printf("%s", json_output);
    free(json_output);

    free_tree(node);
    return 0;
}