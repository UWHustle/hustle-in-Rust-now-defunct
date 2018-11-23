#include "parser_wrapper.h"
#include "parser.h"
#include "lexer.h"
#include "utility/stringify.h"
#include "optimizer/optimizer_wrapper.hpp"

int optimizer(char *input);

parse_node *get_parse_tree(char *command) {
    yyscan_t scanner;
    if (yylex_init(&scanner)) {
        return NULL;
    }
    YY_BUFFER_STATE state = yy_scan_string(command, scanner);

    parse_node *node;
    if (yyparse(&node, scanner)) {
        return NULL;
    }

    yy_delete_buffer(state, scanner);
    yylex_destroy(scanner);

    return node;
}

void parse(char *command) {
    parse_node *node = get_parse_tree(command);
    char *json_output = json_stringify(node);
    optimizer(command);
    free(json_output);
    free_tree(node);
}
