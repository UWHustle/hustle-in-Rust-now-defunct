#include "parser_wrapper.h"
#include "parser.h"
#include "lexer.h"
#include "utility/stringify.h"
#include "resolver/resolver.h"


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
    if (node) {
        char *json_output = json_stringify(node);
        resolve(command);
        free(json_output);
        free_tree(node);
    }
}
