#include "parser_wrapper.h"
#include "parser.h"
#include "lexer.h"
#include "utility/stringify.h"
#include "optimizer/example.hpp"

int optimizer(char *input);
parse_node *parse(char *command) {
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

    char *json_output = json_stringify(node);

    printf("%s\n", json_output);

    optimizer(json_output);

    free(json_output);
    free(node);
}
