#include "parser_wrapper.h"
#include "parser.h"
#include "lexer.h"

parse_node *parse(char *command) {
    yyscan_t scanner;
    if (yylex_init(&scanner)) {
        return NULL;
    }
    YY_BUFFER_STATE state = yy_scan_string(command, scanner);

    parse_node *result;
    if (yyparse(&result, scanner)) {
        return NULL;
    }

    yy_delete_buffer(state, scanner);
    yylex_destroy(scanner);
    return result;
}