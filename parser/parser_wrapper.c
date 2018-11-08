#include "parser_wrapper.h"

#include "parser.h"
#include "lexer.h"

parse_node *parse(char *command) {
    parse_node *result;
    yyscan_t scanner;
    YY_BUFFER_STATE state;
    int parse_status;
    int lex_status;

    lex_status = yylex_init(&scanner);
    if (lex_status) return NULL;

    state = yy_scan_string(command, scanner);

    parse_status = yyparse(&result, scanner);
    if (parse_status) return NULL;

    yy_delete_buffer(state, scanner);
    yylex_destroy(scanner);

    return result;
}