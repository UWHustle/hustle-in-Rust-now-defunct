#include <stdio.h>

#include "parse_node.h"
#include "stringify.h"
#include "parser.h"
#include "lexer.h"

parse_node *get_ast(char *command) {
    parse_node *statement;
    yyscan_t scanner;
    YY_BUFFER_STATE state;
    int parse_status;
    int lex_status;

    lex_status = yylex_init(&scanner);
    if (lex_status) return NULL;

    state = yy_scan_string(command, scanner);

    parse_status = yyparse(&statement, scanner);
    if (parse_status) return NULL;

    yy_delete_buffer(state, scanner);
    yylex_destroy(scanner);

    return statement;
}

int main(void) {
    char test[] = "SELECT * FROM test;";
    parse_node *statement = get_ast(test);
    char *quickstep_output = quickstep_stringify(statement);
    printf("%s\n", quickstep_output);
    free(quickstep_output);

    char *json_output = json_stringify(statement);
    printf("%s", json_output);
    free(json_output);

    free(statement);
    return 0;
}