#include "parser_wrapper.h"
#include "parser.h"
#include "lexer.h"
#include "util/stringify.h"

char *c_parse(char *sql) {
    yyscan_t scanner;
    if (yylex_init(&scanner)) {
        return NULL;
    }
    YY_BUFFER_STATE state = yy_scan_string(sql, scanner);

    parse_node *node;
    if (yyparse(&node, scanner)) {
        return NULL;
    }

    yy_delete_buffer(state, scanner);
    yylex_destroy(scanner);

    return json_stringify(node);
}
