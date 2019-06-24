#include "parser_wrapper.h"
#include "parser.h"
#include "lexer.h"
#include "util/stringify.h"

int c_parse(char *sql, char **result) {
    yyscan_t scanner;
    if (yylex_init(&scanner)) {
        *result = "Error initializing scanner";
        return 1;
    }
    YY_BUFFER_STATE state = yy_scan_string(sql, scanner);

    parse_context *context = parse_context_alloc();
    if (yyparse(context, scanner)) {
        *result = context->err;
        return 1;
    }

    yy_delete_buffer(state, scanner);
    yylex_destroy(scanner);

    *result = json_stringify(context->ast);
    parse_context_free(context);
    return 0;
}
