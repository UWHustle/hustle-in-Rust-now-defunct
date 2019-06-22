#include "parse_context.h"

parse_context *parse_context_alloc() {
    parse_context *context = malloc(sizeof(parse_context));
    return context;
}

void parse_context_free(parse_context *context) {
    parse_node_free(context->ast);
    free(context);
}
