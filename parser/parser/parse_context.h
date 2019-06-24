#ifndef HUSTLE_PARSE_CONTEXT_H
#define HUSTLE_PARSE_CONTEXT_H

#include "parse_node.h"

typedef struct parse_context {
    parse_node *ast;
    char *err;
} parse_context;

parse_context *parse_context_alloc();

void parse_context_free(parse_context *context);

#endif //HUSTLE_PARSE_CONTEXT_H
