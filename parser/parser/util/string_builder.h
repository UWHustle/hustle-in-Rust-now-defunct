#ifndef HUSTLE_PARSER_STRING_BUILDER_H
#define HUSTLE_PARSER_STRING_BUILDER_H

#include <stdio.h>

#define STARTING_SIZE (size_t) 64
#define LOAD_FACTOR (size_t) 2

typedef struct string_builder {
    char *buffer;
    size_t buffer_size;
    size_t string_len;
} string_builder;

string_builder *alloc_builder();

void free_builder(string_builder *builder);

/*
 * The only format specifier allowed is %s (plus %%, which represents the
 * literal character '%').
 */
void append_fmt(string_builder *builder, char *fmt, ...);

/*
 * Converts the string builder to a null-terminated character array. The caller
 * is responsible for freeing this array!
 */
char *to_string(string_builder *builder);

#endif //HUSTLE_PARSER_STRING_BUILDER_H
