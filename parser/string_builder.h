//
// Created by Matthew Dutson on 11/2/18.
//

#ifndef HUSTLE_PARSER_STRING_BUILDER_H
#define HUSTLE_PARSER_STRING_BUILDER_H

typedef struct string_builder {
    char *buffer;
    int buf_size;
    int end;
} string_builder;

string_builder *alloc_builder();

void free_builder(string_builder *builder);

void append_char(string_builder *builder, char c);

void append_string(string_builder *builder, char *str);

char *to_string(string_builder *builder);

#endif //HUSTLE_PARSER_STRING_BUILDER_H
