//
// Created by Matthew Dutson on 11/2/18.
//

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "string_builder.h"

string_builder *create_builder() {
    string_builder *builder = malloc(sizeof(string_builder));
    builder->buf_size = 64;
    builder->end = 0;
    builder->buffer = malloc((size_t) builder->buf_size);
    return builder;
}

void free_builder(string_builder *builder) {
    free(builder->buffer);
    free(builder);
}

void append_char(string_builder *builder, char c) {
    builder->buffer[builder->end] = c;
    builder->end++;
    if (builder->end == builder->buf_size) {
        char *new_mem = realloc(builder->buffer, builder->buf_size * (size_t) 2);
        memset(new_mem + builder->buf_size, 0, builder->buf_size);
        builder->buffer = new_mem;
        builder->buf_size *= 2;
    }
}

// todo: potential security issue?
void append_string(string_builder *builder, char *str) {
    while (*str != '\0') {
        append_char(builder, *str);
        str++;
    }
}

char *to_string(string_builder *builder) {
    char *output = malloc(builder->end + (size_t) 1);
    for (int i = 0; i < builder->end; i++) {
        output[i] = builder->buffer[i];
    }
    return output;
}