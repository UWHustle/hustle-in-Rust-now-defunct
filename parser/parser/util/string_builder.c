#include <stdlib.h>
#include <stdarg.h>

#include "string_builder.h"

string_builder *alloc_builder() {
    string_builder *builder = malloc(sizeof(string_builder));
    builder->buffer_size = STARTING_SIZE;
    builder->string_len = 0;
    builder->buffer = malloc(builder->buffer_size);
    return builder;
}

void free_builder(string_builder *builder) {
    free(builder->buffer);
    free(builder);
}

void append_char(string_builder *builder, char c) {
    builder->buffer[builder->string_len] = c;
    builder->string_len++;
    if (builder->string_len == builder->buffer_size) {
        builder->buffer = realloc(builder->buffer,
                                  builder->buffer_size * LOAD_FACTOR);
        builder->buffer_size *= LOAD_FACTOR;
    }
}

void append_string(string_builder *builder, char *str) {
    while (*str != '\0') {
        append_char(builder, *str);
        str++;
    }
}

void append_fmt(string_builder *builder, char *fmt, ...) {
    va_list arg_list;
    va_start(arg_list, fmt);
    for (; *fmt != '\0'; fmt++) {
        if (*fmt != '%') {
            append_char(builder, *fmt);
        } else {
            fmt++;
            if (*fmt == 's') {
                append_string(builder, va_arg(arg_list, char *));
            } else if (*fmt == '%') {
                append_char(builder, *fmt);
            } else {
                fprintf(stderr, "Invalid format specifier: %%%c", *fmt);
                exit(-1);
            }
        }
    }
}

char *to_string(string_builder *builder) {
    char *output = malloc(builder->string_len + (size_t) 1);
    for (int i = 0; i < builder->string_len; i++) {
        output[i] = builder->buffer[i];
    }
    output[builder->string_len] = '\0';
    return output;
}
