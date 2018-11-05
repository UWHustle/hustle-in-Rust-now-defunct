#include <string.h>

#include "stringify.h"
#include "string_builder.h"

#define INDENT_SIZE 4

void new_line(string_builder *builder, int indent) {
    append_fmt(builder, "\n");
    for (int i = 0; i < indent * INDENT_SIZE; i++) {
        append_fmt(builder, " ");
    }
}

void json_traverse(string_builder *builder, parse_node *node, int indent) {
    append_fmt(builder, "{");
    indent++;
    new_line(builder, indent);
    append_fmt(builder, "\"name\": \"%s\"", node->name);

    for (size_t i = 0; i < node->attribute_names->size; i++) {
        append_fmt(builder, ",");
        new_line(builder, indent);
        char *name = node->attribute_names->array[i];
        char *value = node->attribute_values->array[i];
        append_fmt(builder, "\"%s\": \"%s\"", name, value);
    }

    for (size_t i = 0; i < node->child_names->size; i++) {
        append_fmt(builder, ",");
        new_line(builder, indent);
        char *name = node->child_names->array[i];
        append_fmt(builder, "\"%s\": ", name);

        parse_node *child = node->child_values->array[i];
        json_traverse(builder, child, indent);
    }

    for (size_t i = 0; i < node->list_names->size; i++) {
        append_fmt(builder, ",");
        new_line(builder, indent);
        char *name = node->list_names->array[i];
        append_fmt(builder, "\"%s\": [", name);

        dynamic_array *child_list = node->list_values->array[i];
        for (size_t j = 0; j < child_list->size; j++) {
            new_line(builder, indent + 1);
            json_traverse(builder, child_list->array[i], indent + 1);
            if (j < child_list->size - 1) {
                append_fmt(builder, ",");
            }
        }
        new_line(builder, indent);
        append_fmt(builder, "]");
    }
    new_line(builder, indent - 1);
    append_fmt(builder, "}");
}

char *json_stringify(parse_node *node) {
    string_builder *builder = alloc_builder();
    json_traverse(builder, node, 0);
    char *output = to_string(builder);
    free_builder(builder);
    return output;
}

void add_indented_prefix(string_builder *prefix, int is_last) {
    if (!is_last) {
        append_fmt(prefix, "| ");
    } else {
        append_fmt(prefix, "  ");
    }
}

void quickstep_traverse(string_builder *builder, parse_node *node, int is_root,
                        int is_last, string_builder prefix,
                        char *parent_name) {
    append_fmt(builder, to_string(&prefix));
    if (!is_root) {
        append_fmt(builder, "+-");
    }
    if (strlen(parent_name) > 0) {
        append_fmt(builder, "%s=", parent_name);
    }
    append_fmt(builder, node->name);

    if (!is_empty(node->attribute_names)) {
        append_fmt(builder, "[");
        for (size_t i = 0; i < node->attribute_names->size; i++) {
            char *name = node->attribute_names->array[i];
            char *value = node->attribute_values->array[i];
            append_fmt(builder, "%s=%s", name, value);
            if (i < node->attribute_names->size - 1) {
                append_fmt(builder, ",");
            }
        }
        append_fmt(builder, "]");
    }
    append_fmt(builder, "\n");

    if (!is_root) {
        add_indented_prefix(&prefix, is_last);
    }

    for (size_t i = 0; i < node->child_names->size; i++) {
        char *name = node->child_names->array[i];
        parse_node *child = node->child_values->array[i];
        int child_is_last = i == node->child_names->size - 1 &&
                            is_empty(node->list_names);
        quickstep_traverse(builder, child, 0, child_is_last, prefix, name);
    }

    for (size_t i = 0; i < node->list_names->size; i++) {
        char *name = node->list_names->array[i];
        if (strlen(name) > 0) {
            append_fmt(builder, "%s+-%s=\n", to_string(&prefix), name);
        }
        add_indented_prefix(&prefix, is_last);

        dynamic_array *child_list = node->list_values->array[i];
        for (size_t j = 0; j < child_list->size; j++) {
            int child_is_last = child_list->size - 1 &&
                                node->list_names->size - 1;
            quickstep_traverse(builder, child_list->array[i], 0, child_is_last,
                               prefix, "");
            if (j < child_list->size - 1) {
                append_fmt(builder, ",");
            }
        }
        if (child_list->size == 0) {
            append_fmt(builder, "+-[]\n");
        }
    }
}

char *quickstep_stringify(parse_node *node) {
    string_builder *builder = alloc_builder();
    string_builder *prefix = alloc_builder();
    quickstep_traverse(builder, node, 1, 1, *prefix, "");
    char *output = to_string(builder);
    free_builder(builder);
    free_builder(prefix);
    return output;
}