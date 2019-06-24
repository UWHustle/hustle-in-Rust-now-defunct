#include <stdio.h>

#include "parse_node.h"

parse_node *parse_node_alloc(char *name) {
    parse_node *node = malloc(sizeof(parse_node));
    node->type = name;
    node->value_keys = dynamic_array_alloc();
    node->value_values = dynamic_array_alloc();
    node->child_keys = dynamic_array_alloc();
    node->child_values = dynamic_array_alloc();
    node->child_list_keys = dynamic_array_alloc();
    node->child_list_values = dynamic_array_alloc();
    return node;
}

void parse_node_free(parse_node *node) {
    dynamic_array_free(node->value_keys);
    dynamic_array_free(node->value_values);
    dynamic_array_free(node->child_keys);
    for (int i = 0; i < node->child_values->size; i++) {
        parse_node_free(node->child_values->array[i]);
    }
    dynamic_array_free(node->child_values);
    dynamic_array_free(node->child_list_keys);
    for (int i = 0; i < node->child_list_values->size; i++) {
        dynamic_array *child_list = node->child_list_values->array[i];
        for (int j = 0; j < child_list->size; j++) {
            parse_node_free(child_list->array[j]);
        }
    }
    dynamic_array_free(node->child_list_values);
    free(node);
}

void parse_node_add_value(parse_node *node, char *name, char *value) {
    dynamic_array_push_back(node->value_keys, name);
    dynamic_array_push_back(node->value_values, value);
}

void parse_node_add_child(parse_node *current, char *name, parse_node *child) {
    dynamic_array_push_back(current->child_keys, name);
    dynamic_array_push_back(current->child_values, child);
}

void parse_node_add_child_list(parse_node *current, char *name, dynamic_array *child_list) {
    dynamic_array_push_back(current->child_list_keys, name);
    dynamic_array_push_back(current->child_list_values, child_list);
}
