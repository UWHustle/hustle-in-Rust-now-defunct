#include <stdio.h>

#include "parse_node.h"

parse_node *alloc_node(char *name) {
    parse_node *node = malloc(sizeof(parse_node));
    node->type = name;
    node->attribute_names = alloc_array();
    node->attribute_values = alloc_array();
    node->child_names = alloc_array();
    node->child_values = alloc_array();
    node->list_names = alloc_array();
    node->list_values = alloc_array();
    return node;
}

void free_tree(parse_node *node) {
    free_array(node->attribute_names);
    free_array(node->attribute_values);
    free_array(node->child_names);
    free_array(node->child_values);
    free_array(node->list_names);
    free_array(node->list_values);
    free(node);
}

void add_attribute(parse_node *node, char *name, char *value) {
    add_last(node->attribute_names, name);
    add_last(node->attribute_values, value);
}

void add_child(parse_node *node, char *name, parse_node *child) {
    add_last(node->child_names, name);
    add_last(node->child_values, child);
}

void add_child_list(parse_node *node, char *name, dynamic_array *child_list) {
    add_last(node->list_names, name);
    add_last(node->list_values, child_list);
}