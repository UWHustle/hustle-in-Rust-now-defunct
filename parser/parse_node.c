//
// Created by Matthew Dutson on 11/2/18.
//

#include "parse_node.h"
#include "linked_list.h"

#include <stdio.h>

parse_node *alloc_node(char *name) {
    parse_node *node = malloc(sizeof(parse_node));
    node->name = name;
    node->attribute_names = alloc_list();
    node->attribute_values = alloc_list();
    node->child_names = alloc_list();
    node->child_values = alloc_list();
    node->child_list_names = alloc_list();
    node->child_lists = alloc_list();
    return node;
}

void free_node(parse_node *node) {
    free_list(node->attribute_names);
    free_list(node->attribute_values);
    free_list(node->child_names);
    free_list(node->child_values);
    free_list(node->child_list_names);
    free_list(node->child_lists);
    free(node);
}

void add_attribute(parse_node *current, char *name, char *value) {
    add_last(current->attribute_names, name);
    add_last(current->attribute_values, value);
}

void add_child(parse_node *current, char *name, parse_node *child) {
    add_last(current->child_names, name);
    add_last(current->child_values, child);
}

void add_child_list(parse_node *current, char *name, linked_list *child_list) {
    add_last(current->child_list_names, name);
    add_last(current->child_lists, child_list);
}