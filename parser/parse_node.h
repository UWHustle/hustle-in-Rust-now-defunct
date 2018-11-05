#ifndef HUSTLE_PARSER_PARSE_NODE_H
#define HUSTLE_PARSER_PARSE_NODE_H

#include "dynamic_array.h"

typedef struct parse_node {
    char *name;
    dynamic_array *attribute_names;
    dynamic_array *attribute_values;
    dynamic_array *child_names;
    dynamic_array *child_values;
    dynamic_array *child_list_names;
    dynamic_array *child_lists;
} parse_node;

/*
 * Allocates the parse node and its underlying dynamics arrays on the heap.
 */
parse_node *alloc_node(char *name);

/*
 * Frees the heap-allocated parse node and its underlying dynamic arrays.
 */
void free_node(parse_node *node);

/*
 * Adds the inline attribute name=value by appending to attribute_names and
 * attribute_values.
 */
void add_attribute(parse_node *node, char *name, char *value);

/*
 * Adds the child parse node by appending to child_names and child_values.
 */
void add_child(parse_node *current, char *name, parse_node *child);

/*
 * Adds the child list by appending to child_list_names and child_lists.
 */
void
add_child_list(parse_node *current, char *name, dynamic_array *child_list);

#endif // HUSTLE_PARSER_PARSE_NODE_H