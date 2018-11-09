#ifndef HUSTLE_PARSER_PARSE_NODE_H
#define HUSTLE_PARSER_PARSE_NODE_H

#include "utility/dynamic_array.h"

typedef struct parse_node {
    char *type;
    dynamic_array *attribute_names;
    dynamic_array *attribute_values;
    dynamic_array *child_names;
    dynamic_array *child_values;
    dynamic_array *list_names;
    dynamic_array *list_values;
} parse_node;

/*
 * Allocates the parse node and its underlying dynamics arrays on the heap.
 */
parse_node *alloc_node(char *name);

/*
 * Free all nodes and arrays rooted at this node. It is assumed all nodes were
 * created with alloc_node and all arrays were created with alloc_array,
 * otherwise this may result in an error. This function does NOT free strings
 * contained in the *_names arrays (these are typically literals in the code
 * segment).
 */
void free_tree(parse_node *node);

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
 * Adds the child list by appending to list_names and list_values.
 */
void
add_child_list(parse_node *current, char *name, dynamic_array *child_list);

#endif // HUSTLE_PARSER_PARSE_NODE_H