#ifndef HUSTLE_PARSER_PARSE_NODE_H
#define HUSTLE_PARSER_PARSE_NODE_H

#include "util/dynamic_array.h"

typedef struct parse_node {
    char *type;
    dynamic_array *value_keys;
    dynamic_array *value_values;
    dynamic_array *child_keys;
    dynamic_array *child_values;
    dynamic_array *child_list_keys;
    dynamic_array *child_list_values;
} parse_node;

/*
 * Allocates the parse node and its underlying dynamics arrays on the heap.
 */
parse_node *parse_node_alloc(char *name);

/*
 * Free all nodes and arrays rooted at this node. It is assumed all nodes were
 * created with parse_node_alloc and all arrays were created with dynamic_array_alloc,
 * otherwise this may result in an error. This function does NOT free strings
 * contained in the *_names arrays (these are typically literals in the code
 * segment).
 */
void parse_node_free(parse_node *node);

/*
 * Adds the inline attribute name=value by appending to value_keys and
 * value_values.
 */
void parse_node_add_value(parse_node *node, char *name, char *value);

/*
 * Adds the child parse node by appending to child_keys and child_values.
 */
void parse_node_add_child(parse_node *current, char *name, parse_node *child);

/*
 * Adds the child list by appending to child_list_keys and child_list_values.
 */
void parse_node_add_child_list(parse_node *current, char *name, dynamic_array *child_list);

#endif // HUSTLE_PARSER_PARSE_NODE_H
