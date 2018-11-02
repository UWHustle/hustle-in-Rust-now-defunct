//
// Created by Matthew Dutson on 11/2/18.
//

#ifndef HUSTLE_PARSER_NODE_H
#define HUSTLE_PARSER_NODE_H

#include "linked_list.h"

typedef struct parse_node
{
    char *name;
    linked_list attribute_names;
    linked_list attribute_values;
    linked_list child_names;
    linked_list child_values;
    linked_list child_list_names;
    linked_list child_list_values;
} parse_node;

void add_attribute(parse_node* current, char* name, char* value);

void add_child(parse_node* current, char* name, parse_node *child);

void add_child_list(parse_node* current, char* name, linked_list *child_list);

#endif // HUSTLE_PARSER_NODE_H
