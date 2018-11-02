//
// Created by Matthew Dutson on 11/2/18.
//

#include "parse_node.h"
#include "linked_list.h"

#include <stdio.h>

void add_attribute(parse_node* current, char* name, char* value)
{
    add_last(&current->attribute_names, name);
    add_last(&current->attribute_values, value);
}

void add_child(parse_node* current, char* name, parse_node *child)
{
    add_last(&current->child_names, name);
    add_last(&current->child_values, child);
}

void add_child_list(parse_node* current, char* name, linked_list *child_list)
{
    add_last(&current->child_list_names, name);
    add_last(&current->child_list_values, child_list);
}