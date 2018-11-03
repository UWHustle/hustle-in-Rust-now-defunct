//
// Created by Matthew Dutson on 11/2/18.
//

#include <stdio.h>
#include <string.h>

#include "parse_node.h"
#include "stringify.h"

int main(int argc, char **argv) {
    parse_node *head = alloc_node("head");
    add_attribute(head, "attr1", "attr_val1");
    add_attribute(head, "attr2", "attr_val2");
    parse_node *child1 = alloc_node("child1");
    parse_node *child2 = alloc_node("child2");
    add_child(head, "child1", child1);
    add_child(head, "child2", child2);
    linked_list *list = alloc_list();
    parse_node *list_child1 = alloc_node("list_child1");
    parse_node *list_child2 = alloc_node("list_child2");
    add_last(list, list_child1);
    add_last(list, list_child2);
    add_child_list(head, "list", list);

    printf("%s", json_stringify(head));

    free_node(list_child2);
    free_node(list_child1);
    free_list(list);
    free_node(child2);
    free_node(child1);
    free_node(head);
}