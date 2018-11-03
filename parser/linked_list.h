//
// Created by Matthew Dutson on 11/2/18.
//

#ifndef HUSTLE_PARSER_LINKED_LIST_H
#define HUSTLE_PARSER_LINKED_LIST_H

#include <stdlib.h>

typedef struct list_node list_node;
struct list_node {
    list_node *next;
    void *contents;
};

typedef struct linked_list {
    list_node *first;
    list_node *last;
} linked_list;

linked_list *alloc_list();

void free_list(linked_list *list);

void add_after(list_node *position, void *contents);

void add_last(linked_list *list, void *contents);

int is_empty(linked_list *list);

#endif //HUSTLE_PARSER_LINKED_LIST_H
