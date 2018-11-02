//
// Created by Matthew Dutson on 11/2/18.
//

#include "linked_list.h"

linked_list *create_list() {
    linked_list *list = malloc(sizeof(linked_list));
    list->last = malloc(sizeof(list_node));
    list->last->next = NULL;
    list->last->contents = NULL;
    list->first = malloc(sizeof(list_node));
    list->first->next = list->last;
    list->first->contents = NULL;
    return list;
}

void free_list(linked_list *list) {
    list_node *current = list->first;
    list_node *next;
    while (current != NULL) {
        next = current->next;
        free(current->contents);
        free(current);
        current = next;
    }
    free(list);
}

void add_after(list_node *position, void *contents) {
    list_node *addition = malloc(sizeof(list_node));
    addition->contents = contents;
    addition->next = position->next;
    position->next = addition;
}

void add_last(linked_list *list, void *contents) {
    list_node *new_end = malloc(sizeof(list_node));
    new_end->next = NULL;
    new_end->contents = NULL;
    list->last->next = new_end;
    list->last->contents = contents;
    list->last = new_end;
}