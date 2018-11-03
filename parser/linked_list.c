//
// Created by Matthew Dutson on 11/2/18.
//

#include "linked_list.h"

linked_list *alloc_list() {
    linked_list *list = malloc(sizeof(linked_list));
    list->first = malloc(sizeof(list_node));
    list->last = malloc(sizeof(list_node));
    list->last->next = NULL;
    list->last->contents = NULL;
    list->first->next = list->last;
    list->first->contents = NULL;
    return list;
}

void free_list(linked_list *list) {
    list_node *current = list->first;
    list_node *next;
    // This method is not responsible for freeing contents
    while (current->next != NULL) {
        next = current->next;
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

int is_empty(linked_list *list) {
    return list->first->next->next == NULL;
}