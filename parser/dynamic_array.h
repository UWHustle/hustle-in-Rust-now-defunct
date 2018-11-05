#ifndef HUSTLE_PARSER_DYNAMIC_ARRAY_H
#define HUSTLE_PARSER_DYNAMIC_ARRAY_H

#include <stdlib.h>

#define STARTING_SIZE (size_t) 64
#define LOAD_FACTOR (size_t) 2

typedef struct dynamic_array {
    void **array;
    size_t max_size;
    size_t size;
} dynamic_array;

/*
 * Allocates an array of size STARTING_SIZE * sizeof(void*) on the heap. Sets
 * the size tracker to 0.
 */
dynamic_array *alloc_array();

/*
 * Frees the heap-allocated dynamic array passed as an argument.
 */
void free_array(dynamic_array *array);

/*
 * Adds the specified item to the end of the array, incrementing the size
 * counter and resizing the array if needed.
 */
void add_last(dynamic_array *array, void *item);

/*
 * Returns nonzero iff the array has size > 0.
 */
int is_empty(dynamic_array *array);

#endif // HUSTLE_PARSER_DYNAMIC_ARRAY_H