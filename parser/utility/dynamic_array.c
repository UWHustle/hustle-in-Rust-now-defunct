#include "dynamic_array.h"

dynamic_array *alloc_array() {
    dynamic_array *array = malloc(sizeof(dynamic_array));
    array->max_size = STARTING_SIZE;
    array->size = 0;
    array->array = malloc(array->max_size);
    return array;
}

void free_array(dynamic_array *array) {
    free(array->array);
    free(array);
}

void add_last(dynamic_array *array, void *item) {
    array->array[array->size] = item;
    array->size++;
    if (array->size == array->max_size) {
        array->array = realloc(array->array, array->max_size * LOAD_FACTOR);
        array->max_size *= LOAD_FACTOR;
    }
}

int is_empty(dynamic_array *array) {
    return array->size == 0;
}