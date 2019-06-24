#include "dynamic_array.h"

dynamic_array *dynamic_array_alloc() {
    dynamic_array *array = malloc(sizeof(dynamic_array));
    array->max_size = STARTING_SIZE;
    array->size = 0;
    array->array = malloc(sizeof(void *) * array->max_size);
    return array;
}

void dynamic_array_free(dynamic_array *array) {
    free(array->array);
    free(array);
}

void dynamic_array_push_back(dynamic_array *array, void *item) {
    array->array[array->size] = item;
    array->size++;
    if (array->size == array->max_size) {
        array->max_size *= LOAD_FACTOR;
        array->array = realloc(array->array, array->max_size);
    }
}

int dynamic_array_is_empty(dynamic_array *array) {
    return array->size == 0;
}
