#include <stdio.h>

#include "parser/parser_wrapper.h"
#include "parser/stringify.h"

#define HUSTLE_VERSION "0.1.0"
#define BUFFER_SIZE 1024
#define PROMPT "hustle> "

int main(int argc, char **argv) {
    char buffer[BUFFER_SIZE];

    printf("Hustle version %s\n", HUSTLE_VERSION);

    while (!feof(stdin)) {
        printf("%s", PROMPT);
        if (fgets(buffer, BUFFER_SIZE, stdin)) {
            parse_node *node = parse(buffer);
            char *quickstep_output = quickstep_stringify(node);
            printf("%s\n", quickstep_output);
            free(quickstep_output);

            char *json_output = json_stringify(node);
            printf("%s\n", json_output);
            free(json_output);

            free(node);
        }
    }

    printf("\n");
    return 0;
}