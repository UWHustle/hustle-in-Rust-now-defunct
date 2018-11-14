#include <stdio.h>

#include "../parser/parser_wrapper.h"

#define HUSTLE_VERSION "0.1.0"
#define BUFFER_SIZE 1024
#define PROMPT "hustle> "

int main(int argc, char **argv) {
    char buffer[BUFFER_SIZE];

    printf("Hustle version %s\n", HUSTLE_VERSION);

    while (!feof(stdin)) {
        printf("%s", PROMPT);
        if (fgets(buffer, BUFFER_SIZE, stdin)) {
            parse(buffer);
        }
    }

    printf("\n");
    return 0;
}