#ifndef HUSTLE_PARSER_WRAPPER_H
#define HUSTLE_PARSER_WRAPPER_H

#include "parse_node.h"
#ifdef __cplusplus
extern "C" {
#endif
void parse(char *command);
parse_node *get_parse_tree(char *command);
#ifdef __cplusplus
}
#endif


#endif //HUSTLE_PARSER_WRAPPER_H
