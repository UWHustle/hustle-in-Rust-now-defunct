#ifndef HUSTLE_PARSER_STRINGIFY_H
#define HUSTLE_PARSER_STRINGIFY_H

#include "parse_node.h"

/*
 * Returns a JSON string representing the tree of which the node is the root.
 * The caller is responsible for freeing this string!
 */
char *json_stringify(parse_node *node);

/*
 * Returns a string which should be equivalent to
 * quickstep::TreeStringSerializable::getNodeString. The caller is responsible
 * for freeing this string!
 */
char *quickstep_stringify(parse_node *node);

#endif //HUSTLE_PARSER_STRINGIFY_H
