#include "resolver.h"
#include "optimizer/optimizer_wrapper.hpp"

void resolver::resolve(parse_node* node, char* input) {
    optimizer(input);
}