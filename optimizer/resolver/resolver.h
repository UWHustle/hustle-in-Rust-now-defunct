#ifndef HUSTLE_RESOLVER_H
#define HUSTLE_RESOLVER_H

#include "parser/ParseNode.h"

#ifdef __cplusplus
namespace resolver {
    extern "C" {
#endif
        void resolve(parse_node* node, char* input);
#ifdef __cplusplus
    }
}
#endif

#endif //HUSTLE_RESOLVER_H
