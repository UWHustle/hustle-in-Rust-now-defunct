#ifndef HUSTLE_RESOLVER_H
#define HUSTLE_RESOLVER_H

#include <memory>
#include <string>
#include "parser/ParseNode.h"

namespace resolver {
    void resolve(std::shared_ptr<ParseNode> syntax_tree, std::string input);
}

#endif //HUSTLE_RESOLVER_H
