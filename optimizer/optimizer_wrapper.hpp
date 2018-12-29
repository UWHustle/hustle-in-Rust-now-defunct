#ifndef HUSTLE_OPTIMIZER_OPTIMIZER_WRAPPER_H
#define HUSTLE_OPTIMIZER_OPTIMIZER_WRAPPER_H

#include <memory>
#include <string>
#include "parser/ParseNode.h"

int optimizer(std::shared_ptr<ParseNode> syntax_tree);

#endif //HUSTLE_OPTIMIZER_OPTIMIZER_WRAPPER_H
