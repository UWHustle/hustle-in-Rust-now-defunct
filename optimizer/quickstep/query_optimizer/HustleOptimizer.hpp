#ifndef QUICKSTEP_HUSTLEOPTIMIZER_H
#define QUICKSTEP_HUSTLEOPTIMIZER_H
#include <iostream>
#include <memory>
#include "parser/ParseNode.h"

std::string hustle_optimize(const std::shared_ptr<ParseNode> &syntax_tree, const std::string &sql = std::string());

#endif //QUICKSTEP_HUSTLEOPTIMIZER_H
