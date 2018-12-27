#include "resolver.h"
#include "optimizer/optimizer_wrapper.hpp"

using namespace std;

void resolver::resolve(shared_ptr<ParseNode> syntax_tree, string input) {
    int res = optimizer(input);
}
