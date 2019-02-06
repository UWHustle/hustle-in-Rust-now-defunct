#include "ParserDriver.h"

#include <utility>
#include "parser.h"
#include "lexer.h"
#include "optimizer/optimizer_wrapper.hpp"

using namespace std;

ParserDriver::ParserDriver() {
    join_type = JoinNode::NONE;
};

void ParserDriver::parse_and_optimize(string sql) {
    parse(move(sql));
    optimizer(syntax_tree);
}

void ParserDriver::parse(string sql) {
    location.initialize(&sql);
    YY_BUFFER_STATE state = yy_scan_string(sql.c_str());
    yy::parser parser(*this);
    parser.set_debug_level(false);
    parser.parse();
    yy_delete_buffer(state);
}
