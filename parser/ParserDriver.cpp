#include "ParserDriver.h"
#include "parser.h"
#include "lexer.h"
#include "optimizer/optimizer_wrapper.hpp"

ParserDriver::ParserDriver() = default;

int ParserDriver::parse(std::string s) {
    auto input = s;
    location.initialize (&s);
    YY_BUFFER_STATE state = yy_scan_string(s.c_str());
    yy::parser parser(*this);
    parser.set_debug_level(false);
    int res = parser.parse();
    std::cout << syntax_tree->to_sql_string() << std::endl;
    yy_delete_buffer(state);
//    this->syntax_tree->json_stringify();
    optimizer(syntax_tree);
    return res;
}
