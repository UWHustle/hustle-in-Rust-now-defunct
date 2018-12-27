#include "ParserDriver.h"
#include "parser.h"
#include "lexer.h"
#include "optimizer/resolver/resolver.h"

ParserDriver::ParserDriver() = default;

int ParserDriver::parse(std::string s) {
    auto input = s;
    location.initialize (&s);
    YY_BUFFER_STATE state = yy_scan_string(s.c_str());
    yy::parser parser(*this);
    parser.set_debug_level(true);
    int res = parser.parse();
    yy_delete_buffer(state);
    this->syntax_tree->json_stringify();
    resolver::resolve(this->syntax_tree, input);
    return res;
}
