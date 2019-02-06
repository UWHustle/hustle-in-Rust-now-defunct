#ifndef HUSTLE_PARSERDRIVER_H
#define HUSTLE_PARSERDRIVER_H

#include "parser.h"
#include "JoinNode.h"

#define YY_DECL yy::parser::symbol_type yylex(ParserDriver& drv)
YY_DECL;

class ParserDriver {
public:
    std::shared_ptr<ParseNode> syntax_tree;
    yy::location location;
    JoinNode::JoinType join_type;
    ParserDriver();
    void parse_and_optimize(std::string sql);
    void parse(std::string sql);
};

#endif //HUSTLE_PARSERDRIVER_H
