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
    int parse(std::string s);
};

#endif //HUSTLE_PARSERDRIVER_H
