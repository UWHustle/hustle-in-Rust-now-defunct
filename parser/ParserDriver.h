#ifndef HUSTLE_PARSERDRIVER_H
#define HUSTLE_PARSERDRIVER_H

#include "parser.h"

#define YY_DECL yy::parser::symbol_type yylex(ParserDriver& drv)
YY_DECL;

class ParserDriver {
public:
    std::unique_ptr<ParseNode> syntax_tree;
    yy::location location;
    ParserDriver();
    int parse(std::string s);
private:

};

#endif //HUSTLE_PARSERDRIVER_H
