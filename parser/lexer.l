%{
#include "parse_node.h"
#include "parser.h"
#include <stdio.h>
%}

%option outfile="lexer.c" header-file="lexer.h"

%option reentrant noyywrap
%option bison-bridge

%%
BY                      { return TK_BY; }
FROM					{ return TK_FROM; }
GROUP                   { return TK_GROUP; }
SELECT					{ return TK_SELECT; }
","                     { return TK_COMMA; }
"("                     { return TK_LP; }
")"                     { return TK_RP; }
";"						{ return TK_SEMI; }
[a-zA-Z][a-zA-Z0-9_]*	{ yylval->strval = strdup(yytext); return TK_ID; }
[ \t]
%%

void yyerror(parse_node **s, yyscan_t scanner, char const *msg) {
  fprintf(stderr, "Error: %s\n", msg);
  exit(1);
}
