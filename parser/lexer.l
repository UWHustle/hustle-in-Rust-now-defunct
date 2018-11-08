%{
#include "parse_node.h"
#include "parser.h"
#include <stdio.h>
%}

%option outfile="lexer.c" header-file="lexer.h"

%option reentrant noyywrap
%option bison-bridge

%%
FROM					{ return TK_FROM; }
SELECT					{ return TK_SELECT; }
";"						{ return TK_SEMI; }
[a-zA-Z][a-zA-Z0-9]*	{ yylval->strval = strdup(yytext); return TK_ID; }
[ \t]
%%

int yyerror(const char *s) {
  fprintf(stderr, "Error: %s\n", s);
  exit(1);
}