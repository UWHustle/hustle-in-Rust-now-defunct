%{
#include "statement.h"
#include "parser.h"
#include "lexer.h"

void yyerror(Statement **s, yyscan_t scanner, char const *msg);
%}

%output "parser.c"
%defines "parser.h"

%code requires {
#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif
}

%define api.pure full
%lex-param   { yyscan_t scanner }
%parse-param { Statement **statement }
%parse-param { yyscan_t scanner }

%define api.token.prefix {TK_}
%define parse.error verbose

%union {
  int intval;
  char *strval;
  Statement *statement;
}


%token FROM
%token SELECT
%token SEMI
%token STAR
%token <strval> ID

%type <strval> nm id
%type <statement> cmd

%%
input:
  cmdlist
;

cmdlist:
  cmdlist ecmd
| ecmd
;

ecmd:
  SEMI
| cmdx SEMI
;

cmdx: 
  cmd { *statement = $1;}
;

cmd:
  SELECT distinct selcollist from where_opt groupby_opt having_opt orderby_opt limit_opt { $$ = create_statement(SELECT); }
;

distinct:
  /* empty */
;

selcollist:
  sclp scanpt STAR
;

sclp:
  /* empty */
;

scanpt:
  /* empty */
;

from:
  FROM seltablist
;

seltablist:
  stl_prefix nm dbnm as indexed_opt on_opt using_opt
;

stl_prefix:
  /* empty */
;

nm:
  id
;

id:
  ID
;

dbnm:
  /* empty */
;

as:
  /* empty */
;

indexed_opt:
  /* empty */
;

on_opt:
  /* empty */
;

using_opt:
  /* empty */
;

where_opt:
  /* empty */
;

groupby_opt:
  /* empty */
;

having_opt:
  /* empty */
;

orderby_opt:
  /* empty */
;

limit_opt:
  /* empty */
;
%%