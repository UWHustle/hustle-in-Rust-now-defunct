%{
#include "parse_node.h"
#include "parser.h"
#include "lexer.h"

void yyerror(parse_node **s, yyscan_t scanner, char const *msg);
%}

%output "parser.c"
%defines "parser.h"

%code requires {
#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif
}

%require "3.2"

%define api.pure full
%lex-param   { yyscan_t scanner }
%parse-param { parse_node **statement }
%parse-param { yyscan_t scanner }

%define api.token.prefix {TK_}
%define parse.error verbose

%union {
  int intval;
  char *strval;
  parse_node *statement;
}


%token FROM
%token SELECT
%token SEMI
%token <strval> ID

%type <strval> name id
%type <statement> cmd select_column_list from
    select_table_list

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
  SELECT distinct select_column_list from where_opt groupby_opt having_opt orderby_opt limit_opt {
    $$ = alloc_node("SetOperationStatement");
    parse_node *select_query = alloc_node("SetOperation");
    add_child($$, "set_operation_query", select_query);
    add_attribute(select_query, "set_operation_type", "Select");

    dynamic_array *clauses = alloc_array();
    add_child_list(select_query, "children", clauses);

    parse_node *select_child = alloc_node("Select");
    add_last(clauses, select_child);

    add_child(select_child, "select_clause", $3);
    add_child(select_child, "from_clause", $4);
  }
;

distinct:
  /* empty */
;

select_column_list:
  sclp scanpt expr scanpt as {
    $$ = alloc_node("SelectStar");
  }
;

sclp:
  /* empty */
;

scanpt:
  /* empty */
;

expr:
  id
;

from:
  FROM select_table_list {
    $$ = $2;
  }
;

select_table_list:
  stl_prefix name dbname as indexed_opt on_opt using_opt {
    $$ = alloc_node("");
    parse_node *table_ref = alloc_node("TableReference");
    add_child($$, "", table_ref);
    add_attribute(table_ref, "table", $2);
  }
;

stl_prefix:
  /* empty */
;

name:
  id
;

id:
  ID
;

dbname:
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