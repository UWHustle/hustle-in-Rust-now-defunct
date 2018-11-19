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

%require "3.1"

%define api.pure full
%lex-param   { yyscan_t scanner }
%parse-param { parse_node **statement }
%parse-param { yyscan_t scanner }

%define api.token.prefix {TK_}
%define parse.error verbose

%union {
  int intval;
  char *strval;
  parse_node *node;
  dynamic_array *list;
}

%token COMMA
%token FROM
%token LP
%token RP
%token SELECT
%token SEMI
%token <strval> ID

%type <strval>
  name
  id
%type <node>
  cmd
  expression
%type <list>
  from
  select_table_list
  select_column_list

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
    $$ = alloc_node("ParseStatementSetOperation");

    parse_node *select_query = alloc_node("ParseSetOperation");
    add_child($$, "set_operation_query", select_query);
    add_attribute(select_query, "set_operation_type", "Select");

    dynamic_array *operands = alloc_array();
    add_child_list(select_query, "operands", operands);

    parse_node *select_child = alloc_node("ParseSelect");
    add_last(operands, select_child);

    add_child_list(select_child, "selection", $3);
    add_child_list(select_child, "from_list", $4);
  }
;

distinct:
  /* empty */
;

select_column_list:
  sclp scanpt expression scanpt as {
    $$ = alloc_array();
    parse_node *selection_item = alloc_node("ParseSelectionItem");
    add_child(selection_item, "expression", $3);
    add_last($$, selection_item);
  }
;

sclp:
  /* empty */
;

scanpt:
  /* empty */
;

expression:
  id {
    $$ = alloc_node("AttributeReference");
    add_attribute($$, "attribute_name", $1);
  }
| id LP distinct expression_list RP {
    $$ = alloc_node("FunctionCall");
    add_attribute($$, "name", $1);
  }
;

expression_list:
  next_expression_list
| /* empty */
;

next_expression_list:
  next_expression_list COMMA expression
| expression
;

from:
  FROM select_table_list {
    $$ = $2;
  }
;

select_table_list:
  stl_prefix name db_name as indexed_opt on_opt using_opt {
    $$ = alloc_array();
    parse_node *table_ref = alloc_node("ParseSimpleTableReference");
    add_attribute(table_ref, "table_name", $2);
    add_last($$, table_ref);
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

db_name:
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