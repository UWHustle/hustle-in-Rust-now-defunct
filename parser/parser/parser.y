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

%require "3.0"

%define api.pure full
%lex-param   { yyscan_t scanner }
%parse-param { parse_node **parse_tree }
%parse-param { yyscan_t scanner }

%define api.token.prefix {TK_}
%define parse.error verbose

%union {
  int intval;
  char *strval;
  parse_node *node;
  dynamic_array *list;
}

%token ABORT
%token ACTION
%token ADD
%token AFTER
%token ALL
%token ALTER
%token ANALYZE
%token AND
%token ANY
%token AS
%token ASC
%token ATTACH
%token AUTOINCR
%token BEFORE
%token BEGIN
%token BETWEEN
%token BITAND
%token BITNOT
%token BITOR
%token <strval> BLOB
%token BY
%token CASCADE
%token CASE
%token CAST
%token CHECK
%token COLLATE
%token COLUMNKW
%token COMMA
%token COMMIT
%token CONCAT
%token CONFLICT
%token CONSTRAINT
%token CREATE
%token <strval> CTIME_KW
%token CURRENT
%token DATABASE
%token DEFAULT
%token DEFERRABLE
%token DEFERRED
%token DELETE
%token DESC
%token DETACH
%token DISTINCT
%token DO
%token DOT
%token DROP
%token EACH
%token ELSE
%token END
%token EQ
%token ESCAPE
%token EXCEPT
%token EXCLUSIVE
%token EXISTS
%token EXPLAIN
%token FAIL
%token FILTER
%token <strval> FLOAT
%token FOLLOWING
%token FOR
%token FOREIGN
%token FROM
%token GE
%token GROUP
%token GT
%token HAVING
%token <strval> ID
%token IF
%token IGNORE
%token IMMEDIATE
%token IN
%token INDEX
%token <strval> INDEXED
%token INITIALLY
%token INSERT
%token INSTEAD
%token <strval> INTEGER
%token INTERSECT
%token INTO
%token IS
%token ISNULL
%token JOIN
%token <strval> JOIN_KW
%token KEY
%token LE
%token LIKE_KW
%token LIMIT
%token LP
%token LSHIFT
%token LT
%token MATCH
%token MINUS
%token NE
%token NO
%token NOT
%token NOTHING
%token NOTNULL
%token <strval> NULL
%token OF
%token OFFSET
%token ON
%token OR
%token ORDER
%token OVER
%token PARTITION
%token PLAN
%token PLUS
%token PRAGMA
%token PRECEDING
%token PRIMARY
%token QUERY
%token RAISE
%token RANGE
%token RECURSIVE
%token REFERENCES
%token REINDEX
%token RELEASE
%token REM
%token RENAME
%token REPLACE
%token RESTRICT
%token ROLLBACK
%token ROW
%token ROWS
%token RP
%token RSHIFT
%token SAVEPOINT
%token SELECT
%token SEMI
%token SET
%token SLASH
%token STAR
%token <strval> STRING
%token TABLE
%token TEMP
%token THEN
%token TO
%token TRANSACTION
%token TRIGGER
%token UNBOUNDED
%token UNION
%token UNIQUE
%token UPDATE
%token USING
%token VACUUM
%token VALUES
%token VARIABLE
%token VIEW
%token VIRTUAL
%token WHEN
%token WHERE
%token WINDOW
%token WITH
%token WITHOUT

%left OR
%left AND
%right NOT
%left IS MATCH LIKE_KW BETWEEN IN ISNULL NOTNULL NE EQ
%left GT LE LT GE
%right ESCAPE
%left BITAND BITOR LSHIFT RSHIFT
%left PLUS MINUS
%left STAR SLASH REM
%left CONCAT
%left COLLATE
%right BITNOT
%nonassoc ON

%type <strval>
  id
  ids
  nm
  signed
  plus_num
  minus_num
  number
%type <node>
  cmd
  expr
  select
  selectnowith
  oneselect
  from
  stl_prefix
  seltablist
  on_opt
  where_opt
  columnname
  term
  values
  xfullname
  fullname
  typetoken
  typename
%type <list>
  exprlist
  groupby_opt
  nexprlist
  sclp
  selcollist
  create_table_args
  columnlist
  idlist_opt
  idlist
  setlist

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
| explain cmdx SEMI
;

explain:
  EXPLAIN
| EXPLAIN QUERY PLAN
;

cmdx:
  cmd { *parse_tree = $1; }
;

cmd:
  BEGIN transtype trans_opt { yyerror(NULL, scanner, "BEGIN not yet supported"); }
| COMMIT trans_opt { yyerror(NULL, scanner, "COMMIT not yet supported"); }
| END trans_opt { yyerror(NULL, scanner, "END not yet supported"); }
| ROLLBACK trans_opt { yyerror(NULL, scanner, "ROLLBACK not yet supported"); }
| SAVEPOINT nm { yyerror(NULL, scanner, "SAVEPOINT not yet supported"); }
| RELEASE savepoint_opt nm { yyerror(NULL, scanner, "RELEASE not yet supported"); }
| ROLLBACK trans_opt TO savepoint_opt nm { yyerror(NULL, scanner, "ROLLBACK not yet supported"); }
| createkw temp TABLE ifnotexists nm dbnm create_table_args {
    $$ = parse_node_alloc("create_table");
    parse_node_add_value($$, "name", $5);
    parse_node_add_child_list($$, "attributes", $7);
  }
| DROP TABLE ifexists fullname {
    $$ = parse_node_alloc("drop_table");
    parse_node_add_child($$, "name", $4);
  }
| createkw temp VIEW ifnotexists nm dbnm eidlist_opt AS select { yyerror(NULL, scanner, "CREATE VIEW not yet supported"); }
| DROP VIEW ifexists fullname { yyerror(NULL, scanner, "DROP VIEW not yet supported"); }
| select { $$ = $1; }
| with DELETE FROM xfullname indexed_opt where_opt orderby_opt limit_opt {
    $$ = parse_node_alloc("delete");
    parse_node_add_child($$, "from", $4);
    if ($6) {
      parse_node_add_child($$, "where", $6);
    }
  }
| with UPDATE orconf xfullname indexed_opt SET setlist where_opt orderby_opt limit_opt {
    $$ = parse_node_alloc("update");
    parse_node_add_child($$, "relation", $4);
    parse_node_add_child_list($$, "assignments", $7);
    if ($8) {
      parse_node_add_child($$, "where", $8);
    }
  }
| with insert_cmd INTO xfullname idlist_opt select upsert {
    $$ = parse_node_alloc("insert");
    parse_node_add_child($$, "into", $4);
    if ($5) {
      parse_node_add_child_list($$, "attributes", $5);
    }
    parse_node_add_child($$, "source", $6);
  }
| with insert_cmd INTO xfullname idlist_opt DEFAULT VALUES { yyerror(NULL, scanner, "INSERT DEFAULT VALUES not yet supported"); }
| createkw uniqueflag INDEX ifnotexists nm dbnm ON nm LP sortlist RP where_opt { yyerror(NULL, scanner, "CREATE INDEX not yet supported"); }
| DROP INDEX ifexists fullname { yyerror(NULL, scanner, "DROP INDEX not yet supported"); }
| VACUUM { yyerror(NULL, scanner, "VACUUM not yet supported"); }
| VACUUM nm { yyerror(NULL, scanner, "VACUUM not yet supported"); }
| PRAGMA nm dbnm { yyerror(NULL, scanner, "PRAGMA not yet supported"); }
| PRAGMA nm dbnm EQ nmnum { yyerror(NULL, scanner, "PRAGMA not yet supported"); }
| PRAGMA nm dbnm LP nmnum RP { yyerror(NULL, scanner, "PRAGMA not yet supported"); }
| PRAGMA nm dbnm EQ minus_num { yyerror(NULL, scanner, "PRAGMA not yet supported"); }
| PRAGMA nm dbnm LP minus_num RP { yyerror(NULL, scanner, "PRAGMA not yet supported"); }
| createkw trigger_decl BEGIN trigger_cmd_list END { yyerror(NULL, scanner, "CREATE BEGIN END not yet supported"); }
| DROP TRIGGER ifexists fullname { yyerror(NULL, scanner, "DROP TRIGGER not yet supported"); }
| ATTACH database_kw_opt expr AS expr key_opt { yyerror(NULL, scanner, "ATTACH not yet supported"); }
| DETACH database_kw_opt expr { yyerror(NULL, scanner, "DETACH not yet supported"); }
| REINDEX { yyerror(NULL, scanner, "REINDEX not yet supported"); }
| REINDEX nm dbnm { yyerror(NULL, scanner, "REINDEX not yet supported"); }
| ANALYZE { yyerror(NULL, scanner, "ANALYZE not yet supported"); }
| ANALYZE nm dbnm { yyerror(NULL, scanner, "ANALYZE not yet supported"); }
| ALTER TABLE fullname RENAME TO nm { yyerror(NULL, scanner, "ALTER TABLE not yet supported"); }
| ALTER TABLE add_column_fullname ADD kwcolumn_opt columnname carglist { yyerror(NULL, scanner, "ALTER TABLE not yet supported"); }
| ALTER TABLE fullname RENAME kwcolumn_opt nm TO nm { yyerror(NULL, scanner, "ALTER TABLE not yet supported"); }
| create_vtab { yyerror(NULL, scanner, "VIRTUAL TABLE not yet supported"); }
| create_vtab LP vtabarglist RP { yyerror(NULL, scanner, "VIRTUAL TABLE not yet supported"); }
;

trans_opt:
  /* empty */
| TRANSACTION
| TRANSACTION nm
;

transtype:
  /* empty */
| DEFERRED
| IMMEDIATE
| EXCLUSIVE
;

savepoint_opt:
  SAVEPOINT
| /* empty */
;

createkw:
  CREATE
;

ifnotexists:
  /* empty */
| IF NOT EXISTS { yyerror(NULL, scanner, "IF NOT EXISTS not yet supported"); }
;

temp:
  TEMP { yyerror(NULL, scanner, "TEMP not yet supported"); }
| /* empty */
;

create_table_args:
  LP columnlist conslist_opt RP table_options {
    $$ = $2;
  }
| AS select { yyerror(NULL, scanner, "CREATE TABLE AS not yet supported"); }
;

table_options:
  /* empty */
| WITHOUT nm { yyerror(NULL, scanner, "WITHOUT not yet supported"); }
;

columnlist:
  columnlist COMMA columnname carglist {
    $$ = $1;
    dynamic_array_push_back($$, $3);
  }
| columnname carglist {
    $$ = dynamic_array_alloc();
    dynamic_array_push_back($$, $1);
  }
;

columnname:
  nm typetoken {
    $$ = parse_node_alloc("attribute");
    parse_node_add_value($$, "name", $1);
    parse_node_add_child($$, "attribute_type", $2);
  }
;

nm:
  id { $$ = $1; }
| STRING { $$ = $1; }
| JOIN_KW { $$ = $1; }
;

typetoken:
  /* empty */ {
    $$ = 0;
  }
| typename {
    $$ = parse_node_alloc("type");
    parse_node_add_value($$, "name", $1);
  }
| typename LP signed RP {
    $$ = parse_node_alloc("type");
    parse_node_add_value($$, "name", $1);
    parse_node_add_value($$, "argument", $3);
  }
| typename LP signed COMMA signed RP { yyerror(NULL, scanner, "complex type definitions not yet supported"); }
;

typename:
  ids {
    $$ = $1;
  }
| typename ids { yyerror(NULL, scanner, "multiple type names not yet supported"); }
;

signed:
  plus_num { $$ = $1; }
| minus_num { $$ = $1; }
;

scanpt:
  /* empty */
;

carglist:
  carglist ccons
| /* empty */
;

ccons:
  CONSTRAINT nm
| DEFAULT scanpt term scanpt
| DEFAULT LP expr RP
| DEFAULT PLUS term scanpt
| DEFAULT MINUS term scanpt
| DEFAULT scanpt id
| NULL onconf
| NOT NULL onconf
| PRIMARY KEY sortorder onconf autoinc
| UNIQUE onconf
| CHECK LP expr RP
| REFERENCES nm eidlist_opt refargs
| defer_subclause
| COLLATE ids
;

autoinc:
  /* empty */
| AUTOINCR
;

refargs:
  /* empty */
| refargs refarg
;

refarg:
  MATCH nm
| ON INSERT refact
| ON DELETE refact
| ON UPDATE refact
;

refact:
  SET NULL
| SET DEFAULT
| CASCADE
| RESTRICT
| NO ACTION
;

defer_subclause:
  NOT DEFERRABLE init_deferred_pred_opt
| DEFERRABLE init_deferred_pred_opt
;

init_deferred_pred_opt:
  /* empty */
| INITIALLY DEFERRED
| INITIALLY IMMEDIATE
;

conslist_opt:
  /* empty */
| COMMA conslist
;

conslist:
  conslist tconscomma tcons
| tcons
;

tconscomma:
  COMMA
| /* empty */
;

tcons:
  CONSTRAINT nm
| PRIMARY KEY LP sortlist autoinc RP onconf
| UNIQUE LP sortlist RP onconf
| CHECK LP expr RP onconf
| FOREIGN KEY LP eidlist RP REFERENCES nm eidlist_opt refargs defer_subclause_opt
;

defer_subclause_opt:
  /* empty */
| defer_subclause
;

onconf:
  /* empty */
| ON CONFLICT resolvetype
;

orconf:
  /* empty */
| OR resolvetype { yyerror(NULL, scanner, "OR not yet supported"); }
;

resolvetype:
  raisetype
| IGNORE
| REPLACE
;

ifexists:
  IF EXISTS { yyerror(NULL, scanner, "IF EXISTS not yet supported"); }
| /* empty */
;

select:
  WITH wqlist selectnowith { yyerror(NULL, scanner, "WITH SELECT not yet supported"); }
| WITH RECURSIVE wqlist selectnowith  { yyerror(NULL, scanner, "WITH RECURSIVE SELECT not yet supported"); }
| selectnowith { $$ = $1; }
;

selectnowith:
  oneselect { $$ = $1; }
| selectnowith multiselect_op oneselect { yyerror(NULL, scanner, "multiselect not yet supported"); }
;

multiselect_op:
  UNION
| UNION ALL
| EXCEPT
| INTERSECT
;

oneselect:
  SELECT distinct selcollist from where_opt groupby_opt having_opt orderby_opt limit_opt {
    $$ = parse_node_alloc("select");

    parse_node_add_child_list($$, "select", $3);
    parse_node_add_child($$, "from", $4);

    if ($5) {
      parse_node_add_child($$, "where", $5);
    }

    if ($6) {
      parse_node_add_child_list($$, "group_by", $6);
    }
  }
| SELECT distinct selcollist from where_opt groupby_opt having_opt window_clause orderby_opt limit_opt { yyerror(NULL, scanner, "window queries not yet supported"); }
| values { $$ = $1; }
;

values:
  VALUES LP nexprlist RP {
    $$ = parse_node_alloc("values");
    parse_node_add_child_list($$, "values", $3);
  }
| values COMMA LP nexprlist RP { yyerror(NULL, scanner, "multiple VALUES lists not yet supported"); }
;

distinct:
  DISTINCT
| ALL
| /* empty */
;

sclp:
  selcollist COMMA {
    $$ = $1;
  }
| /* empty */ {
    $$ = dynamic_array_alloc();
  }
;

selcollist:
  sclp scanpt expr scanpt as {
    $$ = $1;
    dynamic_array_push_back($$, $3);
  }
| sclp scanpt STAR { yyerror(NULL, scanner, "SELECT *  not yet supported"); }
| sclp scanpt nm DOT STAR { yyerror(NULL, scanner, "SELECT .* not yet supported"); }
;

as:
  AS nm
| ids
| /* empty */
;

from:
  /* empty */ { $$ = 0; }
| FROM seltablist {
    $$ = $2;
  }
;

stl_prefix:
  seltablist joinop {
    $$ = $1;
  }
| /* empty */ {
    $$ = 0;
  }
;

seltablist:
  stl_prefix nm dbnm as indexed_opt on_opt using_opt {
    parse_node *reference_node = parse_node_alloc("reference");
    parse_node_add_value(reference_node, "relation", $2);

    if ($1) {
      $$ = parse_node_alloc("join");
      parse_node_add_child($$, "left", $1);
      parse_node_add_child($$, "right", reference_node);
      if ($6) {
        parse_node_add_child($$, "predicate", $6);
      }
    } else {
      $$ = reference_node;
    }
  }
| stl_prefix nm dbnm LP exprlist RP as on_opt using_opt { yyerror(NULL, scanner, "parentheses in FROM clause not yet supported"); }
| stl_prefix LP select RP as on_opt using_opt { yyerror(NULL, scanner, "nested select not yet supported"); }
| stl_prefix LP seltablist RP as on_opt using_opt { yyerror(NULL, scanner, "parentheses in FROM clause not yet supported"); }
;

dbnm:
  /* empty */
| DOT nm
;

fullname:
  nm {
    $$ = parse_node_alloc("reference");
    parse_node_add_value($$, "relation", $1);
  }
| nm DOT nm {
    $$ = parse_node_alloc("reference");
    parse_node_add_value($$, "database", $1);
    parse_node_add_value($$, "relation", $3);
  }
;

xfullname:
  nm {
    $$ = parse_node_alloc("reference");
    parse_node_add_value($$, "relation", $1);
  }
| nm DOT nm {
    $$ = parse_node_alloc("reference");
    parse_node_add_value($$, "database", $1);
    parse_node_add_value($$, "relation", $3);
  }
| nm DOT nm AS nm {
    $$ = parse_node_alloc("reference");
    parse_node_add_value($$, "database", $1);
    parse_node_add_value($$, "relation", $3);
    parse_node_add_value($$, "alias", $5);
  }
| nm AS nm {
    $$ = parse_node_alloc("reference");
    parse_node_add_value($$, "relation", $1);
    parse_node_add_value($$, "alias", $3);
  }
;

joinop:
  COMMA
| JOIN
| JOIN_KW JOIN
| JOIN_KW nm JOIN
| JOIN_KW nm nm JOIN
;

on_opt:
  ON expr {
    $$ = $2;
  }
| /* empty */ %prec OR {
    $$ = 0;
  }
;

indexed_opt:
  /* empty */
| INDEXED BY nm { yyerror(NULL, scanner, "INDEXED BY not yet supported"); }
| NOT INDEXED { yyerror(NULL, scanner, "NOT INDEXED not yet supported"); }
;

using_opt:
  USING LP idlist RP
| /* empty */
;

orderby_opt:
  /* empty */
| ORDER BY sortlist { yyerror(NULL, scanner, "ORDER BY not yet supported"); }
;

sortlist:
  sortlist COMMA expr sortorder
| expr sortorder
;

sortorder:
  ASC
| DESC
| /* empty */
;

groupby_opt:
  /* empty */ { $$ = 0; }
| GROUP BY nexprlist {
    $$ = $3;
  }
;

having_opt:
  /* empty */
| HAVING expr
;

limit_opt:
  /* empty */
| LIMIT expr { yyerror(NULL, scanner, "LIMIT not yet supported"); }
| LIMIT expr OFFSET expr { yyerror(NULL, scanner, "LIMIT not yet supported"); }
| LIMIT expr COMMA expr { yyerror(NULL, scanner, "LIMIT not yet supported"); }
;

where_opt:
  /* empty */ { $$ = 0; }
| WHERE expr { $$ = $2; }
;

setlist:
  setlist COMMA nm EQ expr {
    $$ = $1;
    parse_node *set_node = parse_node_alloc("assignment");
    parse_node_add_value(set_node, "attribute", $3);
    parse_node_add_child(set_node, "value", $5);
    dynamic_array_push_back($$, set_node);
  }
| setlist COMMA LP idlist RP EQ expr { yyerror(NULL, scanner, "multiple SET not yet supported"); }
| nm EQ expr {
    $$ = dynamic_array_alloc();
    parse_node *set_node = parse_node_alloc("assignment");
    parse_node_add_value(set_node, "attribute", $1);
    parse_node_add_child(set_node, "value", $3);
    dynamic_array_push_back($$, set_node);
  }
| LP idlist RP EQ expr { yyerror(NULL, scanner, "multiple SET not yet supported"); }
;

upsert:
  /* empty */
| ON CONFLICT LP sortlist RP where_opt DO UPDATE SET setlist where_opt { yyerror(NULL, scanner, "ON CONFLICT not yet supported"); }
| ON CONFLICT LP sortlist RP where_opt DO NOTHING { yyerror(NULL, scanner, "ON CONFLICT not yet supported"); }
| ON CONFLICT DO NOTHING { yyerror(NULL, scanner, "ON CONFLICT not yet supported"); }
;

insert_cmd:
  INSERT orconf
| REPLACE { yyerror(NULL, scanner, "REPLACE not yet supported"); }
;

idlist_opt:
  /* empty */ { $$ = 0; }
| LP idlist RP { $$ = $2; }
;

idlist:
  idlist COMMA nm {
    $$ = $1;
    parse_node *reference_node = parse_node_alloc("reference");
    parse_node_add_value(reference_node, "attribute", $3);
    dynamic_array_push_back($$, reference_node);
  }
| nm {
    $$ = dynamic_array_alloc();
    parse_node *reference_node = parse_node_alloc("reference");
    parse_node_add_value(reference_node, "attribute", $1);
    dynamic_array_push_back($$, reference_node);
  }
;

expr:
  term { $$ = $1; }
| LP expr RP { yyerror(NULL, scanner, "parentheses in expression not yet supported"); }
| id {
    $$ = parse_node_alloc("reference");
    parse_node_add_value($$, "attribute", $1);
  }
| JOIN_KW { yyerror(NULL, scanner, "join keyword in expression not yet supported"); }
| nm DOT nm {
    $$ = parse_node_alloc("reference");
    parse_node_add_value($$, "relation", $1);
    parse_node_add_value($$, "attribute", $3);
  }
| nm DOT nm DOT nm { yyerror(NULL, scanner, "nm.nm.nm in expression not yet supported"); }
| VARIABLE { yyerror(NULL, scanner, "VARIABLE not yet supported"); }
| expr COLLATE ids { yyerror(NULL, scanner, "COLLATE not yet supported"); }
| CAST LP expr AS typetoken RP { yyerror(NULL, scanner, "CAST not yet supported"); }
| id LP distinct exprlist RP {
    $$ = parse_node_alloc("function");
    parse_node_add_value($$, "name", $1);
    parse_node_add_child_list($$, "arguments", $4);
  }
| id LP STAR RP { yyerror(NULL, scanner, "(*) in expression not yet supported"); }
| id LP distinct exprlist RP over_clause { yyerror(NULL, scanner, "OVER not yet supported"); }
| id LP STAR RP over_clause { yyerror(NULL, scanner, "OVER not yet supported"); }
| LP nexprlist COMMA expr RP { yyerror(NULL, scanner, "expression lists not yet supported"); }
| expr AND expr {
    $$ = parse_node_alloc("binary_operation");
    parse_node_add_value($$, "operator", "and");
    parse_node_add_child($$, "left", $1);
    parse_node_add_child($$, "right", $3);
  }
| expr OR expr { yyerror(NULL, scanner, "OR in expression not yet supported"); }
| expr LT expr {
    $$ = parse_node_alloc("binary_operation");
    parse_node_add_value($$, "operator", "lt");
    parse_node_add_child($$, "left", $1);
    parse_node_add_child($$, "right", $3);
  }
| expr GT expr {
    $$ = parse_node_alloc("binary_operation");
    parse_node_add_value($$, "operator", "gt");
    parse_node_add_child($$, "left", $1);
    parse_node_add_child($$, "right", $3);
}
| expr GE expr {
    $$ = parse_node_alloc("binary_operation");
    parse_node_add_value($$, "operator", "ge");
    parse_node_add_child($$, "left", $1);
    parse_node_add_child($$, "right", $3);
}
| expr LE expr {
    $$ = parse_node_alloc("binary_operation");
    parse_node_add_value($$, "operator", "le");
    parse_node_add_child($$, "left", $1);
    parse_node_add_child($$, "right", $3);
}
| expr EQ expr {
    $$ = parse_node_alloc("binary_operation");
    parse_node_add_value($$, "operator", "eq");
    parse_node_add_child($$, "left", $1);
    parse_node_add_child($$, "right", $3);
  }
| expr NE expr { yyerror(NULL, scanner, "<> in expression not yet supported"); }
| expr BITAND expr { yyerror(NULL, scanner, "& in expression not yet supported"); }
| expr BITOR expr { yyerror(NULL, scanner, "| in expression not yet supported"); }
| expr LSHIFT expr { yyerror(NULL, scanner, "<< in expression not yet supported"); }
| expr RSHIFT expr { yyerror(NULL, scanner, ">> in expression not yet supported"); }
| expr PLUS expr { yyerror(NULL, scanner, "+ in expression not yet supported"); }
| expr MINUS expr { yyerror(NULL, scanner, "- in expression not yet supported"); }
| expr STAR expr { yyerror(NULL, scanner, "* in expression not yet supported"); }
| expr SLASH expr { yyerror(NULL, scanner, "/ in expression not yet supported"); }
| expr REM expr { yyerror(NULL, scanner, "% in expression not yet supported"); }
| expr CONCAT expr { yyerror(NULL, scanner, "|| in expression not yet supported"); }
| expr likeop expr %prec LIKE_KW { yyerror(NULL, scanner, "LIKE in expression not yet supported"); }
| expr likeop expr ESCAPE expr %prec LIKE_KW { yyerror(NULL, scanner, "ESCAPE in expression not yet supported"); }
| expr ISNULL { yyerror(NULL, scanner, "ISNULL in expression not yet supported"); }
| expr NOTNULL { yyerror(NULL, scanner, "NOTNULL in expression not yet supported"); }
| expr NOT NULL { yyerror(NULL, scanner, "NOT NULL in expression not yet supported"); }
| expr IS expr { yyerror(NULL, scanner, "IS in expression not yet supported"); }
| NOT expr { yyerror(NULL, scanner, "NOT in expression not yet supported"); }
| BITNOT expr { yyerror(NULL, scanner, "~ in expression not yet supported"); }
| PLUS expr %prec BITNOT { yyerror(NULL, scanner, "+ in expression not yet supported"); }
| MINUS expr %prec BITNOT { yyerror(NULL, scanner, "- in expression not yet supported"); }
| expr between_op expr %prec BETWEEN { yyerror(NULL, scanner, "BETWEEN in expression not yet supported"); }
| expr in_op LP exprlist RP %prec IN { yyerror(NULL, scanner, "IN in expression not yet supported"); }
| LP select RP { yyerror(NULL, scanner, "SELECT in expression not yet supported"); }
| expr in_op LP select RP %prec IN { yyerror(NULL, scanner, "IN in expression not yet supported"); }
| expr in_op nm dbnm paren_exprlist %prec IN { yyerror(NULL, scanner, "IN in expression not yet supported"); }
| EXISTS LP select RP { yyerror(NULL, scanner, "EXISTS in expression not yet supported"); }
| CASE case_operand case_exprlist case_else END { yyerror(NULL, scanner, "CASE in expression not yet supported"); }
| RAISE LP IGNORE RP { yyerror(NULL, scanner, "RAISE in expression not yet supported"); }
| RAISE LP raisetype COMMA nm RP { yyerror(NULL, scanner, "RAISE in expression not yet supported"); }
;

term:
  NULL {
    $$ = parse_node_alloc("term");
    parse_node_add_value($$, "value", $1);
  }
| FLOAT {
    $$ = parse_node_alloc("term");
    parse_node_add_value($$, "value", $1);
  }
| BLOB {
    $$ = parse_node_alloc("term");
    parse_node_add_value($$, "value", $1);
  }
| STRING {
    $$ = parse_node_alloc("term");
    parse_node_add_value($$, "value", $1);
  }
| INTEGER {
    $$ = parse_node_alloc("term");
    parse_node_add_value($$, "value", $1);
  }
| CTIME_KW {
    $$ = parse_node_alloc("term");
    parse_node_add_value($$, "value", $1);
  }
;

likeop:
  LIKE_KW
| MATCH
| NOT LIKE_KW
| NOT MATCH
;

between_op:
  BETWEEN
| NOT BETWEEN
;

in_op:
  IN
| NOT IN
;

case_exprlist:
  case_exprlist WHEN expr THEN expr
| WHEN expr THEN expr
;

case_else:
  ELSE expr
| /* empty */
;

case_operand:
  expr
| /* empty */
;

exprlist:
  nexprlist {
    $$ = $1;
  }
| /* empty */ {
    $$ = dynamic_array_alloc();
  }
;

nexprlist:
  nexprlist COMMA expr {
    $$ = $1;
    dynamic_array_push_back($$, $3);
  }
| expr {
    $$ = dynamic_array_alloc();
    dynamic_array_push_back($$, $1);
  }
;

paren_exprlist:
  /* empty */
| LP exprlist RP
;

uniqueflag:
  UNIQUE
| /* empty */
;

eidlist_opt:
  /* empty */
| LP eidlist RP
;

eidlist:
  eidlist COMMA nm collate sortorder
| nm collate sortorder
;

collate:
  /* empty */
| COLLATE ids
;

nmnum:
  plus_num
| nm
| ON
| DELETE
| DEFAULT
;

plus_num:
  PLUS number { $$ = $2; }
| number { $$ = $1; }
;

minus_num:
  MINUS number {
    char *num = malloc(strlen("-") + strlen($2) + 1);
    strcpy(num, "-");
    strcat(num, $2);
    $$ = num;
  }
;

trigger_decl:
  temp TRIGGER ifnotexists nm dbnm trigger_time trigger_event ON fullname foreach_clause when_clause
;

trigger_time:
  BEFORE
| AFTER
| INSTEAD OF
| /* empty */
;

trigger_event:
  DELETE
| INSERT
| UPDATE
| UPDATE OF idlist
;

foreach_clause:
  /* empty */
| FOR EACH ROW
;

when_clause:
  /* empty */
| WHEN expr
;

trigger_cmd_list:
  trigger_cmd_list trigger_cmd SEMI
| trigger_cmd SEMI
;

trnm:
  nm
| nm DOT nm
;

tridxby:
  /* empty */
| INDEXED BY nm
| NOT INDEXED
;

trigger_cmd:
  UPDATE orconf trnm tridxby SET setlist where_opt scanpt
| scanpt insert_cmd INTO trnm idlist_opt select upsert scanpt
| DELETE FROM trnm tridxby where_opt scanpt
| scanpt select scanpt
;

raisetype:
  ROLLBACK
| ABORT
| FAIL
;

key_opt:
  /* empty */
| KEY expr
;

database_kw_opt:
  DATABASE
| /* empty */
;

add_column_fullname:
  fullname
;

kwcolumn_opt:
  /* empty */
| COLUMNKW
;

create_vtab:
  createkw VIRTUAL TABLE ifnotexists nm dbnm USING nm
;

vtabarglist:
  vtabarg
| vtabarglist COMMA vtabarg
;

vtabarg:
  /* empty */
| vtabarg vtabargtoken
;

vtabargtoken:
  ANY
| lp anylist RP
;

lp:
  LP
;

anylist:
  /* empty */
| anylist LP anylist RP
| anylist ANY
;

with:
  /* empty */
| WITH wqlist { yyerror(NULL, scanner, "WITH not yet supported"); }
| WITH RECURSIVE wqlist { yyerror(NULL, scanner, "WITH RECURSIVE not yet supported"); }
;

wqlist:
  nm eidlist_opt AS LP select RP
| wqlist COMMA nm eidlist_opt AS LP select RP
;

windowdefn_list:
  windowdefn
| windowdefn_list COMMA windowdefn
;

windowdefn:
  nm AS window
;

window:
  LP part_opt orderby_opt frame_opt RP
;

part_opt:
  PARTITION BY nexprlist
| /* empty */
;

frame_opt:
  /* empty */
| range_or_rows frame_bound_s
| range_or_rows BETWEEN frame_bound_s AND frame_bound_e
;

range_or_rows:
  RANGE
| ROWS
;

frame_bound_s:
  frame_bound
| UNBOUNDED PRECEDING
;

frame_bound_e:
  frame_bound
| UNBOUNDED FOLLOWING
;

frame_bound:
  expr PRECEDING
| CURRENT ROW
| expr FOLLOWING
;

window_clause:
  WINDOW windowdefn_list
;

over_clause:
  filter_opt OVER window
| filter_opt OVER nm
;

filter_opt:
  /* empty */
| FILTER LP WHERE expr RP
;

id:
  ID { $$ = $1; }
| INDEXED { $$ = $1; }
;

ids:
  ID { $$ = $1; }
| STRING { $$ = $1; }
;

number:
  INTEGER { $$ = $1; }
| FLOAT { $$ = $1; }
;
%%
