%require "3.0"
%language "c++"
%defines "parser.h"

%define api.token.constructor
%define api.value.type variant
%define parse.assert
%define parse.trace
%define parse.error verbose
%define api.token.prefix {TK_}

%locations

%param { ParserDriver& drv }

%code requires {
class ParserDriver;
#include <memory>
#include "ParseNode.h"
#include "SelectNode.h"
#include "FunctionNode.h"
#include "ReferenceNode.h"
#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T typedef void* yyscan_t;
#endif
}

%code {
#include "ParserDriver.h"
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
%token BLOB
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
%token CTIME_KW
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
%token FLOAT
%token FOLLOWING
%token FOR
%token FOREIGN
%token FROM
%token GE
%token GROUP
%token GT
%token HAVING
%token <std::string> ID
%token IF
%token IGNORE
%token IMMEDIATE
%token IN
%token INDEX
%token <std::string> INDEXED
%token INITIALLY
%token INSERT
%token INSTEAD
%token INTEGER
%token INTERSECT
%token INTO
%token IS
%token ISNULL
%token JOIN
%token <std::string> JOIN_KW
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
%token NULL
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
%token <std::string> STRING
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
%token EOF 0

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

%type <std::string>
  id
  nm
%type <std::unique_ptr<ParseNode> >
  cmd
  expr
  select
  selectnowith
  oneselect
%type <std::vector<std::unique_ptr<ParseNode> > >
  exprlist
  from
  groupby_opt
  nexprlist
  sclp
  selcollist
  seltablist

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
  cmd { drv.syntax_tree = std::move($1); }
;

cmd:
  BEGIN transtype trans_opt { error(drv.location, "BEGIN not yet supported"); }
| COMMIT trans_opt { error(drv.location, "COMMIT not yet supported"); }
| END trans_opt { error(drv.location, "END not yet supported"); }
| ROLLBACK trans_opt { error(drv.location, "ROLLBACK not yet supported"); }
| SAVEPOINT nm { error(drv.location, "SAVEPOINT not yet supported"); }
| RELEASE savepoint_opt nm { error(drv.location, "RELEASE not yet supported"); }
| ROLLBACK trans_opt TO savepoint_opt nm { error(drv.location, "ROLLBACK not yet supported"); }
| createkw temp TABLE ifnotexists nm dbnm create_table_args { error(drv.location, "CREATE TABLE not yet supported"); }
| DROP TABLE ifexists fullname { error(drv.location, "DROP TABLE not yet supported"); }
| createkw temp VIEW ifnotexists nm dbnm eidlist_opt AS select { error(drv.location, "CREATE VIEW not yet supported"); }
| DROP VIEW ifexists fullname { error(drv.location, "DROP VIEW not yet supported"); }
| select { $$ = std::move($1); }
| with DELETE FROM xfullname indexed_opt where_opt orderby_opt limit_opt { error(drv.location, "DELETE not yet supported"); }
| with UPDATE orconf xfullname indexed_opt SET setlist where_opt orderby_opt limit_opt { error(drv.location, "UPDATE not yet supported"); }
| with insert_cmd INTO xfullname idlist_opt select upsert { error(drv.location, "INSERT not yet supported"); }
| with insert_cmd INTO xfullname idlist_opt DEFAULT VALUES { error(drv.location, "INSERT not yet supported"); }
| createkw uniqueflag INDEX ifnotexists nm dbnm ON nm LP sortlist RP where_opt { error(drv.location, "CREATE INDEX not yet supported"); }
| DROP INDEX ifexists fullname { error(drv.location, "DROP INDEX not yet supported"); }
| VACUUM { error(drv.location, "VACUUM not yet supported"); }
| VACUUM nm { error(drv.location, "VACUUM not yet supported"); }
| PRAGMA nm dbnm { error(drv.location, "PRAGMA not yet supported"); }
| PRAGMA nm dbnm EQ nmnum { error(drv.location, "PRAGMA not yet supported"); }
| PRAGMA nm dbnm LP nmnum RP { error(drv.location, "PRAGMA not yet supported"); }
| PRAGMA nm dbnm EQ minus_num { error(drv.location, "PRAGMA not yet supported"); }
| PRAGMA nm dbnm LP minus_num RP { error(drv.location, "PRAGMA not yet supported"); }
| createkw trigger_decl BEGIN trigger_cmd_list END { error(drv.location, "CREATE BEGIN END not yet supported"); }
| DROP TRIGGER ifexists fullname { error(drv.location, "DROP TRIGGER not yet supported"); }
| ATTACH database_kw_opt expr AS expr key_opt { error(drv.location, "ATTACH not yet supported"); }
| DETACH database_kw_opt expr { error(drv.location, "DETACH not yet supported"); }
| REINDEX { error(drv.location, "REINDEX not yet supported"); }
| REINDEX nm dbnm { error(drv.location, "REINDEX not yet supported"); }
| ANALYZE { error(drv.location, "ANALYZE not yet supported"); }
| ANALYZE nm dbnm { error(drv.location, "ANALYZE not yet supported"); }
| ALTER TABLE fullname RENAME TO nm { error(drv.location, "ALTER TABLE not yet supported"); }
| ALTER TABLE add_column_fullname ADD kwcolumn_opt columnname carglist { error(drv.location, "ALTER TABLE not yet supported"); }
| ALTER TABLE fullname RENAME kwcolumn_opt nm TO nm { error(drv.location, "ALTER TABLE not yet supported"); }
| create_vtab { error(drv.location, "VIRTUAL TABLE not yet supported"); }
| create_vtab LP vtabarglist RP { error(drv.location, "VIRTUAL TABLE not yet supported"); }
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
| IF NOT EXISTS
;

temp:
  TEMP
| /* empty */
;

create_table_args:
  LP columnlist conslist_opt RP table_options
| AS select
;

table_options:
  /* empty */
| WITHOUT nm
;

columnlist:
  columnlist COMMA columnname carglist
| columnname carglist
;

columnname:
  nm typetoken
;

nm:
  id { $$ = $1; }
| STRING { $$ = $1; }
| JOIN_KW { $$ = $1; }
;

typetoken:
  /* empty */
| typename
| typename LP signed RP
| typename LP signed COMMA signed RP
;

typename:
  ids
| typename ids
;

signed:
  plus_num
| minus_num
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
| OR resolvetype
;

resolvetype:
  raisetype
| IGNORE
| REPLACE
;

ifexists:
  IF EXISTS
| /* empty */
;

select:
  WITH wqlist selectnowith { error(drv.location, "WITH SELECT not yet supported"); }
| WITH RECURSIVE wqlist selectnowith  { error(drv.location, "WITH RECURSIVE SELECT not yet supported"); }
| selectnowith { $$ = std::move($1); }
;

selectnowith:
  oneselect { $$ = std::move($1); }
| selectnowith multiselect_op oneselect { error(drv.location, "multiselect not yet supported"); }
;

multiselect_op:
  UNION
| UNION ALL
| EXCEPT
| INTERSECT
;

oneselect:
  SELECT distinct selcollist from where_opt groupby_opt having_opt orderby_opt limit_opt {
    $$ = std::unique_ptr<SelectNode>(new SelectNode(std::move($3), std::move($4), std::move($6)));
  }
| SELECT distinct selcollist from where_opt groupby_opt having_opt window_clause orderby_opt limit_opt { error(drv.location, "window queries not yet supported"); }
| values { error(drv.location, "VALUES not yet supported"); }
;

values:
  VALUES LP nexprlist RP
| values COMMA LP nexprlist RP
;

distinct:
  DISTINCT
| ALL
| /* empty */
;

sclp:
  selcollist COMMA {
    $$ = std::move($1);
  }
| /* empty */ {}
;

selcollist:
  sclp scanpt expr scanpt as {
    $$ = std::move($1);
    $$.push_back(std::move($3));
  }
| sclp scanpt STAR { error(drv.location, "SELECT *  not yet supported"); }
| sclp scanpt nm DOT STAR { error(drv.location, "SELECT .* not yet supported"); }
;

as:
  AS nm
| ids
| /* empty */
;

from:
  /* empty */ {}
| FROM seltablist {
    $$ = std::move($2);
  }
;

stl_prefix:
  seltablist joinop
| /* empty */
;

seltablist:
  stl_prefix nm dbnm as indexed_opt on_opt using_opt {
    $$.push_back(std::unique_ptr<ReferenceNode>(new ReferenceNode($2)));
  }
| stl_prefix nm dbnm LP exprlist RP as on_opt using_opt { error(drv.location, "parentheses in FROM clause not yet supported"); }
| stl_prefix LP select RP as on_opt using_opt { error(drv.location, "nested select not yet supported"); }
| stl_prefix LP seltablist RP as on_opt using_opt { error(drv.location, "parentheses in FROM clause not yet supported"); }
;

dbnm:
  /* empty */
| DOT nm
;

fullname:
  nm
| nm DOT nm
;

xfullname:
  nm
| nm DOT nm
| nm DOT nm AS nm
| nm AS nm
;

joinop:
  COMMA
| JOIN
| JOIN_KW JOIN
| JOIN_KW nm JOIN
| JOIN_KW nm nm JOIN
;

on_opt:
  ON expr
| /* empty */ %prec OR
;

indexed_opt:
  /* empty */
| INDEXED BY nm
| NOT INDEXED
;

using_opt:
  USING LP idlist RP
| /* empty */
;

orderby_opt:
  /* empty */
| ORDER BY sortlist
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
  /* empty */ {}
| GROUP BY nexprlist {
    $$ = std::move($3);
  }
;

having_opt:
  /* empty */
| HAVING expr
;

limit_opt:
  /* empty */
| LIMIT expr
| LIMIT expr OFFSET expr
| LIMIT expr COMMA expr
;

where_opt:
  /* empty */
| WHERE expr
;

setlist:
  setlist COMMA nm EQ expr
| setlist COMMA LP idlist RP EQ expr
| nm EQ expr
| LP idlist RP EQ expr
;

upsert:
  /* empty */
| ON CONFLICT LP sortlist RP where_opt DO UPDATE SET setlist where_opt
| ON CONFLICT LP sortlist RP where_opt DO NOTHING
| ON CONFLICT DO NOTHING
;

insert_cmd:
  INSERT orconf
| REPLACE
;

idlist_opt:
  /* empty */
| LP idlist RP
;

idlist:
  idlist COMMA nm
| nm
;

expr:
  term { error(drv.location, "expression terms not yet supported"); }
| LP expr RP { error(drv.location, "parentheses in expression not yet supported"); }
| id {
    $$ = std::unique_ptr<ReferenceNode>(new ReferenceNode($1));
  }
| JOIN_KW { error(drv.location, "join keyword in expression not yet supported"); }
| nm DOT nm { error(drv.location, "nm.nm in expression not yet supported"); }
| nm DOT nm DOT nm { error(drv.location, "nm.nm.nm in expression not yet supported"); }
| VARIABLE { error(drv.location, "VARIABLE not yet supported"); }
| expr COLLATE ids { error(drv.location, "COLLATE not yet supported"); }
| CAST LP expr AS typetoken RP { error(drv.location, "CAST not yet supported"); }
| id LP distinct exprlist RP {
    $$ = std::unique_ptr<FunctionNode>(new FunctionNode($1, std::move($4)));
  }
| id LP STAR RP { error(drv.location, "(*) in expression not yet supported"); }
| id LP distinct exprlist RP over_clause { error(drv.location, "OVER not yet supported"); }
| id LP STAR RP over_clause { error(drv.location, "OVER not yet supported"); }
| LP nexprlist COMMA expr RP { error(drv.location, "expression lists not yet supported"); }
| expr AND expr { error(drv.location, "AND in expression not yet supported"); }
| expr OR expr { error(drv.location, "OR in expression not yet supported"); }
| expr LT expr { error(drv.location, "< in expression not yet supported"); }
| expr GT expr { error(drv.location, "> in expression not yet supported"); }
| expr GE expr { error(drv.location, ">= in expression not yet supported"); }
| expr LE expr { error(drv.location, "<= in expression not yet supported"); }
| expr EQ expr { error(drv.location, "= in expression not yet supported"); }
| expr NE expr { error(drv.location, "<> in expression not yet supported"); }
| expr BITAND expr { error(drv.location, "& in expression not yet supported"); }
| expr BITOR expr { error(drv.location, "| in expression not yet supported"); }
| expr LSHIFT expr { error(drv.location, "<< in expression not yet supported"); }
| expr RSHIFT expr { error(drv.location, ">> in expression not yet supported"); }
| expr PLUS expr { error(drv.location, "+ in expression not yet supported"); }
| expr MINUS expr { error(drv.location, "- in expression not yet supported"); }
| expr STAR expr { error(drv.location, "* in expression not yet supported"); }
| expr SLASH expr { error(drv.location, "/ in expression not yet supported"); }
| expr REM expr { error(drv.location, "% in expression not yet supported"); }
| expr CONCAT expr { error(drv.location, "|| in expression not yet supported"); }
| expr likeop expr %prec LIKE_KW { error(drv.location, "LIKE in expression not yet supported"); }
| expr likeop expr ESCAPE expr %prec LIKE_KW { error(drv.location, "ESCAPE in expression not yet supported"); }
| expr ISNULL { error(drv.location, "ISNULL in expression not yet supported"); }
| expr NOTNULL { error(drv.location, "NOTNULL in expression not yet supported"); }
| expr NOT NULL { error(drv.location, "NOT NULL in expression not yet supported"); }
| expr IS expr { error(drv.location, "IS in expression not yet supported"); }
| NOT expr { error(drv.location, "NOT in expression not yet supported"); }
| BITNOT expr { error(drv.location, "~ in expression not yet supported"); }
| PLUS expr %prec BITNOT { error(drv.location, "+ in expression not yet supported"); }
| MINUS expr %prec BITNOT { error(drv.location, "- in expression not yet supported"); }
| expr between_op expr %prec BETWEEN { error(drv.location, "BETWEEN in expression not yet supported"); }
| expr in_op LP exprlist RP %prec IN { error(drv.location, "IN in expression not yet supported"); }
| LP select RP { error(drv.location, "SELECT in expression not yet supported"); }
| expr in_op LP select RP %prec IN { error(drv.location, "IN in expression not yet supported"); }
| expr in_op nm dbnm paren_exprlist %prec IN { error(drv.location, "IN in expression not yet supported"); }
| EXISTS LP select RP { error(drv.location, "EXISTS in expression not yet supported"); }
| CASE case_operand case_exprlist case_else END { error(drv.location, "CASE in expression not yet supported"); }
| RAISE LP IGNORE RP { error(drv.location, "RAISE in expression not yet supported"); }
| RAISE LP raisetype COMMA nm RP { error(drv.location, "RAISE in expression not yet supported"); }
;

term:
  NULL
| FLOAT
| BLOB
| STRING
| INTEGER
| CTIME_KW
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
    $$ = std::move($1);
  }
| /* empty */ {}
;

nexprlist:
  nexprlist COMMA expr {
    $$ = std::move($1);
    $$.push_back(std::move($3));
  }
| expr {
    $$.push_back(std::move($1));
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
  PLUS number
| number
;

minus_num:
  MINUS number
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
| WITH wqlist
| WITH RECURSIVE wqlist
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
  ID
| INDEXED
;

ids:
  ID
| STRING
;

number:
  INTEGER
| FLOAT
;
%%

void yy::parser::error(const location_type& l, const std::string& m)
{
  std::cerr << l << ": " << m << '\n';
}
