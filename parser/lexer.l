%{
#include "ParserDriver.h"
%}

%option header-file="lexer.h"
%option noyywrap

%%
%{
  yy::location& loc = drv.location;
  loc.step ();
%}

ABORT           		{ return yy::parser::make_ABORT(loc); }
ACTION          		{ return yy::parser::make_ACTION(loc); }
ADD             		{ return yy::parser::make_ADD(loc); }
AFTER           		{ return yy::parser::make_AFTER(loc); }
ALL             		{ return yy::parser::make_ALL(loc); }
ALTER           		{ return yy::parser::make_ALTER(loc); }
ANALYZE         		{ return yy::parser::make_ANALYZE(loc); }
AND             		{ return yy::parser::make_AND(loc); }
ANY             		{ return yy::parser::make_ANY(loc); }
AS              		{ return yy::parser::make_AS(loc); }
ASC             		{ return yy::parser::make_ASC(loc); }
ATTACH          		{ return yy::parser::make_ATTACH(loc); }
AUTOINCREMENT   		{ return yy::parser::make_AUTOINCR(loc); }
BEFORE          		{ return yy::parser::make_BEFORE(loc); }
BEGIN           		{ return yy::parser::make_BEGIN(loc); }
BETWEEN         		{ return yy::parser::make_BETWEEN(loc); }
BY              		{ return yy::parser::make_BY(loc); }
CASCADE         		{ return yy::parser::make_CASCADE(loc); }
CASE            		{ return yy::parser::make_CASE(loc); }
CAST            		{ return yy::parser::make_CAST(loc); }
CHECK           		{ return yy::parser::make_CHECK(loc); }
COLLATE         		{ return yy::parser::make_COLLATE(loc); }
COLUMN          		{ return yy::parser::make_COLUMNKW(loc); }
COMMIT          		{ return yy::parser::make_COMMIT(loc); }
CONFLICT        		{ return yy::parser::make_CONFLICT(loc); }
CONSTRAINT      		{ return yy::parser::make_CONSTRAINT(loc); }
CREATE          		{ return yy::parser::make_CREATE(loc); }
CROSS					{ return yy::parser::make_JOIN_KW(yytext, loc); }
CURRENT         		{ return yy::parser::make_CURRENT(loc); }
CURRENT_DATE			{ return yy::parser::make_CTIME_KW(loc); }
CURRENT_TIME			{ return yy::parser::make_CTIME_KW(loc); }
CURRENT_TIMESTAMP		{ return yy::parser::make_CTIME_KW(loc); }
DATABASE        		{ return yy::parser::make_DATABASE(loc); }
DEFAULT         		{ return yy::parser::make_DEFAULT(loc); }
DEFERRABLE      		{ return yy::parser::make_DEFERRABLE(loc); }
DEFERRED        		{ return yy::parser::make_DEFERRED(loc); }
DELETE          		{ return yy::parser::make_DELETE(loc); }
DESC            		{ return yy::parser::make_DESC(loc); }
DETACH          		{ return yy::parser::make_DETACH(loc); }
DISTINCT        		{ return yy::parser::make_DISTINCT(loc); }
DO              		{ return yy::parser::make_DO(loc); }
DROP            		{ return yy::parser::make_DROP(loc); }
EACH            		{ return yy::parser::make_EACH(loc); }
ELSE            		{ return yy::parser::make_ELSE(loc); }
END             		{ return yy::parser::make_END(loc); }
EQ              		{ return yy::parser::make_EQ(loc); }
ESCAPE          		{ return yy::parser::make_ESCAPE(loc); }
EXCEPT          		{ return yy::parser::make_EXCEPT(loc); }
EXCLUSIVE       		{ return yy::parser::make_EXCLUSIVE(loc); }
EXISTS          		{ return yy::parser::make_EXISTS(loc); }
EXPLAIN         		{ return yy::parser::make_EXPLAIN(loc); }
FAIL            		{ return yy::parser::make_FAIL(loc); }
FILTER          		{ return yy::parser::make_FILTER(loc); }
FOLLOWING       		{ return yy::parser::make_FOLLOWING(loc); }
FOR             		{ return yy::parser::make_FOR(loc); }
FOREIGN         		{ return yy::parser::make_FOREIGN(loc); }
FROM            		{ return yy::parser::make_FROM(loc); }
GLOB					{ return yy::parser::make_LIKE_KW(loc); }
GROUP           		{ return yy::parser::make_GROUP(loc); }
HAVING          		{ return yy::parser::make_HAVING(loc); }
IF              		{ return yy::parser::make_IF(loc); }
IGNORE          		{ return yy::parser::make_IGNORE(loc); }
IMMEDIATE       		{ return yy::parser::make_IMMEDIATE(loc); }
IN              		{ return yy::parser::make_IN(loc); }
INDEX           		{ return yy::parser::make_INDEX(loc); }
INDEXED         		{ return yy::parser::make_INDEXED(yytext, loc); }
INITIALLY       		{ return yy::parser::make_INITIALLY(loc); }
INNER					{ return yy::parser::make_JOIN_KW(yytext, loc); }
INSERT          		{ return yy::parser::make_INSERT(loc); }
INSTEAD         		{ return yy::parser::make_INSTEAD(loc); }
INTERSECT       		{ return yy::parser::make_INTERSECT(loc); }
INTO            		{ return yy::parser::make_INTO(loc); }
IS              		{ return yy::parser::make_IS(loc); }
ISNULL          		{ return yy::parser::make_ISNULL(loc); }
JOIN            		{ return yy::parser::make_JOIN(loc); }
KEY             		{ return yy::parser::make_KEY(loc); }
LEFT					{ return yy::parser::make_JOIN_KW(yytext, loc); }
LIKE            		{ return yy::parser::make_LIKE_KW(loc); }
LIMIT           		{ return yy::parser::make_LIMIT(loc); }
MATCH           		{ return yy::parser::make_MATCH(loc); }
NO              		{ return yy::parser::make_NO(loc); }
NOT             		{ return yy::parser::make_NOT(loc); }
NOTHING         		{ return yy::parser::make_NOTHING(loc); }
NOTNULL         		{ return yy::parser::make_NOTNULL(loc); }
NULL            		{ return yy::parser::make_NULL(loc); }
OF              		{ return yy::parser::make_OF(loc); }
OFFSET          		{ return yy::parser::make_OFFSET(loc); }
ON              		{ return yy::parser::make_ON(loc); }
OR              		{ return yy::parser::make_OR(loc); }
ORDER           		{ return yy::parser::make_ORDER(loc); }
OUTER					{ return yy::parser::make_JOIN_KW(yytext, loc); }
OVER            		{ return yy::parser::make_OVER(loc); }
PARTITION       		{ return yy::parser::make_PARTITION(loc); }
PLAN            		{ return yy::parser::make_PLAN(loc); }
PRAGMA          		{ return yy::parser::make_PRAGMA(loc); }
PRECEDING       		{ return yy::parser::make_PRECEDING(loc); }
PRIMARY         		{ return yy::parser::make_PRIMARY(loc); }
QUERY           		{ return yy::parser::make_QUERY(loc); }
RAISE           		{ return yy::parser::make_RAISE(loc); }
RANGE           		{ return yy::parser::make_RANGE(loc); }
RECURSIVE       		{ return yy::parser::make_RECURSIVE(loc); }
REFERENCES      		{ return yy::parser::make_REFERENCES(loc); }
REGEXP					{ return yy::parser::make_LIKE_KW(loc); }
REINDEX         		{ return yy::parser::make_REINDEX(loc); }
RELEASE         		{ return yy::parser::make_RELEASE(loc); }
RENAME          		{ return yy::parser::make_RENAME(loc); }
REPLACE         		{ return yy::parser::make_REPLACE(loc); }
RESTRICT        		{ return yy::parser::make_RESTRICT(loc); }
RIGHT					{ return yy::parser::make_JOIN_KW(yytext, loc); }
ROLLBACK        		{ return yy::parser::make_ROLLBACK(loc); }
ROW             		{ return yy::parser::make_ROW(loc); }
ROWS            		{ return yy::parser::make_ROWS(loc); }
SAVEPOINT       		{ return yy::parser::make_SAVEPOINT(loc); }
SELECT          		{ return yy::parser::make_SELECT(loc); }
SET             		{ return yy::parser::make_SET(loc); }
TABLE           		{ return yy::parser::make_TABLE(loc); }
TEMP            		{ return yy::parser::make_TEMP(loc); }
TEMPORARY				{ return yy::parser::make_TEMP(loc); }
THEN            		{ return yy::parser::make_THEN(loc); }
TO              		{ return yy::parser::make_TO(loc); }
TRANSACTION     		{ return yy::parser::make_TRANSACTION(loc); }
TRIGGER         		{ return yy::parser::make_TRIGGER(loc); }
UNBOUNDED       		{ return yy::parser::make_UNBOUNDED(loc); }
UNION           		{ return yy::parser::make_UNION(loc); }
UNIQUE          		{ return yy::parser::make_UNIQUE(loc); }
UPDATE          		{ return yy::parser::make_UPDATE(loc); }
USING           		{ return yy::parser::make_USING(loc); }
VACUUM          		{ return yy::parser::make_VACUUM(loc); }
VALUES          		{ return yy::parser::make_VALUES(loc); }
VIEW            		{ return yy::parser::make_VIEW(loc); }
VIRTUAL         		{ return yy::parser::make_VIRTUAL(loc); }
WHEN            		{ return yy::parser::make_WHEN(loc); }
WHERE           		{ return yy::parser::make_WHERE(loc); }
WINDOW          		{ return yy::parser::make_WINDOW(loc); }
WITH            		{ return yy::parser::make_WITH(loc); }
WITHOUT         		{ return yy::parser::make_WITHOUT(loc); }
"&"                   	{ return yy::parser::make_BITAND(loc); }
"|"                   	{ return yy::parser::make_BITOR(loc); }
"~"                   	{ return yy::parser::make_BITNOT(loc); }
[xX]'[a-zA-Z0-9]'   	{ return yy::parser::make_BLOB(loc); }
","                   	{ return yy::parser::make_COMMA(loc); }
"||"                  	{ return yy::parser::make_CONCAT(loc); }
"."						{ return yy::parser::make_DOT(loc); }
[0-9]*\.[0-9]+			{ return yy::parser::make_FLOAT(loc); }
">="					{ return yy::parser::make_GE(loc); }
">"						{ return yy::parser::make_GT(loc); }
[0-9]+					{ return yy::parser::make_INTEGER(loc); }
"<="					{ return yy::parser::make_LE(loc); }
"("  					{ return yy::parser::make_LP(loc); }
"<"						{ return yy::parser::make_LT(loc); }
"-"						{ return yy::parser::make_MINUS(loc); }
"<>"					{ return yy::parser::make_NE(loc); }
"+"						{ return yy::parser::make_PLUS(loc); }
"%"						{ return yy::parser::make_REM(loc); }
")"						{ return yy::parser::make_RP(loc); }
">>"					{ return yy::parser::make_RSHIFT(loc); }
"/"						{ return yy::parser::make_SLASH(loc); }
"*"						{ return yy::parser::make_STAR(loc); }
\'.*\'					{ return yy::parser::make_STRING(yytext, loc); }
";"						{ return yy::parser::make_SEMI(loc); }
[a-zA-Z][a-zA-Z0-9_]*	{ return yy::parser::make_ID(yytext, loc); }
[ \t]+                  { loc.step(); }
.                       { throw yy::parser::syntax_error(loc, "invalid character: " + std::string(yytext)); }
<<EOF>>                 { return yy::parser::make_EOF (loc); }
%%
