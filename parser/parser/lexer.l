%{
#include "parse_node.h"
#include "parser.h"
#include <stdio.h>
%}

%option outfile="lexer.c" header-file="lexer.h"

%option reentrant noyywrap
%option bison-bridge

%%
ABORT           		{ return TK_ABORT; }
ACTION          		{ return TK_ACTION; }
ADD             		{ return TK_ADD; }
AFTER           		{ return TK_AFTER; }
ALL             		{ return TK_ALL; }
ALTER           		{ return TK_ALTER; }
ANALYZE         		{ return TK_ANALYZE; }
AND             		{ return TK_AND; }
ANY             		{ return TK_ANY; }
AS              		{ return TK_AS; }
ASC             		{ return TK_ASC; }
ATTACH          		{ return TK_ATTACH; }
AUTOINCREMENT   		{ return TK_AUTOINCR; }
BEFORE          		{ return TK_BEFORE; }
BEGIN           		{ return TK_BEGIN; }
BETWEEN         		{ return TK_BETWEEN; }
BY              		{ return TK_BY; }
CASCADE         		{ return TK_CASCADE; }
CASE            		{ return TK_CASE; }
CAST            		{ return TK_CAST; }
CHECK           		{ return TK_CHECK; }
COLLATE         		{ return TK_COLLATE; }
COLUMN          		{ return TK_COLUMNKW; }
COMMIT          		{ return TK_COMMIT; }
CONFLICT        		{ return TK_CONFLICT; }
CONSTRAINT      		{ return TK_CONSTRAINT; }
CREATE          		{ return TK_CREATE; }
CROSS					{ return TK_JOIN_KW; }
CURRENT         		{ return TK_CURRENT; }
CURRENT_DATE			{ return TK_CTIME_KW; }
CURRENT_TIME			{ return TK_CTIME_KW; }
CURRENT_TIMESTAMP		{ return TK_CTIME_KW; }
DATABASE        		{ return TK_DATABASE; }
DEFAULT         		{ return TK_DEFAULT; }
DEFERRABLE      		{ return TK_DEFERRABLE; }
DEFERRED        		{ return TK_DEFERRED; }
DELETE          		{ return TK_DELETE; }
DESC            		{ return TK_DESC; }
DETACH          		{ return TK_DETACH; }
DISTINCT        		{ return TK_DISTINCT; }
DO              		{ return TK_DO; }
DROP            		{ return TK_DROP; }
EACH            		{ return TK_EACH; }
ELSE            		{ return TK_ELSE; }
END             		{ return TK_END; }
EQ              		{ return TK_EQ; }
ESCAPE          		{ return TK_ESCAPE; }
EXCEPT          		{ return TK_EXCEPT; }
EXCLUSIVE       		{ return TK_EXCLUSIVE; }
EXISTS          		{ return TK_EXISTS; }
EXPLAIN         		{ return TK_EXPLAIN; }
FAIL            		{ return TK_FAIL; }
FILTER          		{ return TK_FILTER; }
FOLLOWING       		{ return TK_FOLLOWING; }
FOR             		{ return TK_FOR; }
FOREIGN         		{ return TK_FOREIGN; }
FROM            		{ return TK_FROM; }
GLOB					{ return TK_LIKE_KW; }
GROUP           		{ return TK_GROUP; }
HAVING          		{ return TK_HAVING; }
IF              		{ return TK_IF; }
IGNORE          		{ return TK_IGNORE; }
IMMEDIATE       		{ return TK_IMMEDIATE; }
IN              		{ return TK_IN; }
INDEX           		{ return TK_INDEX; }
INDEXED         		{ return TK_INDEXED; }
INITIALLY       		{ return TK_INITIALLY; }
INNER					{ return TK_JOIN_KW; }
INSERT          		{ return TK_INSERT; }
INSTEAD         		{ return TK_INSTEAD; }
INTERSECT       		{ return TK_INTERSECT; }
INTO            		{ return TK_INTO; }
IS              		{ return TK_IS; }
ISNULL          		{ return TK_ISNULL; }
JOIN            		{ return TK_JOIN; }
KEY             		{ return TK_KEY; }
LEFT					{ return TK_JOIN_KW; }
LIKE            		{ return TK_LIKE_KW; }
LIMIT           		{ return TK_LIMIT; }
MATCH           		{ return TK_MATCH; }
NO              		{ return TK_NO; }
NOT             		{ return TK_NOT; }
NOTHING         		{ return TK_NOTHING; }
NOTNULL         		{ return TK_NOTNULL; }
NULL            		{ return TK_NULL; }
OF              		{ return TK_OF; }
OFFSET          		{ return TK_OFFSET; }
ON              		{ return TK_ON; }
OR              		{ return TK_OR; }
ORDER           		{ return TK_ORDER; }
OUTER					{ return TK_JOIN_KW; }
OVER            		{ return TK_OVER; }
PARTITION       		{ return TK_PARTITION; }
PLAN            		{ return TK_PLAN; }
PRAGMA          		{ return TK_PRAGMA; }
PRECEDING       		{ return TK_PRECEDING; }
PRIMARY         		{ return TK_PRIMARY; }
QUERY           		{ return TK_QUERY; }
RAISE           		{ return TK_RAISE; }
RANGE           		{ return TK_RANGE; }
RECURSIVE       		{ return TK_RECURSIVE; }
REFERENCES      		{ return TK_REFERENCES; }
REGEXP					{ return TK_LIKE_KW; }
REINDEX         		{ return TK_REINDEX; }
RELEASE         		{ return TK_RELEASE; }
RENAME          		{ return TK_RENAME; }
REPLACE         		{ return TK_REPLACE; }
RESTRICT        		{ return TK_RESTRICT; }
RIGHT					{ return TK_JOIN_KW; }
ROLLBACK        		{ return TK_ROLLBACK; }
ROW             		{ return TK_ROW; }
ROWS            		{ return TK_ROWS; }
SAVEPOINT       		{ return TK_SAVEPOINT; }
SELECT          		{ return TK_SELECT; }
SET             		{ return TK_SET; }
TABLE           		{ return TK_TABLE; }
TEMP            		{ return TK_TEMP; }
TEMPORARY				{ return TK_TEMP; }
THEN            		{ return TK_THEN; }
TO              		{ return TK_TO; }
TRANSACTION     		{ return TK_TRANSACTION; }
TRIGGER         		{ return TK_TRIGGER; }
UNBOUNDED       		{ return TK_UNBOUNDED; }
UNION           		{ return TK_UNION; }
UNIQUE          		{ return TK_UNIQUE; }
UPDATE          		{ return TK_UPDATE; }
USING           		{ return TK_USING; }
VACUUM          		{ return TK_VACUUM; }
VALUES          		{ return TK_VALUES; }
VIEW            		{ return TK_VIEW; }
VIRTUAL         		{ return TK_VIRTUAL; }
WHEN            		{ return TK_WHEN; }
WHERE           		{ return TK_WHERE; }
WINDOW          		{ return TK_WINDOW; }
WITH            		{ return TK_WITH; }
WITHOUT         		{ return TK_WITHOUT; }

"&"                   	{ return TK_BITAND; }
"|"                   	{ return TK_BITOR; }
"~"                   	{ return TK_BITNOT; }
[xX]'[a-zA-Z0-9]'   	{ yylval->strval = strdup(yytext); return TK_BLOB; }
","                   	{ return TK_COMMA; }
"||"                  	{ return TK_CONCAT; }
"."						{ return TK_DOT; }
"="                     { return TK_EQ; }
[0-9]*\.[0-9]+			{ yylval->strval = strdup(yytext); return TK_FLOAT; }
">="					{ return TK_GE; }
">"						{ return TK_GT; }
[0-9]+					{ yylval->strval = strdup(yytext); return TK_INTEGER; }
"<="					{ return TK_LE; }
"("  					{ return TK_LP; }
"<"						{ return TK_LT; }
"-"						{ return TK_MINUS; }
"<>"					{ return TK_NE; }
"+"						{ return TK_PLUS; }
"%"						{ return TK_REM; }
")"						{ return TK_RP; }
">>"					{ return TK_RSHIFT; }
"/"						{ return TK_SLASH; }
"*"						{ return TK_STAR; }
\'.*\'					{ yylval->strval = strdup(yytext); return TK_STRING; }
";"						{ return TK_SEMI; }
[a-zA-Z][a-zA-Z0-9_]*	{ yylval->strval = strdup(yytext); return TK_ID; }
[ \t]
%%

void yyerror(parse_node **s, yyscan_t scanner, char const *msg) {
  fprintf(stderr, "Error: %s\n", msg);
  exit(1);
}
