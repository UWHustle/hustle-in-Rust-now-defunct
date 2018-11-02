/* 
This is a minimal example of a parser that accepts a SQL string and outputs 
the type of statement using ONLY SQLite grammar rules. Currently the grammar
only accepts statements of the form "SELECT * FROM <table>", hence the 
numerous empty rules.
*/

#include "statement.h"
#include "parser.h"
#include "lexer.h"
#include <stdio.h>

Statement *get_ast(char *command) {
	Statement *statement;
	yyscan_t scanner;
	YY_BUFFER_STATE state;
	int parse_status;
	int lex_status;
	int result;

	lex_status = yylex_init(&scanner);
	if (lex_status) return NULL;

	state = yy_scan_string(command, scanner);

	parse_status = yyparse(&statement, scanner);
	if (parse_status) return NULL;

	yy_delete_buffer(state, scanner);
	yylex_destroy(scanner);

	return statement;
}

int main(void) {
	char test[] = "SELECT * FROM test;";
	Statement *statement = get_ast(test);
	printf("result: %d\n", statement->type);
	delete_statement(statement);
	return 0;
}