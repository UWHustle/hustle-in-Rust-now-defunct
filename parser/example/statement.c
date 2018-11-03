#include "statement.h"
#include <stdlib.h>
#include <stdio.h>

static Statement *allocate_statement() {
	Statement *s = (Statement *) malloc(sizeof(Statement));
	return s;
}

Statement *create_statement(StatementType type) {
	Statement *s = allocate_statement();
	s->type = type;
	return s;
}

void delete_statement(Statement *s) {
	free(s);
}