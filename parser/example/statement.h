#ifndef __STATEMENT_H__
#define __STATEMENT_H__

typedef enum {
	SELECT
} StatementType;

typedef struct {
	StatementType type;
} Statement;

Statement *create_statement(StatementType type);

void delete_statement(Statement *s);

#endif // __STATEMENT_H__