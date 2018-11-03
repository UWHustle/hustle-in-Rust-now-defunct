FILES = lexer.c parser.c statement.c main.c

all: $(FILES)
	gcc $(FILES) -o parser

lexer.c: lexer.l
	flex lexer.l

parser.c: parser.y lexer.c
	bison parser.y

clean:
	rm -f lexer.c lexer.h parser.c parser.h parser