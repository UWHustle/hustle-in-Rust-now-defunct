PARSER_PATH = ../parser

all: cli

cli: cli.c $(PARSER_PATH)/libparser.a
	gcc -c cli.c
	gcc -o $@ cli.o $(PARSER_PATH)/libparser.a

cli.o: cli.c
	gcc -c $< -o $@


clean:
	$(RM) cli
	$(RM) *.o