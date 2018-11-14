PARSER_PATH = ../parser
OPTIMIZER_PATH = ../optimizer
all: cli

cli: cli.c $(PARSER_PATH)/libparser.a $(OPTIMIZER_PATH)/optimizer.a
	g++ -c cli.c
	g++ -o $@ cli.o $(PARSER_PATH)/libparser.a $(OPTIMIZER_PATH)/optimizer.a

cli.o: cli.c
	gcc -c $< -o $@


clean:
	$(RM) cli
	$(RM) *.o