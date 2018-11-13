
RUST_LIBS =
RUST_PATH = ../execution

all: example

example: example.cpp $(RUST_PATH)/libexecution.a
	g++ -std=c++1y -c example.cpp
	g++ -std=c++1y -o $@ example.o $(RUST_PATH)/libexecution.a $(RUST_LIBS)

example.o: example.cpp
	g++ -std=c++1y -c $< -o $@


clean:
	rm example
	rm *.o *.a