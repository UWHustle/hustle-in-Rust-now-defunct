
RUST_LIBS =
RUST_PATH = ../execution

all: example.cpp $(RUST_PATH)/libexecution.a
	g++ -std=c++1y -c example.cpp
	ar rvs example.a example.o
	libtool -static -o optimizer.a example.a $(RUST_PATH)/libexecution.a

#example.o: example.cpp
#	g++ -std=c++1y -c $< -o $@

clean:
	$(RM) example
	$(RM) *.o *.a
