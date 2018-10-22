
all: example

example: example.o libcatalog.dylib
	g++ -L. -v example.o libcatalog.dylib -o example

example.o: example.cpp
	g++ -c example.cpp

.PHONY: libcatalog.dylib

libcatalog.dylib:
	cargo test --manifest-path ../Cargo.toml
	cp ../target/debug/libcatalog.dylib ./libcatalog.dylib

clean:
	rm ../target/debug/libcatalog.dylib
	rm example.o
	rm libcatalog.dylib
	rm example
	rm *.catalog

run:
	./example
