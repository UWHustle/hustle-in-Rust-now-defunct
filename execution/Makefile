
all: libexecution.a

libexecution.a libexecution.a.out : ./src/lib.rs Cargo.toml
	cargo test 2> $(<:lib.rs=libexecution.a.out)
	cp ./target/debug/libexecution.a libexecution.a


clean:
	$(RM) libexecution.a
	$(RM) ./src/libexecution.a.out