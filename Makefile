
all:
	+$(MAKE) -C execution
	+$(MAKE) -C optimizer
	+$(MAKE) -C cli

clean:
	$(MAKE) -C ./cli clean
	$(MAKE) -C ./optimizer clean
	$(MAKE) -C ./execution clean


run:
	RUST_BACKTRACE=1 ./optimizer/example