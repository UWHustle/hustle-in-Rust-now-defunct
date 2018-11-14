
all:
	+$(MAKE) -C execution
	+$(MAKE) -C optimizer
	cd ./parser; cmake .;
	+$(MAKE) -C parser 
	+$(MAKE) -C cli

clean:
	$(MAKE) -C ./cli clean
	$(MAKE) -C ./optimizer clean
	$(MAKE) -C ./execution clean
	rm -rf ./parser/CMakeFiles
	rm -f ./parser/CMakeCache.txt
	rm -f ./parser/cmake_install.cmake
	rm -f ./parser/Makefile
	rm -f ./parser/libparser.a
 
run:
	RUST_BACKTRACE=1 ./cli/cli