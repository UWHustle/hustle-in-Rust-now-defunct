all:
	git submodule init && git submodule update
	cd optimizer/quickstep/third_party && ./download_and_patch_prerequisites.sh
	mkdir -p build
	cd build ; cmake .. ;
