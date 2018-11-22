
all:
cd optimizer/quickstep/third_party; ./download_and_patch_prerequisites.sh
cd optimizer/quickstep;git submodule init;git submodule update
mkdir build
cd build ;cmake ..;make -j20
