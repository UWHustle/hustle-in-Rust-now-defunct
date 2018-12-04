sudo apt-get install cargo
sudo apt-get install cmake

git submodule init
git submodule update

cd optimizer/quickstep/third_party
./download_and_patch_prerequisites.sh
cd ../../..

mkdir -p build
cd build
cmake ..

