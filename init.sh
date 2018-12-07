if [[   -z  `which cargo` ]]; then
sudo apt-get install cargo
fi
if [[   -z  `which cmake` ]]; then
  sudo apt-get install cmake
fi

git submodule init
git submodule update

cd optimizer/quickstep/third_party
./download_and_patch_prerequisites.sh
cd ../../..

mkdir -p build
cd build
cmake ..

