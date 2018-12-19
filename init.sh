case "$OSTYPE" in
  darwin*)
    update="brew update"
    command="brew install -y" ;;
  linux*)
    update="sudo apt-get update"
    command="sudo apt-get install" ;;
  *)
    echo "unknown OS: $OSTYPE" ;;
esac

if [[   -z  `which cargo` ]]; then
  $update
  $command cargo
fi
if [[   -z  `which cmake` ]]; then
  $update
  $command cmake
fi

git submodule init
git submodule update

cd optimizer/quickstep/third_party
./download_and_patch_prerequisites.sh
cd ../../..

mkdir -p build
cd build
cmake ..
