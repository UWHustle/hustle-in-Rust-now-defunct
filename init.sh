case "$OSTYPE" in
  darwin*)
    update="brew update"
    command="brew install -y" ;;
  linux*)
    update="sudo apt-get update"
    command="sudo apt-get -y install" ;;
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

mkdir -p build
cd build
cmake ..


echo -e "\n\nTo build Hustle run: cd build; make -j<number of cores of your machine>"