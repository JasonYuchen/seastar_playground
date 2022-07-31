#!/bin/bash

pushd seastar
sudo apt-get install -y $1
echo "installing seastar dependencies..."
sudo ./install-dependencies.sh
echo "configuring and compiling seastar..."
./configure.py --compiler $1 --c++-standard $2 --mode $3
sudo ninja -C build/$3 install
echo "testing seastar unit tests..."
./test.py --mode=$3
popd
