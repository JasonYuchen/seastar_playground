#!/bin/bash

echo "installing $1"
sudo apt-get install -y $1

echo "installing mold"
git clone https://github.com/rui314/mold.git
pushd mold || return 255
git checkout v1.4.0
make -j$(nproc) CXX=$1
sudo make install
popd || return 255

echo "installing seastar dependencies..."
pushd seastar || return 255
sudo ./install-dependencies.sh
#echo "configuring and compiling seastar..."
#./configure.py --compiler $1 --c++-standard $2 --mode $3
#sudo ninja -C build/$3 install
#echo "testing seastar unit tests..."
#./test.py --mode=$3
popd || return 255
