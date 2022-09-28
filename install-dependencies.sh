#!/bin/bash

echo "installing $1"
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
# avoid user input prompt
sed -i 's/add-apt-repository "${REPO_NAME}"/add-apt-repository -y "${REPO_NAME}"/g' llvm.sh
sudo ./llvm.sh "${1:0-2}"

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
popd || return 255
