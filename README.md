**library managing process communication information for deltafs.**

[![GitHub release](https://img.shields.io/github/release/pdlfs/deltafs-nexus.svg)](https://github.com/pdlfs/deltafs-nexus/releases)
[![License](https://img.shields.io/badge/license-New%20BSD-blue.svg)](LICENSE.txt)

deltafs-nexus
=============

```
xxxxxxxx           xxxx xxxx        xxx
xxxx xxxx          xxxx  xxxx      xxx
xxxx  xxxx         xxxx   xxxx    xxx
xxxx   xxxx        xxxx    xxxx  xxx
xxxx    xxxx       xxxx     xxxxxx
xxxx     xxxx      xxxx      xxxx
xxxx      xxxx     xxxx      xxxxx
xxxx       xxxx    xxxx     xxxxxxx
xxxx        xxxx   xxxx    xxx  xxxx
xxxx         xxxx  xxxx   xxx    xxxx
xxxx          xxxx xxxx  xxx      xxxx
xxxx           xxxxxxxx xxx        xxxx
```

Please see the accompanying LICENSE file for licensing details.

# Software requirements

First, if on a Ubuntu box, do the following:

```bash
sudo apt-get install -y gcc g++ make
sudo apt-get install -y pkg-config
sudo apt-get install -y autoconf automake libtool
sudo apt-get install -y cmake
sudo apt-get install -y libmpich-dev
sudo apt-get install -y mpich
```

If on a Mac machine with `brew`, do the following:

```bash
brew install gcc
brew install pkg-config
brew install autoconf automake libtool
brew install cmake
env \
CC=/usr/local/bin/gcc-<n> \
CXX=/usr/local/bin/g++-<n> \
brew install --build-from-source \
mpich
```

Next, we need to install `mercury`. This can be done as follows:

```bash
git clone git@github.com:mercury-hpc/mercury.git
cd mercury
mkdir build
cd build
cmake \
-DBUILD_SHARED_LIBS=ON \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
-DCMAKE_INSTALL_PREFIX=</tmp/mercury-prefix> \
-DNA_USE_SM=ON \
..
```

# Building

After installing dependencies, we can build `deltafs-nexus` as follows:

```bash
git clone git@github.com:pdlfs/deltafs-nexus.git
cd deltafs-nexus
mkdir build
cd build
cmake \
-DBUILD_SHARED_LIBS=ON \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
-DCMAKE_INSTALL_PREFIX=</tmp/deltafs-nexus-prefix> \
-DCMAKE_PREFIX_PATH=</tmp/mercury-prefix> \
..
```
