#!/bin/sh
#
# Basic data shuffling test for shuffle library.
# Arguments are optional. If not provided, we run in cwd with 1 MPI process.
#
# Argument 1: directory containing library and test executable
# Argument 2: number of MPI processes to spawn

BUILD_PREFIX="."
if [ ! -z "$1" ]; then
    BUILD_PREFIX="$1"
fi

MPI_PROCS=1
if [ ! -z "$2" ]; then
    MPI_PROCS=$2
fi

#
# XXX: this assumes a SunOS/linux-style ld.so (won't work on macosx)
#
mpirun -np $MPI_PROCS -mca btl ^openib $BUILD_PREFIX/tests/nexus-test

if [ $? != 0 ]; then
    echo "Nexus test failed ($?)"
    exit 1
fi

echo "Nexus test successful"
exit 0
