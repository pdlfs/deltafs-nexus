#!/bin/bash
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

source $BUILD_PREFIX/../deltafs-nexus/tests/common.sh

#
# Remove na_sm files in case we hit the same PID
# XXX: this assumes we're the only ones using na+sm instances
#
rm -Rf /tmp/na_sm
rm -Rf /dev/shm/na_sm*
sleep 1

#
# XXX: this assumes a SunOS/linux-style ld.so (won't work on macosx)
#
do_mpirun $MPI_PROCS 4 "" "$BUILD_PREFIX/tests/nexus-test -s \"10.92\""

if [ $? != 0 ]; then
    echo "Nexus test failed ($?)"
    exit 1
fi

echo "Nexus test successful"
exit 0
