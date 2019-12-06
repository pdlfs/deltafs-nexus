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

# nexus overview

Nexus creates and provides access to a three-level mercury routing table
for a group of processes in a multiprocess job.  Our goal is to reduce
per-process memory usage by reducing the number of output queues and
connections to manage on each node (we are assuming an environment where
each node has multiple CPUs and there is an app with processes running
on each CPU).

Nexus uses MPI to determine the number of nodes and number of processes
on each node in the job.  Once the routing table has been created, the nexus
lookup API can be used to map a processes id (the MPI rank) to the next
mercury endpoint (hg_addr_t) to use to reach the target process.


Messages sent using nexus routing use the following path:
```
 SRC  ---local--->   SRCREP ---network--->   DESTREP   ---local--->  DST
           1                      2                        3

note: "REP" == representative
```

A msg from a SRC to a remote DST on node N flows like this:
1. SRC find the local proc responsible for talking to N.  this is
   the SRCREP.   it forward the msg to the SRCREP locally.
2. the SRCREP forwards all messages for node N to one process on
   node N over the network.   this is the DESTREP.
3. the DESTREP receives the message and looks for its local connection
   to the DST (which is also on node N) and sends the msg to DST.
at that point the DST will deliver the msg.   Note that it is
possible to skip hops (e.g. if SRC==SRCREP, the first hop can be
skipped).  Local communication is typically done using Mercury's
shared memory transport (na+sm).  

Note that nexus only provides the routing table for this type of
data flow.  Services that move data (e.g. deltafs-shuffle) are
built on top of nexus.

# nexus API

Nexus has a C-style API based around two opaque pointers:
```
typedef struct nexus_ctx* nexus_ctx_t;    /* main object */
typedef struct nexus_iter* nexus_iter_t;  /* iterator in nexus table */
```

To initialize nexus, first init mercury and create a mercury-progressor
handle for it.   Then nexus_bootstrap() can be used:
```
nexus_ctx_t nexus_bootstrap(progressor_handle_t *nethand,
                            progressor_handle_t *localhand);
```
The "nethand" progressor is used for communications between nodes.
The "localhand" is optional.  If it is provided, then nexus will
use it for local node communications.  On the other hand, if it
is NULL then nexus will create a new instance of Mercury's na+sm
shared memory transport for this instance of nexus.  Note that
nexus_bootstrap() uses collective MPI calls, so it must be called
collectively too.

Exiting applications can use nexus_shutdown() to dispose of a
nexus context previously allocated with nexus_bootstrap():
```
void nexus_destroy(nexus_ctx_t nctx);
```

To lookup routing information in nexus, use nexus_next_hop():
```
nexus_ret_t nexus_next_hop(nexus_ctx_t nctx, int dest, int* rank,
                           hg_addr_t* addr);
```
Where "dest" is the rank of the destination process.  The nexus_next_hop()
function returns the rank and mercury address of the next hop in "rank"
and "addr" if another hop is required to reach the destination process.
The return value of nexus_next_hop() indicates what type of hop the 
next hop is:
* NX_NOTFOUND - nexus doesn't know how to reach "dest"
* NX_DONE - the current process is "dest"
* NX_ISLOCAL - the dest is local and can be directly reached
* NX_SRCREP - the next hop is local to the SRCREP
* NX_DESTREP - the next hop is remote to the DESTREP

Nexus provides access to its underlying MPI rank and size info
with the following calls:
```
int nexus_global_rank(nexus_ctx_t nctx);   /* for all procs */
int nexus_global_size(nexus_ctx_t nctx);
int nexus_local_rank(nexus_ctx_t nctx);    /* for procs just on this node */
int nexus_local_size(nexus_ctx_t nctx);
```

Nexus provides access to collective MPI barrier calls using:
```
nexus_ret_t nexus_global_barrier(nexus_ctx_t nctx);
nexus_ret_t nexus_local_barrier(nexus_ctx_t nctx);
```

Nexus provides access to the the mercury progressors it used to
create the routing table.  At minimum, these progressor_handle_t's 
will remain valid until nexus_shutdown() is called.  Nexus users
may dup these progressor_handle_t's if they need to maintain a
reference to them independently of their reference to nexus:
```
progressor_handle_t *nexus_localprogressor(nexus_ctx_t nctx);
progressor_handle_t *nexus_remoteprogressor(nexus_ctx_t nctx);
```

Nexus provides several debugging APIs:
```
/* dump nexus tables to an outfile (if NULL, stderr) */
void nexus_dump(nexus_ctx_t nctx, char *outfile);

/* set global rank for debugging (override MPI) */
nexus_ret_t nexus_set_grank(nexus_ctx_t nctx, int rank);
```

Nexus uses a "nexus_iter_t" to allow users to iterate through the
routing tables (e.g. to setup output queues).   The nexus iter
API is as follows:
```
/* create a nexus_iter_t object that is either on the local or remote table */
nexus_iter_t nexus_iter(nexus_ctx_t nctx, int local);

hg_addr_t nexus_iter_addr(nexus_iter_t nit);  /* current hg_addr_t */
int nexus_iter_globalrank(nexus_iter_t nit);  /* current global rank */
int nexus_iter_subrank(nexus_iter_t nit);     /* current subrank */

int nexus_iter_atend(nexus_iter_t nit);    /* is nit at the end? */
void nexus_iter_advance(nexus_iter_t nit); /* advance iterator */

void nexus_iter_free(nexus_iter_t* nitp);  /* free iterator */
```

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
