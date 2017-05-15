/*
 * Copyright (c) 2017 Carnegie Mellon University.
 * George Amvrosiadis <gamvrosi@cs.cmu.edu>
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <cstdio>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <ifaddrs.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <mpi.h>

#include "deltafs_nexus.h"

/* TODO: Convert to parameters */
#define TEST_MIN_PORT 50000
#define TEST_MAX_PORT 59999
#define TEST_SUBNET "127.0.0.1"
#define TEST_PROTO "bmi+tcp"

int myrank;

/*
 * msg_abort: abort with a message
 */
static inline void msg_abort(const char* msg)
{
    if (errno != 0) {
        fprintf(stderr, "Error: %s (%s)\n", msg, strerror(errno));   
    } else {
        fprintf(stderr, "Error: %s\n", msg);
    }

    abort();
}

/*
 * prepare_addr(): obtain the mercury addr to bootstrap the rpc
 *
 * Write the server uri into *buf on success.
 *
 * Abort on errors.
 */
static const char* prepare_addr(char* buf)
{
    struct ifaddrs *ifaddr, *cur;
    int family, ret, rank, size, port;
    char ip[16];
    MPI_Comm comm;

    /* Query local socket layer to get our IP addr */
    if (getifaddrs(&ifaddr) == -1)
        msg_abort("getifaddrs failed");

    if (myrank == 0)
        fprintf(stdout, "Info: Using subnet %s*", TEST_SUBNET);

    for (cur = ifaddr; cur != NULL; cur = cur->ifa_next) {
        if (cur->ifa_addr != NULL) {
            family = cur->ifa_addr->sa_family;

            if (family == AF_INET) {
                if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in), ip,
                                sizeof(ip), NULL, 0, NI_NUMERICHOST) == -1)
                    msg_abort("getnameinfo failed");

                if (strncmp(TEST_SUBNET, ip, strlen(TEST_SUBNET)) == 0)
                    break;
            }
        }
    }

    if (cur == NULL)
        msg_abort("no ip addr");

    freeifaddrs(ifaddr);

    /* sanity check on port range */
    if (TEST_MAX_PORT - TEST_MIN_PORT < 0)
        msg_abort("bad min-max port");
    if (TEST_MIN_PORT < 1)
        msg_abort("bad min port");
    if (TEST_MAX_PORT > 65535)
        msg_abort("bad max port");

    if (myrank == 0)
        fprintf(stdout, "Info: Using port range [%d,%d]\n",
                TEST_MIN_PORT, TEST_MAX_PORT);

#if MPI_VERSION >= 3
    ret = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
                             MPI_INFO_NULL, &comm);
    if (ret != MPI_SUCCESS)
        msg_abort("MPI_Comm_split_type failed");
#else
    comm = MPI_COMM_WORLD;
#endif

    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);
    port = TEST_MIN_PORT + (rank % (1 + TEST_MAX_PORT - TEST_MIN_PORT));
    for (; port <= TEST_MAX_PORT; port += size) {
        int so, n = 1;
        struct sockaddr_in addr;

        /* test port availability */
        so = socket(PF_INET, SOCK_STREAM, 0);
        setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n));
        if (so != -1) {
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(port);
            n = bind(so, (struct sockaddr*)&addr, sizeof(addr));
            close(so);
            if (n == 0) {
                break; /* done */
            }
        } else {
            msg_abort("socket");
        }
    }

    if (port > TEST_MAX_PORT) {
        int so, n = 1;
        struct sockaddr_in addr;
        socklen_t addr_len;

        port = 0;
        fprintf(stderr, "Warning: no free ports available within the specified "
                "range\n>>> auto detecting ports ...\n");
        so = socket(PF_INET, SOCK_STREAM, 0);
        setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n));
        if (so != -1) {
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(0);
            n = bind(so, (struct sockaddr*)&addr, sizeof(addr));
            if (n == 0) {
                n = getsockname(so, (struct sockaddr*)&addr, &addr_len);
                if (n == 0) {
                    port = ntohs(addr.sin_port);
                    /* okay */
                }
            }
            close(so);
        } else {
            msg_abort("socket");
        }
    }

    if (port == 0)
        msg_abort("no free ports");

    /* add proto */
    sprintf(buf, "%s://%s:%d", TEST_PROTO, ip, port);
    if (myrank == 0)
        fprintf(stdout, "Info: Using address %s\n", buf);

    return (buf);
}

int main(int argc, char **argv)
{
    char hgaddr[128];
    hg_class_t *hgcl;
    //hg_context_t *hgctx;
    //hg_id_t hgid;

    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        perror("Error: MPI_Init failed");
        exit(1);
    }

    /* Create Mercury instance */
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    prepare_addr(hgaddr);
    fprintf(stderr, "Generated address: %s\n", hgaddr);
    hgcl = HG_Init(hgaddr, HG_TRUE);
    if (!hgcl)
        msg_abort("HG_Init failed");

    /* TODO: Register RPCs */

    if (nexus_bootstrap(hgcl)) {
        fprintf(stderr, "Error: nexus_bootstrap failed\n");
        goto error;
    }

    /* TODO: Exchange RPCs */

    if (nexus_destroy()) {
        fprintf(stderr, "Error: nexus_destroy failed\n");
        goto error;
    }

    /* Destroy Mercury instance */
    if (hgcl)
        HG_Finalize(hgcl);
    MPI_Finalize();
    exit(0);

error:
    MPI_Finalize();
    exit(1);
}
