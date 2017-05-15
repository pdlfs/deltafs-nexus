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
#include <ifaddrs.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <mpi.h>

#include "deltafs_nexus.h"

#define DEFAULT_MIN_PORT 50000
#define DEFAULT_MAX_PORT 59999
#define DEFAULT_SUBNET "127.0.0.1"
#define DEFAULT_PROTO "bmi+tcp"

int myrank;

/*
 * prepare_addr(): obtain the mercury addr to bootstrap the rpc
 *
 * Write the server uri into *buf on success.
 *
 * Abort on errors.
 */
static const char* prepare_addr(char* buf)
{
    int family;
    int port;
    const char* env;
    int min_port;
    int max_port;
    struct ifaddrs *ifaddr, *cur;
    struct sockaddr_in addr;
    socklen_t addr_len;
    MPI_Comm comm;
    int rank;
    int size;
    const char* subnet;
    char msg[100];
    char ip[50]; // ip
    int so;
    int rv;
    int n;

    /* figure out our ip addr by query the local socket layer */
    if (getifaddrs(&ifaddr) == -1)
        fprintf(stderr, "Error: getifaddrs failed\n");

    subnet = DEFAULT_SUBNET;

    if (myrank == 0) {
        snprintf(msg, sizeof(msg), "using subnet %s*", subnet);
        if (strcmp(subnet, "127.0.0.1") == 0) {
            fprintf(stderr, "Warning: %s\n", msg);
        } else {
            fprintf(stderr, "Info: %s\n", msg);
        }
    }

    for (cur = ifaddr; cur != NULL; cur = cur->ifa_next) {
        if (cur->ifa_addr != NULL) {
            family = cur->ifa_addr->sa_family;

            if (family == AF_INET) {
                if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in), ip,
                                sizeof(ip), NULL, 0, NI_NUMERICHOST) == -1)
                    fprintf(stderr, "Error: getnameinfo failed\n");

                if (strncmp(subnet, ip, strlen(subnet)) == 0) {
                    break;
                }
            }
        }
    }

    if (cur == NULL) /* maybe a wrong subnet has been specified */
        fprintf(stderr, "Error: no ip addr\n");

    freeifaddrs(ifaddr);

    /* get port through MPI rank */
    min_port = DEFAULT_MIN_PORT;
    max_port = DEFAULT_MAX_PORT;

    /* sanity check on port range */
    if (max_port - min_port < 0)
        fprintf(stderr, "Error: bad min-max port\n");
    if (min_port < 1)
        fprintf(stderr, "Error: bad min port\n");
    if (max_port > 65535)
        fprintf(stderr, "Error: bad max port\n");

    if (myrank == 0) {
        snprintf(msg, sizeof(msg), "using port range [%d,%d]", min_port,
                 max_port);
        fprintf(stderr, "Info: %s\n", msg);
    }

#if MPI_VERSION >= 3
    rv = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
                             MPI_INFO_NULL, &comm);
    if (rv != MPI_SUCCESS)
        fprintf(stderr, "Error: MPI_Comm_split_type failed\n");
#else
    comm = MPI_COMM_WORLD;
#endif

    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);
    port = min_port + (rank % (1 + max_port - min_port));
    for (; port <= max_port; port += size) {
        n = 1;
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
            fprintf(stderr, "Error: socket error\n");
        }
    }

    if (port > max_port) {
        port = 0;
        n = 1;
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
            fprintf(stderr, "Error: socket error\n");
        }
    }

    if (port == 0) /* maybe a wrong port range has been specified */
        fprintf(stderr, "Error: no free ports\n");

    /* add proto */
    env = DEFAULT_PROTO;
    sprintf(buf, "%s://%s:%d", env, ip, port);
    if (myrank == 0) {
        snprintf(msg, sizeof(msg), "using %s", env);
        if (strstr(env, "tcp") != NULL) {
            fprintf(stderr, "Warning: %s\n", msg);
        } else {
            fprintf(stderr, "Info: %s\n", msg);
        }
    }

    return (buf);
}

int main(int argc, char **argv)
{
    char hgaddr[128];

    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        perror("Error: MPI_Init failed");
        exit(1);
    }

    //MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

    prepare_addr(hgaddr);
    fprintf(stderr, "Generated address: %s\n", hgaddr);

#if 0
    if (nexus_bootstrap(NULL)) {
        fprintf(stderr, "Error: nexus_bootstrap failed\n");
        goto error;
    }

    if (nexus_destroy()) {
        fprintf(stderr, "Error: nexus_destroy failed\n");
        goto error;
    }
#endif

    MPI_Finalize();

    exit(0);

error:
    MPI_Finalize();
    exit(1);
}
