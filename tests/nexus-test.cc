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
#include <mpi.h>

#include <deltafs_nexus.h>

struct test_ctx {
    int myrank;
    int ranksize;

    int count;
    int minport;
    int maxport;
    char subnet[16];
    char proto[8];

    /* Nexus context */
    nexus_ctx_t nctx;
};

char *me;
struct test_ctx tctx;

/*
 * usage: prints usage information and exits
 */
static void usage(int ret)
{
    printf("usage: %s [options]\n"
           "\n"
           "options:\n"
           " -c count       number of RPCs to perform\n"
           " -p baseport    base port number\n"
           " -t proto       transport protocol\n"
           " -s subnet      subnet for Mercury instances\n"
           " -h             this usage info\n"
           "\n", me);

    exit(ret);
}

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

#if 0
void print_hg_addr(hg_class_t *hgcl, int rank, const char *str)
{
    char *addr_str = NULL;
    hg_size_t addr_size = 0;
    hg_addr_t hgaddr;
    hg_return_t hret;

    if (nexus_get_addr(rank, &hgaddr))
        msg_abort("nexus_get_addr failed");

    hret = HG_Addr_to_string(hgcl, NULL, &addr_size, hgaddr);
    if (hgaddr == NULL)
        msg_abort("HG_Addr_to_string failed");

    addr_str = (char *)malloc(addr_size);
    if (addr_str == NULL)
        msg_abort("malloc failed");

    hret = HG_Addr_to_string(hgcl, addr_str, &addr_size, hgaddr);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Addr_to_string failed");

    fprintf(stdout, "[r%d] %s addr: %s\n", rank, str, addr_str);
}
#endif

int main(int argc, char **argv)
{
    int c;
    char *end;

    me = argv[0];

    /* set random data generator seed */
    srandom(getpid());

    /* set default parameter values */
    tctx.count = 2;
    tctx.minport = 50000;
    tctx.maxport = 59999;

    if (snprintf(tctx.subnet, sizeof(tctx.subnet), "127.0.0.1") <= 0)
        msg_abort("sprintf for subnet failed");

    if (snprintf(tctx.proto, sizeof(tctx.proto), "bmi+tcp") <= 0)
        msg_abort("sprintf for proto failed");

    while ((c = getopt(argc, argv, "c:p:t:s:h")) != -1) {
        switch(c) {
        case 'h': /* print help */
            usage(0);
        case 'c': /* number of RPCs to transport */
            tctx.count = strtol(optarg, &end, 10);
            if (*end) {
                perror("Error: invalid RPC count");
                usage(1);
            }
            break;
        case 'p': /* base port number */
            tctx.minport = strtol(optarg, &end, 10);
            if (*end) {
                perror("Error: invalid base port");
                usage(1);
            }
            tctx.maxport = tctx.minport + 9999;
            break;
        case 't': /* transport protocol */
            if (!strncpy(tctx.proto, optarg, sizeof(tctx.proto))) {
                perror("Error: invalid proto");
                usage(1);
            }
            break;
        case 's': /* subnet to pick IP from */
            if (!strncpy(tctx.subnet, optarg, sizeof(tctx.subnet))) {
                perror("Error: invalid subnet");
                usage(1);
            }
            break;
        default:
            usage(1);
        }
    }

    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        perror("Error: MPI_Init failed");
        exit(1);
    }

    MPI_Comm_size(MPI_COMM_WORLD, &(tctx.ranksize));
    MPI_Comm_rank(MPI_COMM_WORLD, &(tctx.myrank));

    /* Output test configuration */
    if (!tctx.myrank) {
        printf("\n%s options:\n", me);
        printf("\tTrials = %d\n", tctx.count);
        printf("\tPorts used = %d - %d\n", tctx.minport, tctx.maxport);
        printf("\tSubnet = %s\n", tctx.subnet);
        printf("\tProtocol = %s\n", tctx.proto);
    }

    if (nexus_bootstrap(&(tctx.nctx), tctx.minport, tctx.maxport,
                        tctx.subnet, tctx.proto) != NX_SUCCESS) {
        fprintf(stderr, "Error: nexus_bootstrap failed\n");
        goto error;
    }

    for (int i = 1; i <= tctx.count; i++) {
        int srcrep = -1, dstrep = -1, dest = -1;
        int src = tctx.myrank;
        int dst = rand() % tctx.ranksize; /* not uniform, but ok */
        hg_addr_t sr_addr, dr_addr, d_addr;
        nexus_ret_t nret;

        /* Get srcrep */
        nret = nexus_next_hop(&(tctx.nctx), dst, &srcrep, &sr_addr);
        if (nret == NX_DONE) {
            fprintf(stdout, "[r%d,i%d] Route: src (%d) and dst (%d) overlap\n",
                    src, i, src, dst);
            continue;
        } else if (nret == NX_ISLOCAL) {
            fprintf(stdout, "[r%d,i%d] Route src (%d) and dst (%d) is local\n",
                    src, i, src, dst);
            continue;
        } else if (nret == NX_SRCREP) {
            /* Get dstrep */
            tctx.nctx.grank = srcrep; /* Trick Nexus to think we advanced */
            nret = nexus_next_hop(&(tctx.nctx), dst, &dstrep, &dr_addr);

            /* Don't look for dest because we only have rep addresses at src */
            if (nret == NX_DESTREP || nret == NX_SUCCESS)
                goto done;
            else
                msg_abort("nexus_next_hop for destrep failed");
        } else if (nret == NX_DESTREP) {
            goto done;
        } else if (nret != NX_SUCCESS) {
            msg_abort("nexus_next_hop for srcrep failed");
        }

#if 0
        print_hg_addr(tctx.hgcl, srcrep, "srcrep");
#endif
done:
        tctx.nctx.grank = src;
        fprintf(stdout, "[r%d,i%d] Route: src=%d -> src_rep=%d"
                        " -> dst_rep=%d -> dst=%d\n",
                src, i, src, srcrep, dstrep, dst);
    }

    if (nexus_destroy(&(tctx.nctx))) {
        fprintf(stderr, "Error: nexus_destroy failed\n");
        goto error;
    }

    MPI_Finalize();
    exit(0);

error:
    MPI_Finalize();
    exit(1);
}
