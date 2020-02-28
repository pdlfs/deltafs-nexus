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
#include <string.h>

#include "deltafs-nexus_api.h"

/* start: ipurl */
#include <ifaddrs.h>
#include <netdb.h>
#include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// #include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <netinet/in.h>


/*
 * mercury_gen_ipurl: generate a mercury IP url using subnet spec
 * to select the network to use.  if subnet is NULL or empty, we
 * use the first non-127.0.0.1 IP address.
 *
 * @param protocol mercury protocol (e.g. "bmi+tcp")
 * @param subnet IP subnet to use (e.g. "10.92")
 * @param port port number to use, zero means any
 * @param wa_base work-around base port (for bmi+tcp workaround)
 * @param wa_stride work-around port stride (for bmi+tcp workaround)
 * @return a malloc'd buffer with the new URL, or NULL on error
 */
char *mercury_gen_ipurl(char *protocol, char *subnet, int port,
                        int wa_base, int wa_stride) {
    int snetlen, rlen, so, n, lcv;
    struct ifaddrs *ifaddr, *cur;
    char tmpip[16];   /* strlen("111.222.333.444") == 15 */
    char *ret;
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);

    /* query socket layer to get our IP address list */
    if (getifaddrs(&ifaddr) == -1) {
        fprintf(stderr, "mercury_gen_ipurl: getifaddrs failed?\n");
        return(NULL);
    }

    snetlen = (subnet) ? strlen(subnet) : 0;

    /* walk list looking for match */
    for (cur = ifaddr ; cur != NULL ; cur = cur->ifa_next) {

        /* skip interfaces without an IP address */
        if (cur->ifa_addr == NULL || cur->ifa_addr->sa_family != AF_INET)
            continue;

        /* get full IP address */
        if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in),
                        tmpip, sizeof(tmpip), NULL, 0, NI_NUMERICHOST) == -1)
            continue;

        if (snetlen == 0) {
          if (strcmp(tmpip, "127.0.0.1") == 0)
            continue; /* skip localhost */
          break;      /* take first non-localhost match */
        }
        if (strncmp(subnet, tmpip, snetlen) == 0)
          break;
    }

    /* dump the ifaddr list and return if there was no match */
    freeifaddrs(ifaddr);
    if (cur == NULL)
        return(NULL);

    rlen = strlen(protocol) + 32; /* +32 enough for ip, port, etc. */
    ret = (char *)malloc(rlen);
    if (ret == NULL)
      return(NULL);

    if (port != 0 || strcmp(protocol, "bmi+tcp") != 0) {
        /* set port 0, let OS fill it, collect later w/HG_Addr_to_string */
        snprintf(ret, rlen, "%s://%s:%d", protocol, tmpip, port);
        return(ret);
    }

    /*
     * XXX: bmi+tcp HG_Addr_to_string() is broken.  if we request
     * port 0 (to let the OS fill it in) and later use HG_Addr_to_string()
     * to request the actual port number allocated, it still returns
     * 0 as the port number...  here's an attempt to hack around this
     * problem.  we take wa_base and wa_stride as hints on how to pick
     * a port number so that it doesn't conflict with other local ports.
     * e.g. wa_base= X+my_local_rank, wa_stride=#local_ranks
     */
    if (wa_base < 1) wa_base = 10000;
    if (wa_stride < 1) wa_stride = 1;
    so = socket(PF_INET, SOCK_STREAM, 0);
    if (so < 0) {
        perror("socket");
        free(ret);
        return(NULL);
    }
    n = 1;
    setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n));
    for (lcv = 0 ; lcv < 1024 ; lcv++) {   /* try up to 1024 times */
        port = wa_base + (lcv * wa_stride);
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);
        n = bind(so, (struct sockaddr*)&addr, addr_len);
        if (n == 0) break;
    }
    close(so);

    if (n != 0) {
        perror("bind");
        free(ret);
        return(NULL);
    }

    snprintf(ret, rlen, "%s://%s:%d", protocol, tmpip, port);
    return(ret);
}
/* end: ipurl  */
/* start: mpi_localcfg */
/**
 * mpi_localcfg: get local-node mpi config (for working around bmi+tcp issues)
 *
 * assumes MPI has been init'd.  this is a collective MPI call.
 *
 * @param world our top-level comm
 * @param lrnk local rank will be placed here
 * @param lsz local size will be placed here
 * @return 0 or -1 on error
 */
int mpi_localcfg(MPI_Comm world, int *lrnk, int *lsz) {
    MPI_Comm local;
    int ok;

    /* split the world into local and remote */
    if (MPI_Comm_split_type(world, MPI_COMM_TYPE_SHARED, 0,
                            MPI_INFO_NULL, &local) != MPI_SUCCESS)
    return(-1);

    ok = MPI_Comm_rank(local, lrnk) == MPI_SUCCESS &&
         MPI_Comm_size(local, lsz) == MPI_SUCCESS;

    MPI_Comm_free(&local);    /* ignore errors */
    return(ok ? 0 : -1);
}
/* end: mpi_localcfg */

struct test_ctx {
    int myrank;
    int ranksize;

    int count;
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
static inline void nx_fatal(const char *msg)
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
    int c, lr, ls, lbase;
    char *end, *myurl = NULL;
    hg_class_t *cls = NULL;
    hg_context_t *ctx = NULL;
    progressor_handle_t *prg = NULL;

    me = argv[0];

    /* set random data generator seed */
    srandom(getpid());

    /* set default parameter values */
    tctx.count = 2;

    if (snprintf(tctx.subnet, sizeof(tctx.subnet), "127.0.0.1") <= 0)
      nx_fatal("sprintf for subnet failed");

    if (snprintf(tctx.proto, sizeof(tctx.proto), "bmi+tcp") <= 0)
      nx_fatal("sprintf for proto failed");

    while ((c = getopt(argc, argv, "c:t:s:h")) != -1) {
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
        printf("\tSubnet = %s\n", tctx.subnet);
        printf("\tProtocol = %s\n", tctx.proto);
    }

    if (strcmp(tctx.proto, "bmi+tcp") == 0) {
        if (mpi_localcfg(MPI_COMM_WORLD, &lr, &ls) < 0) {
            fprintf(stderr, "nexus-test: mpi_localcfg failed!\n");
            exit(1);
        }
        lbase = 10000 + lr;
    } else {
        lbase = lr = ls = 0;
    }
    myurl = mercury_gen_ipurl(tctx.proto, tctx.subnet, 0, lbase, ls);
    if (!myurl) {
        fprintf(stderr, "nexus-test: mercury_gen_ipurl failed!\n");
        goto error;
    }

    if ((cls = HG_Init(myurl, HG_TRUE)) == NULL) {
        fprintf(stderr, "nexus-test: HG_Init(%s, TRUE) failed!\n", myurl);
        goto error;
    }
    if ((ctx = HG_Context_create(cls)) == NULL) {
        fprintf(stderr, "nexus-test: HG_Context_create() failed!\n");
        goto error;
    }
    if ((prg = mercury_progressor_init(cls, ctx)) == NULL) {
        fprintf(stderr, "nexus-test: progressor init failed!\n");
        goto error;
    }

    if (!(tctx.nctx = nexus_bootstrap(prg, NULL))) {
        fprintf(stderr, "Error: nexus_bootstrap failed\n");
        goto error;
    }

    for (int i = 1; i <= tctx.count; i++) {
        int srcrep = -1, dstrep = -1, dest = -1;
        int src = tctx.myrank;
        int dst = rand() % tctx.ranksize; /* not uniform, but ok */
        hg_addr_t sr_addr, dr_addr, d_addr;
        nexus_ret_t nret;

        fprintf(stdout, "[r%d,i%d] Trying to route: src=%d -> ... -> dst=%d\n",
                src, i, src, dst);

        /* Get srcrep */
        nret = nexus_next_hop(tctx.nctx, dst, &srcrep, &sr_addr);
#if 0
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
            fprintf(stdout, "[r%d,i%d] got srcrep = %d\n", src, i, srcrep);
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
#endif

#if 0
        print_hg_addr(tctx.hgcl, srcrep, "srcrep");
#endif
done:
        nexus_set_grank(tctx.nctx, src);
        fprintf(stdout, "[r%d,i%d] Route: src=%d -> src_rep=%d"
                        " -> dst_rep=%d -> dst=%d\n",
                src, i, src, srcrep, dstrep, dst);
    }

    if (myurl) free(myurl);
    nexus_destroy(tctx.nctx);
    if (prg) mercury_progressor_freehandle(prg);
    if (ctx) HG_Context_destroy(ctx);
    if (cls) HG_Finalize(cls);
    MPI_Finalize();
    exit(0);

error:
    if (myurl) free(myurl);
    if (prg) mercury_progressor_freehandle(prg);
    if (ctx) HG_Context_destroy(ctx);
    if (cls) HG_Finalize(cls);
    MPI_Finalize();
    exit(1);
}
