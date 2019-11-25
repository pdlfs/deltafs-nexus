/*
 * Copyright (c) 2019, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * nexus-dumper  bring up nexus and dump out its routing state
 * 22-Nov-2019  chuck@ece.cmu.edu
 */

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/utsname.h>

#include <mpi.h>

/*
 * ipurl and mpi_localcfg helper routines
 */
/* start: ipurl */
#include <ifaddrs.h>
#include <netdb.h>
// #include <stdio.h>
// #include <stdlib.h>
#include <string.h>
// #include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <netinet/in.h>

/*
 * mercury_gen_ipurl: generate a mercury IP url using subnet spec
 * to select the network to use.
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

    snetlen = strlen(subnet);

    /* walk list looking for match */
    for (cur = ifaddr ; cur != NULL ; cur = cur->ifa_next) {

        /* skip interfaces without an IP address */
        if (cur->ifa_addr == NULL || cur->ifa_addr->sa_family != AF_INET)
            continue;

        /* get full IP address */
        if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in),
                        tmpip, sizeof(tmpip), NULL, 0, NI_NUMERICHOST) == -1)
            continue;

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

#include "deltafs-nexus_api.h"

/*
 * usage: nexus-dumper [-s subnet] net-protocol [outfile]
 */
int main(int argc, char **argv) {
    char *myprog = argv[0];
    int exitval = 0;
    int opt, lr, ls, lbase;
    struct utsname uts;
    struct hostent *he;
    struct in_addr ia;
    char *proto, *subnet, *outfile, *myurl;
    hg_class_t *cls;
    hg_context_t *ctx;
    nexus_ctx_t nctx = NULL;

    subnet = NULL;
    while ((opt = getopt(argc, argv, "s:")) != -1) {
        switch (opt) {
        case 's':
            subnet = optarg;
            break;
        default:
            fprintf(stderr, "%s: bad flag\n", myprog);
            exit(1);
        }
    }
    argc -= optind;
    argv += optind;
    
    if (subnet == NULL) {   /* pick default IP address */
        if (uname(&uts) < 0) {
            perror("uname");
            exit(1);
        }
        he = gethostbyname(uts.nodename);
        if (!he || he->h_addrtype != AF_INET || he->h_length < 1) {
            fprintf(stderr, "%s: gethostbyname(%s) failed\n", myprog,
                    uts.nodename);
            exit(1);
        }
        memcpy(&ia, he->h_addr, sizeof(ia));
        subnet = inet_ntoa(ia);
    }

    if (argc < 1 || argc > 2) {
        fprintf(stderr, "usage: %s net-protocol [outfile]\n", myprog);
        exit(1);
    }
    proto = argv[0];
    outfile = (argc == 2) ? argv[1] : NULL;
   
    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        fprintf(stderr, "MPI_Init failed\n");
        exitval = 1; goto done;
    }
    
    if (strcmp(proto, "bmi+tcp") == 0) {
        if (mpi_localcfg(MPI_COMM_WORLD, &lr, &ls) < 0) {
            fprintf(stderr, "nexus-test: mpi_localcfg failed!\n");
            exitval = 1; goto done;
        }
        lbase = 10000 + lr;
    } else {
        lbase = lr = ls = 0;
    }
    myurl = mercury_gen_ipurl(proto, subnet, 0, lbase, ls);
    if (!myurl) {
        fprintf(stderr, "%s: mercury_gen_ipurl failed!\n", myprog);
        exitval = 1; goto done;
    }

    nctx = nexus_bootstrap(subnet, proto);
    if (!nctx) {
        fprintf(stderr, "nexus-test: nexus bootstrap failed!\n");
        exitval = 1; goto done;
    }

    nexus_dump(nctx, outfile);

done:
    if (nctx) nexus_destroy(nctx);
    MPI_Finalize();
    exit(exitval);
}
