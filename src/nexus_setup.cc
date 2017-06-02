/*
 * Copyright (c) 2017, Carnegie Mellon University.
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

#include <ifaddrs.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <pthread.h>

#include "nexus_internal.h"

typedef struct {
    char addr[HGADDRSZ];
    int grank;
    int lrank;
} xchg_dat_t;

typedef struct {
    hg_context_t *hgctx;
    int bgdone;
} bgthread_dat_t;

/*
 * Network support pthread. Need to call progress to push the network and then
 * trigger to run the callback.
 */
static void *nexus_bgthread(void *arg)
{
    bgthread_dat_t *bgdat = (bgthread_dat_t *)arg;
    hg_return_t hret;

#ifdef NEXUS_DEBUG
    fprintf(stdout, "Network thread running\n");
#endif

    /* while (not done sending or not done recving */
    while (!bgdat->bgdone) {
        unsigned int count = 0;

        do {
            hret = HG_Trigger(bgdat->hgctx, 0, 1, &count);
        } while (hret == HG_SUCCESS && count);

        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            msg_abort("nexus_bgthread: HG_Trigger failed");

        hret = HG_Progress(bgdat->hgctx, 100);
        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            msg_abort("nexus_bgthread: HG_Progress failed");
    }

    return NULL;
}

/*
 * Put together the remote Mercury endpoint address from bootstrap parameters.
 * Writes the server URI into *uri on success. Aborts on error.
 */
static void prepare_addr(nexus_ctx_t *nctx, int minport, int maxport,
                         char *subnet, char *proto, char *uri)
{
    struct ifaddrs *ifaddr, *cur;
    int family, ret, rank, size, port;
    char ip[16];

    /* Query local socket layer to get our IP addr */
    if (getifaddrs(&ifaddr) == -1)
        msg_abort("getifaddrs failed");

    for (cur = ifaddr; cur != NULL; cur = cur->ifa_next) {
        if (cur->ifa_addr != NULL) {
            family = cur->ifa_addr->sa_family;

            if (family == AF_INET) {
                if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in), ip,
                                sizeof(ip), NULL, 0, NI_NUMERICHOST) == -1)
                    msg_abort("getnameinfo failed");

                if (strncmp(subnet, ip, strlen(subnet)) == 0)
                    break;
            }
        }
    }

    if (cur == NULL)
        msg_abort("no ip addr");

    freeifaddrs(ifaddr);

    /* sanity check on port range */
    if (maxport - minport < 0)
        msg_abort("bad min-max port");
    if (minport < 1)
        msg_abort("bad min port");
    if (maxport > 65535)
        msg_abort("bad max port");

    MPI_Comm_rank(nctx->localcomm, &rank);
    MPI_Comm_size(nctx->localcomm, &size);
    port = minport + (rank % (1 + maxport - minport));
    for (; port <= maxport; port += size) {
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
            if (n == 0)
                break; /* done */
        } else {
            msg_abort("socket");
        }
    }

    if (port > maxport) {
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
                if (n == 0)
                    port = ntohs(addr.sin_port); /* okay */
            }
            close(so);
        } else {
            msg_abort("socket");
        }
    }

    if (port == 0)
        msg_abort("no free ports");

    /* add proto */
    sprintf(uri, "%s://%s:%d", proto, ip, port);
#ifdef NEXUS_DEBUG
    fprintf(stdout, "Info: Using address %s\n", uri);
#endif
}

typedef struct hg_lookup_out {
    hg_return_t hret;
    nexus_ctx_t *nctx;
    int grank;

    /* The address map we'll be updating */
    nexus_map_t *map;

    /* State to track progress */
    int *count;
    pthread_mutex_t *cb_mutex;
    pthread_cond_t *cb_cv;
} hg_lookup_out_t;

static hg_return_t hg_lookup_cb(const struct hg_cb_info *info)
{
    hg_lookup_out_t *out = (hg_lookup_out_t *)info->arg;
    nexus_ctx_t *nctx = out->nctx;
    out->hret = info->ret;

    pthread_mutex_lock(out->cb_mutex);

    /* Add address to map */
    if (out->hret != HG_SUCCESS)
        (*(out->map))[out->grank] = HG_ADDR_NULL;
    else
        (*(out->map))[out->grank] = info->info.lookup.addr;

    *(out->count) += 1;
    pthread_cond_signal(out->cb_cv);
    pthread_mutex_unlock(out->cb_mutex);

    return HG_SUCCESS;
}

static hg_return_t lookup_addrs(nexus_ctx_t *nctx, hg_context_t *hgctx,
                                xchg_dat_t *xarray, int xsize, nexus_map_t *map)
{
    hg_lookup_out_t *out = NULL;
    hg_return_t hret;
    pthread_mutex_t cb_mutex;
    pthread_cond_t cb_cv;
    int count = 0;

    /* Init addr metadata */
    out = (hg_lookup_out_t *)malloc(sizeof(*out) * xsize);
    if (out == NULL)
        return HG_NOMEM_ERROR;

    pthread_mutex_init(&cb_mutex, NULL);
    pthread_cond_init(&cb_cv, NULL);

    pthread_mutex_lock(&cb_mutex);

    /* Post all lookups */
    for (int i = 0; i < xsize; i++) {
        int eff_i = (nctx->grank + i) % xsize;

        /* Populate out struct */
        out[eff_i].hret = HG_SUCCESS;
        out[eff_i].nctx = nctx;
        out[eff_i].map = map;
        out[eff_i].grank = xarray[eff_i].grank;
        out[eff_i].count = &count;
        out[eff_i].cb_mutex = &cb_mutex;
        out[eff_i].cb_cv = &cb_cv;

#ifdef NEXUS_DEBUG
        fprintf(stdout, "[%d] Idx %d: addr %s, grank %d (xsize = %d)\n",
                nctx->grank, eff_i, xarray[eff_i].addr, xarray[eff_i].grank, xsize);
#endif

        hret = HG_Addr_lookup(hgctx, &hg_lookup_cb, &out[eff_i],
                              xarray[eff_i].addr, HG_OP_ID_IGNORE);
        if (hret != HG_SUCCESS) {
            pthread_mutex_unlock(&cb_mutex);
            goto err;
        }
    }

    /* Lookup posted, wait until finished */
again:
    if (count < xsize) {
        pthread_cond_wait(&cb_cv, &cb_mutex);
        if (count < xsize)
            goto again;
    }
    pthread_mutex_unlock(&cb_mutex);

    hret = HG_SUCCESS;
    for (int i = 0; i < xsize; i++) {
        if (out[i].hret != HG_SUCCESS) {
            hret = out[i].hret;
            goto err;
        }
    }

err:
    pthread_cond_destroy(&cb_cv);
    pthread_mutex_destroy(&cb_mutex);
    free(out);
    return hret;
}

static void discover_local_info(nexus_ctx_t *nctx)
{
    int ret;
    char hgaddr[HGADDRSZ];
    xchg_dat_t xitem;
    xchg_dat_t *xarray;
    hg_return_t hret;
    pthread_t bgthread; /* network background thread */
    bgthread_dat_t *bgarg;

    MPI_Comm_rank(nctx->localcomm, &(nctx->lrank));
    MPI_Comm_size(nctx->localcomm, &(nctx->lsize));

    /* Initialize local Mercury listening endpoints */
    snprintf(hgaddr, sizeof(hgaddr), "na+sm://%d/0", getpid());
#ifdef NEXUS_DEBUG
    fprintf(stderr, "Initializing for %s\n", hgaddr);
#endif

    nctx->local_hgcl = HG_Init(hgaddr, HG_TRUE);
    if (!nctx->local_hgcl)
        msg_abort("HG_init failed for local endpoint");

    nctx->local_hgctx = HG_Context_create(nctx->local_hgcl);
    if (!nctx->local_hgctx)
        msg_abort("HG_Context_create failed for local endpoint");

    /* Start the network thread */
    bgarg = (bgthread_dat_t *)malloc(sizeof(*bgarg));
    if (!bgarg)
        msg_abort("malloc failed");

    bgarg->hgctx = nctx->local_hgctx;
    bgarg->bgdone = 0;

    ret = pthread_create(&bgthread, NULL, nexus_bgthread, (void*)bgarg);
    if (ret != 0)
        msg_abort("pthread_create failed");

    /* Exchange addresses, global, and local ranks in local comm */
    strncpy(xitem.addr, hgaddr, sizeof(xitem.addr));
    xitem.grank = nctx->grank;
    xitem.lrank = nctx->lrank;

    xarray = (xchg_dat_t *)malloc(sizeof(xchg_dat_t) * (nctx->lsize));
    if (!xarray)
        msg_abort("malloc failed");

    nctx->local2global = (int *)malloc(sizeof(int) * nctx->lsize);
    if (!nctx->local2global)
        msg_abort("malloc failed");

    MPI_Allgather(&xitem, sizeof(xchg_dat_t), MPI_BYTE,
                  xarray, sizeof(xchg_dat_t), MPI_BYTE, nctx->localcomm);

    /* Build map of local to global ranks, find local root */
    for (int i = 0; i < nctx->lsize; i++) {
        nctx->local2global[xarray[i].lrank] = xarray[i].grank;
        if (xarray[i].lrank == 0) {
            nctx->lroot = xarray[i].grank;
            break;
        }
    }

    /* Look up local Mercury addresses */
    if (lookup_addrs(nctx, nctx->local_hgctx, xarray,
                     nctx->lsize, &nctx->laddrs) != HG_SUCCESS)
        msg_abort("lookup_addrs failed");

#ifdef NEXUS_DEBUG
    print_addrs(nctx, nctx->local_hgcl, nctx->laddrs);
#endif

    /* Sync before terminating background threads */
    MPI_Barrier(nctx->localcomm);

    /* Terminate network thread */
    bgarg->bgdone = 1;
    pthread_join(bgthread, NULL);

    free(bgarg);
    free(xarray);
}

#define NADDR(x, y) (&x[y * HGADDRSZ])

static void find_remote_addrs(nexus_ctx_t *nctx, char *myaddr)
{
    int i, npeers, maxnodesz, maxpeers;
    int *msgdata, *nodelists;
    char *rank2addr;
    xchg_dat_t *paddrs;

    /* If we're alone stop here */
    if (nctx->nodesz == 1)
        return;

    /*
     * To find our peer on each node, we need to construct a list of ranks per
     * node. This is tricky with collectives if we want to support heterogeneous
     * nodes. We do this in five steps:
     * - we all-gather a rank => address array
     * - find max number of ranks per node, M
     * - local roots construct (M+1)-sized array of (# ranks, r0, r1, ...)
     * - local roots all-gather (M+1)-sized arrays (on repcomm)
     * - local roots broadcast finished array (on localcomm)
     */

    /* Step 1: build rank => address array */
    rank2addr = (char *)malloc(sizeof(char) * nctx->gsize * HGADDRSZ);
    if (!rank2addr)
        msg_abort("malloc failed");

    MPI_Allgather(myaddr, HGADDRSZ, MPI_BYTE,
                  rank2addr, HGADDRSZ, MPI_BYTE, MPI_COMM_WORLD);
#ifdef NEXUS_DEBUG
    for (i = 0; i < nctx->gsize; i++)
        fprintf(stdout, "[%d] i = %d, data = %s\n",
                nctx->grank, i, NADDR(rank2addr, i));
#endif

    /* Step 2: find max number of ranks per node */
    MPI_Allreduce(&nctx->lsize, &maxnodesz, 1, MPI_INT,
                  MPI_MAX, MPI_COMM_WORLD);

    nodelists = (int *)malloc(sizeof(int) * (maxnodesz+1) * nctx->nodesz);
    if (!nodelists)
        msg_abort("malloc failed");

    if (nctx->grank != nctx->lroot)
        goto nonroot;

    /* Step 3: construct local node list */
    msgdata = (int *)malloc(sizeof(int) * (maxnodesz+1));
    if (!msgdata)
        msg_abort("malloc failed");

    i = 2;
    msgdata[0] = nctx->lsize;
    msgdata[1] = nctx->grank;
    for (nexus_map_t::iterator it = nctx->laddrs.begin();
                               it != nctx->laddrs.end(); it++)
        msgdata[i++] = it->first;

    /* Step 4: exchange local node lists */
    MPI_Allgather(msgdata, maxnodesz + 1, MPI_INT,
                  nodelists, maxnodesz + 1, MPI_INT, nctx->repcomm);

    free(msgdata);
nonroot:
    /* Step 5: broadcast finished node lists array */
    MPI_Bcast(nodelists, (maxnodesz+1) * nctx->nodesz * sizeof(int),
              MPI_BYTE, 0, nctx->localcomm);
#ifdef NEXUS_DEBUG
    for (i = 0; i < ((maxnodesz+1) * nctx->nodesz); i++)
        fprintf(stdout, "[%d] i = %d, data = %d\n",
                nctx->grank, i, nodelists[i]);
#endif

    /*
     * Time to construct an array of peer addresses. We'll do this in two steps:
     * - find total number of peers, which is ceil(# nodes / # local ranks)
     * - construct array of peer addresses and look them all up
     */

    /* Step 1: find total number of peers */
    maxpeers = (nctx->nodesz / nctx->lsize) +
               ((nctx->nodesz % nctx->lsize) ? 1 : 0);

    paddrs = (xchg_dat_t *)malloc(sizeof(xchg_dat_t) * maxpeers);
    if (!paddrs)
        msg_abort("malloc failed");

    /*
     * We will communicate with a node, if its node ID modulo our node's rank
     * size is equal to our local-rank. Of each node we communicate with, we
     * will msg the remote local-rank that is equal to our node's ID modulo
     * the remote node's rank size.
     */
    i = nctx->lrank;
    npeers = 0;
    while (i < nctx->nodesz) {
        int idx = i * (maxnodesz + 1);
        int remote_grank, remote_lsize;
        char *remote_addr;

        /* Skip ourselves */
        if (i == nctx->nodeid) {
            i += nctx->lsize;
            continue;
        }

        remote_lsize = nodelists[idx];
        remote_grank = nodelists[idx + 1 + (nctx->nodeid % remote_lsize)];
        remote_addr = NADDR(rank2addr, remote_grank);

        strncpy(paddrs[npeers].addr, remote_addr, HGADDRSZ);
        paddrs[npeers].grank = i; /* store node ID, not rank */

        i += nctx->lsize;
        npeers += 1;
    }

    /* Get rid of arrays we don't need anymore */
    free(rank2addr);
    free(nodelists);

    /* No peers? Stop here. */
    if (!npeers) {
        free(paddrs);
        return;
    }

#ifdef NEXUS_DEBUG
    for (i = 0; i < npeers; i++)
        fprintf(stdout, "[%d] i = %d, grank = %d, addr = %s\n",
                nctx->grank, i, paddrs[i].grank, paddrs[i].addr);
#endif

    /* Step 2: lookup peer addresses */
    if (lookup_addrs(nctx, nctx->remote_hgctx, paddrs,
                     npeers, &nctx->gaddrs) != HG_SUCCESS)
        msg_abort("lookup_addrs failed");

#ifdef NEXUS_DEBUG
    print_addrs(nctx, nctx->remote_hgcl, nctx->gaddrs);
    fprintf(stdout, "[%d] printed gaddrs, npeers = %d\n", nctx->grank, npeers);
#endif

    free(paddrs);
}

static void discover_remote_info(nexus_ctx_t *nctx, char *hgaddr)
{
    int ret;
    pthread_t bgthread; /* network background thread */
    bgthread_dat_t *bgarg;

    /* Find node IDs, number of nodes and broadcast them */
    if (nctx->grank == nctx->lroot) {
        MPI_Comm_rank(nctx->repcomm, &nctx->nodeid);
        MPI_Comm_size(nctx->repcomm, &nctx->nodesz);
    }

    MPI_Bcast(&nctx->nodeid, 1, MPI_INT, 0, nctx->localcomm);
    MPI_Bcast(&nctx->nodesz, 1, MPI_INT, 0, nctx->localcomm);

    /* Build global rank -> node id array */
    nctx->rank2node = (int *)malloc(sizeof(int) * nctx->gsize);
    if (!nctx->rank2node)
        msg_abort("malloc failed");

    MPI_Allgather(&nctx->nodeid, 1, MPI_INT, nctx->rank2node,
                  1, MPI_INT, MPI_COMM_WORLD);

    /* Initialize remote Mercury listening endpoints */
    nctx->remote_hgcl = HG_Init(hgaddr, HG_TRUE);
    if (!nctx->remote_hgcl)
        msg_abort("HG_Init failed for remote endpoint");

    nctx->remote_hgctx = HG_Context_create(nctx->remote_hgcl);
    if (!nctx->remote_hgctx)
        msg_abort("HG_Context_create failed for remote endpoint");

    /* Start the network thread */
    bgarg = (bgthread_dat_t *)malloc(sizeof(*bgarg));
    if (!bgarg)
        msg_abort("malloc failed");

    bgarg->hgctx = nctx->remote_hgctx;
    bgarg->bgdone = 0;

    ret = pthread_create(&bgthread, NULL, nexus_bgthread, (void*)bgarg);
    if (ret != 0)
        msg_abort("pthread_create failed");

    find_remote_addrs(nctx, hgaddr);

    /* Sync before terminating background threads */
    MPI_Barrier(MPI_COMM_WORLD);

    /* Terminate network thread */
    bgarg->bgdone = 1;
    pthread_join(bgthread, NULL);

    free(bgarg);
}

nexus_ret_t nexus_bootstrap(nexus_ctx_t *nctx, int minport, int maxport,
                            char *subnet, char *proto)
{
    char hgaddr[HGADDRSZ];

    /* Grab MPI rank info */
    MPI_Comm_rank(MPI_COMM_WORLD, &(nctx->grank));
    MPI_Comm_size(MPI_COMM_WORLD, &(nctx->gsize));

    if (!nctx->grank)
        fprintf(stdout, "Nexus: started bootstrap\n");

    init_local_comm(nctx);
    discover_local_info(nctx);

    if (!nctx->grank)
        fprintf(stdout, "Nexus: done local info discovery\n");

    prepare_addr(nctx, minport, maxport, subnet, proto, hgaddr);
    init_rep_comm(nctx);
    discover_remote_info(nctx, hgaddr);

    if (!nctx->grank)
        fprintf(stdout, "Nexus: done remote info discovery\n");

#ifdef NEXUS_DEBUG
    fprintf(stdout, "[%d] grank = %d, lrank = %d, gsize = %d, lsize = %d\n",
            nctx->grank, nctx->grank, nctx->lrank, nctx->gsize, nctx->lsize);
#endif /* NEXUS_DEBUG */

    return NX_SUCCESS;
}

nexus_ret_t nexus_destroy(nexus_ctx_t *nctx)
{
    nexus_map_t::iterator it;

    /* Free local Mercury addresses */
    for (it = nctx->laddrs.begin(); it != nctx->laddrs.end(); it++)
        if (it->second != HG_ADDR_NULL)
            HG_Addr_free(nctx->local_hgcl, it->second);

    /* Sync before tearing down local endpoints */
    MPI_Barrier(nctx->localcomm);
    MPI_Comm_free(&nctx->localcomm);

    /* Destroy Mercury local endpoints */
    HG_Context_destroy(nctx->local_hgctx);
    HG_Finalize(nctx->local_hgcl);

    if (!nctx->grank)
        fprintf(stdout, "Nexus: done local info cleanup\n");

    /* Free remote Mercury addresses */
    for (it = nctx->gaddrs.begin(); it != nctx->gaddrs.end(); it++)
        if (it->second != HG_ADDR_NULL)
            HG_Addr_free(nctx->remote_hgcl, it->second);

    /* Sync before tearing down remote endpoints */
    MPI_Barrier(MPI_COMM_WORLD);

    /* Destroy Mercury remote endpoints */
    HG_Context_destroy(nctx->remote_hgctx);
    HG_Finalize(nctx->remote_hgcl);

    if (!nctx->grank)
        fprintf(stdout, "Nexus: done remote info cleanup\n");

    free(nctx->local2global);
    free(nctx->rank2node);
    MPI_Comm_free(&nctx->repcomm);
    return NX_SUCCESS;
}
