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

#define TMPADDRSZ   32

typedef struct {
    int grank;
    int idx;
    char addr[];
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
 * get_ipmatch: look at our interfaces and find an IP address that
 * matches our spec...  aborts on failure.
 */
static void get_ipmatch(char *subnet, char *i, int ilen) {
    struct ifaddrs *ifaddr, *cur;
    int family;

    /* Query local socket layer to get our IP addr */
    if (getifaddrs(&ifaddr) == -1)
        msg_abort("getifaddrs failed");

    for (cur = ifaddr; cur != NULL; cur = cur->ifa_next) {
        if (cur->ifa_addr != NULL) {
            family = cur->ifa_addr->sa_family;

            if (family == AF_INET) {
                if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in), i,
                                ilen, NULL, 0, NI_NUMERICHOST) == -1)
                    msg_abort("getnameinfo failed");

                if (strncmp(subnet, i, strlen(subnet)) == 0)
                    break;
            }
        }
    }

    if (cur == NULL)
        msg_abort("no ip addr");

    freeifaddrs(ifaddr);
}

/*
 * Put together the remote Mercury endpoint address from bootstrap parameters.
 * Writes the server URI into *uri on success. Aborts on error.
 */
static void prepare_addr(nexus_ctx_t nctx, char *subnet,
                         char *proto, char *uri) {
    char ip[16];
    int lcv, port, so, n;
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);

    get_ipmatch(subnet, ip, sizeof(ip));  /* fill ip w/req'd local IP */

    if (strcmp(proto, "bmi+tcp") == 0) {
        /*
         * XXX: bmi+tcp HG_Addr_to_string() is broken.  if we request
         * port 0 (to let OS fill in) and later use HG_Addr_to_string()
         * to request the actual port number allocated, it still returns
         * 0 as the port number...   here's an attempt to hack around
         * this bug.
         */
        so = socket(PF_INET, SOCK_STREAM, 0);
        if (so < 0) msg_abort("bmi:socket1");
        n = 1;
        setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n));
        for (lcv = 0; lcv < 1024; lcv++) {
            /* have each local rank try a different set of ports */
            port = 10000 + (lcv * nctx->lsize) + nctx->lrank;
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(port);
            n = bind(so, (struct sockaddr *)&addr, sizeof(addr));
            if (n == 0) break;
        }
        close(so);
        if (n != 0) msg_abort("bmi:socket2");

        sprintf(uri, "%s://%s:%d", proto, ip, port);

    } else {

        /* set port 0, let OS fill it, collect later w/HG_Addr_to_string */
        sprintf(uri, "%s://%s:0", proto, ip);

    }

#ifdef NEXUS_DEBUG
    fprintf(stdout, "Info: Using address %s\n", uri);
#endif
}

struct hg_lookup_ctx {
    /* The address map we'll be updating */
    nexus_map_t *map;

    /* State to track progress */
    int count;
    pthread_mutex_t cb_mutex;
    pthread_cond_t cb_cv;
} lkp;

typedef struct hg_lookup_out {
    hg_return_t hret;
    int idx;
} hg_lookup_out_t;

static hg_return_t hg_lookup_cb(const struct hg_cb_info *info)
{
    hg_lookup_out_t *out = (hg_lookup_out_t *)info->arg;
    out->hret = info->ret;

    pthread_mutex_lock(&lkp.cb_mutex);

    /* Add address to map */
    if (out->hret != HG_SUCCESS)
        (*(lkp.map))[out->idx] = HG_ADDR_NULL;
    else
        (*(lkp.map))[out->idx] = info->info.lookup.addr;

    lkp.count += 1;
    pthread_cond_signal(&lkp.cb_cv);
    pthread_mutex_unlock(&lkp.cb_mutex);

    return HG_SUCCESS;
}

static hg_return_t lookup_addrs(nexus_ctx_t nctx,
                                hg_context_t *hgctx, hg_class_t *hgcl,
                                xchg_dat_t *xarr, int xsize, int addrsz,
                                nexus_map_t *map)
{
    hg_lookup_out_t *out = NULL;
    hg_return_t hret;
    hg_addr_t self_addr;
    int cur_i, eff_size;

    lkp.count = 0;
    lkp.map = map;

    out = (hg_lookup_out_t *)malloc(sizeof(*out) * xsize);
    if (out == NULL)
        return HG_NOMEM_ERROR;

    pthread_mutex_init(&lkp.cb_mutex, NULL);
    pthread_cond_init(&lkp.cb_cv, NULL);

    pthread_mutex_lock(&lkp.cb_mutex);

    cur_i = 0;
send_again:
    eff_size = (((xsize - cur_i) < NEXUS_LOOKUP_LIMIT) ?
                 (xsize - cur_i) : NEXUS_LOOKUP_LIMIT);
    /* Post a batch of lookups */
    for (int i = 0; i < eff_size; i++) {
        int eff_i = ((nctx->grank + i) % eff_size) + cur_i;
        xchg_dat_t *xi = (xchg_dat_t *)(((char *)xarr) +
                         eff_i * (sizeof(*xi) + addrsz));

        /* Populate out struct */
        out[eff_i].hret = HG_SUCCESS;
        out[eff_i].idx = xi->idx;

#ifdef NEXUS_DEBUG
        fprintf(stderr, "[%d] Idx %d: addr %s, idx %d, grank %d (xsize = %d)\n",
                nctx->grank, eff_i, xi->addr, xi->idx, xi->grank, xsize);
#endif

        if (xi->grank != nctx->grank) {
            hret = HG_Addr_lookup(hgctx, &hg_lookup_cb, &out[eff_i],
                                  xi->addr, HG_OP_ID_IGNORE);
        } else {
            hret = HG_Addr_self(hgcl, &self_addr);

            /* Add address to map */
            if (hret != HG_SUCCESS)
                (*(lkp.map))[xi->idx] = HG_ADDR_NULL;
            else
                (*(lkp.map))[xi->idx] = self_addr;

            lkp.count += 1;
        }

        if (hret != HG_SUCCESS) {
            pthread_mutex_unlock(&lkp.cb_mutex);
            goto err;
        }
    }

    cur_i += eff_size;

    /* Lookup posted, wait until finished */
wait_again:
    if (lkp.count < cur_i) {
        pthread_cond_wait(&lkp.cb_cv, &lkp.cb_mutex);
        if (lkp.count < cur_i)
            goto wait_again;
    }
    pthread_mutex_unlock(&lkp.cb_mutex);

    if (cur_i < xsize)
        goto send_again;

    hret = HG_SUCCESS;
    for (int i = 0; i < xsize; i++) {
        if (out[i].hret != HG_SUCCESS) {
            hret = out[i].hret;
            goto err;
        }
    }

err:
    pthread_cond_destroy(&lkp.cb_cv);
    pthread_mutex_destroy(&lkp.cb_mutex);
    free(out);
    return hret;
}

static void discover_local_info(nexus_ctx_t nctx)
{
    char *nlocal;
    int ret, len;
    char hgaddr[TMPADDRSZ];
    xchg_dat_t *xitm, *xarr;
    hg_return_t hret;
    pthread_t bgthread; /* network background thread */
    bgthread_dat_t bgarg;

    MPI_Comm_rank(nctx->localcomm, &(nctx->lrank));
    MPI_Comm_size(nctx->localcomm, &(nctx->lsize));

    /* Initialize local Mercury listening endpoints */
    nlocal = getenv("NEXUS_ALT_LOCAL");
    if (nlocal) {
        snprintf(hgaddr, sizeof(hgaddr), "%s://127.0.0.1:%d", nlocal,
        19000+ nctx->lrank);
        fprintf(stderr, "Initializing for %s\n", hgaddr);
    } else {
        snprintf(hgaddr, sizeof(hgaddr), "na+sm://%d/0", getpid());
    }
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
    bgarg.hgctx = nctx->local_hgctx;
    bgarg.bgdone = 0;

    ret = pthread_create(&bgthread, NULL, nexus_bgthread, (void*)&bgarg);
    if (ret != 0)
        msg_abort("pthread_create failed");

    /* Find max address size in local comm */
    len = strnlen(hgaddr, sizeof(hgaddr)) + 1;
    MPI_Allreduce(&len, &nctx->laddrsz, 1, MPI_INT, MPI_MAX, nctx->localcomm);
    len = sizeof(xchg_dat_t) + nctx->laddrsz;

    /* Exchange addresses, global, and local ranks in local comm */
    xitm = (xchg_dat_t *)malloc(len);
    if (!xitm)
        msg_abort("malloc failed");

    /* For local2global our index is lrank */
    xitm->grank = nctx->grank;
    xitm->idx = nctx->lrank;
    strncpy(xitm->addr, hgaddr, nctx->laddrsz);

    xarr = (xchg_dat_t *)malloc(nctx->lsize * len);
    if (!xarr)
        msg_abort("malloc failed");

    nctx->local2global = (int *)malloc(sizeof(int) * nctx->lsize);
    if (!nctx->local2global)
        msg_abort("malloc failed");

    MPI_Allgather(xitm, len, MPI_BYTE, xarr, len, MPI_BYTE, nctx->localcomm);
    free(xitm);

    /* Build map of local to global ranks, find local root */
    for (int i = 0; i < nctx->lsize; i++) {
        xchg_dat_t *xi = (xchg_dat_t *)(((char *)xarr) + i * len);

        nctx->local2global[xi->idx] = xi->grank;
        if (xi->idx == 0)
            nctx->lroot = xi->grank;

        /* For lookups our index is the grank, so switch */
        xi->idx = xi->grank;
    }

    /* Look up local Mercury addresses */
    if (lookup_addrs(nctx, nctx->local_hgctx, nctx->local_hgcl, xarr,
                     nctx->lsize, nctx->laddrsz, &nctx->laddrs) != HG_SUCCESS)
        msg_abort("lookup_addrs failed");

#ifdef NEXUS_DEBUG
    print_addrs(nctx, nctx->local_hgcl, nctx->laddrs);
#endif

    /* Sync before terminating background threads */
    MPI_Barrier(nctx->localcomm);

    /* Terminate network thread */
    bgarg.bgdone = 1;
    pthread_join(bgthread, NULL);
    free(xarr);
}

#define NADDR(x, y, s) (&x[y * s])

static void find_remote_addrs(nexus_ctx_t nctx, char *myaddr)
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
    i = strlen(myaddr) + 1;
    MPI_Allreduce(&i, &nctx->gaddrsz, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    rank2addr = (char *)malloc(nctx->gsize * nctx->gaddrsz);
    if (!rank2addr)
        msg_abort("malloc failed");

    MPI_Allgather(myaddr, nctx->gaddrsz, MPI_BYTE,
                  rank2addr, nctx->gaddrsz, MPI_BYTE, MPI_COMM_WORLD);
#ifdef NEXUS_DEBUG
    for (i = 0; i < nctx->gsize; i++)
        fprintf(stderr, "[%d] rank2addr[%d] = %s\n",
                nctx->grank, i, NADDR(rank2addr, i, nctx->gaddrsz));
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

    i = 1;
    msgdata[0] = nctx->lsize;
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
        fprintf(stderr, "[%d] nodelists[%d] = %d\n",
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

    paddrs = (xchg_dat_t *)malloc(maxpeers * (sizeof(*paddrs) + nctx->gaddrsz));
    if (!paddrs)
        msg_abort("malloc failed");

    /* Keep info on global ranks of remote reps in node2rep */
    nctx->node2rep = (int *)malloc(sizeof(int) * nctx->nodesz);
    if (!nctx->node2rep)
        msg_abort("malloc failed");

    for (i = 0; i < nctx->nodesz; i++) {
        int idx = i * (maxnodesz + 1);
        int remote_lsize = nodelists[idx];
        nctx->node2rep[i] = nodelists[idx + 1 + (nctx->nodeid % remote_lsize)];
    }

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
        xchg_dat_t *ppeer;

        /* Skip ourselves */
        if (i == nctx->nodeid) {
            i += nctx->lsize;
            continue;
        }

        remote_lsize = nodelists[idx];
        remote_grank = nodelists[idx + 1 + (nctx->nodeid % remote_lsize)];
        remote_addr = NADDR(rank2addr, remote_grank, nctx->gaddrsz);

        ppeer = (xchg_dat_t *)(((char *)paddrs) + npeers *
                                (sizeof(*ppeer) + nctx->gaddrsz));
        strncpy(ppeer->addr, remote_addr, nctx->gaddrsz);
        ppeer->idx = i; /* we index by node ID */
        ppeer->grank = remote_grank;

        i += nctx->lsize;
        npeers++;
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
    for (i = 0; i < npeers; i++) {
        xchg_dat_t *ppeer = (xchg_dat_t *)(((char *)paddrs) + i *
                                            (sizeof(*ppeer) + nctx->gaddrsz));

        fprintf(stderr, "[%d] i = %d, idx = %d, grank = %d, addr = %s\n",
                nctx->grank, i, ppeer->idx, ppeer->grank, ppeer->addr);
    }
#endif

    /* Step 2: lookup peer addresses */
    if (lookup_addrs(nctx, nctx->remote_hgctx, nctx->remote_hgcl, paddrs,
                     npeers, nctx->gaddrsz, &nctx->gaddrs) != HG_SUCCESS)
        msg_abort("lookup_addrs failed");

#ifdef NEXUS_DEBUG
    print_addrs(nctx, nctx->remote_hgcl, nctx->gaddrs);
    fprintf(stderr, "[%d] printed gaddrs, npeers = %d\n", nctx->grank, npeers);
#endif

    free(paddrs);
}

static void discover_remote_info(nexus_ctx_t nctx, char *hgaddr_in)
{
    hg_addr_t self;
    int ret;
    pthread_t bgthread; /* network background thread */
    bgthread_dat_t bgarg;
    char hgaddr_out[TMPADDRSZ]; // resolved HG server uri
    hg_size_t outsz;

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
    nctx->remote_hgcl = HG_Init(hgaddr_in, HG_TRUE);
    if (!nctx->remote_hgcl)
        msg_abort("HG_Init failed for remote endpoint");

    nctx->remote_hgctx = HG_Context_create(nctx->remote_hgcl);
    if (!nctx->remote_hgctx)
        msg_abort("HG_Context_create failed for remote endpoint");

    /* Start the network thread */
    bgarg.hgctx = nctx->remote_hgctx;
    bgarg.bgdone = 0;

    ret = pthread_create(&bgthread, NULL, nexus_bgthread, (void*)&bgarg);
    if (ret != 0)
        msg_abort("pthread_create failed");

    /* convert input addr spec to final to get port number used */
    if (HG_Addr_self(nctx->remote_hgcl, &self) != HG_SUCCESS)
        msg_abort("HG_Addr_self failed?");
    outsz = sizeof(hgaddr_out);
    if (HG_Addr_to_string(nctx->remote_hgcl, hgaddr_out, &outsz,
                          self) != HG_SUCCESS)
        msg_abort("HG_Addr_to_string on self failed?");
    HG_Addr_free(nctx->remote_hgcl, self);

    find_remote_addrs(nctx, hgaddr_out);

    /* Sync before terminating background threads */
    MPI_Barrier(MPI_COMM_WORLD);

    /* Terminate network thread */
    bgarg.bgdone = 1;
    pthread_join(bgthread, NULL);
}

nexus_ctx_t nexus_bootstrap_uri(char *uri)
{
    nexus_ctx_t nctx = NULL;

    /* Allocate context */
    nctx = new nexus_ctx;
    if (!nctx)
        return NULL;

    /* Grab MPI rank info */
    MPI_Comm_rank(MPI_COMM_WORLD, &(nctx->grank));
    MPI_Comm_size(MPI_COMM_WORLD, &(nctx->gsize));

    if (!nctx->grank)
        fprintf(stdout, "<nexus>: started bootstrap\n");

    init_local_comm(nctx);
    discover_local_info(nctx);

    if (!nctx->grank)
        fprintf(stdout, "<nexus>: done local info discovery\n");

    init_rep_comm(nctx);
    discover_remote_info(nctx, uri);

    if (!nctx->grank)
        fprintf(stdout, "<nexus>: done remote info discovery\n");

#ifdef NEXUS_DEBUG
    fprintf(stdout, "[%d] grank = %d, lrank = %d, gsize = %d, lsize = %d\n",
            nctx->grank, nctx->grank, nctx->lrank, nctx->gsize, nctx->lsize);
#endif /* NEXUS_DEBUG */

    return nctx;
}

nexus_ctx_t nexus_bootstrap(char *subnet, char *proto)
{
    nexus_ctx_t nctx = NULL;
    char hgaddr[TMPADDRSZ]; // HG server uri

    /* Allocate context */
    nctx = new nexus_ctx;
    if (!nctx)
        return NULL;

    /* Grab MPI rank info */
    MPI_Comm_rank(MPI_COMM_WORLD, &(nctx->grank));
    MPI_Comm_size(MPI_COMM_WORLD, &(nctx->gsize));

    if (!nctx->grank)
        fprintf(stdout, "<nexus>: started bootstrap\n");

    init_local_comm(nctx);
    discover_local_info(nctx);

    if (!nctx->grank)
        fprintf(stdout, "<nexus>: done local info discovery\n");

    prepare_addr(nctx, subnet, proto, hgaddr);
    init_rep_comm(nctx);
    discover_remote_info(nctx, hgaddr);

    if (!nctx->grank)
        fprintf(stdout, "<nexus>: done remote info discovery\n");

#ifdef NEXUS_DEBUG
    fprintf(stdout, "[%d] grank = %d, lrank = %d, gsize = %d, lsize = %d\n",
            nctx->grank, nctx->grank, nctx->lrank, nctx->gsize, nctx->lsize);
#endif /* NEXUS_DEBUG */

    return nctx;
}

void nexus_destroy(nexus_ctx_t nctx)
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
        fprintf(stdout, "<nexus>: done local info cleanup\n");

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
        fprintf(stdout, "<nexus>: done remote info cleanup\n");

    if (nctx->nodesz > 1) free(nctx->node2rep);
    free(nctx->local2global);
    free(nctx->rank2node);
    MPI_Comm_free(&nctx->repcomm);
    delete nctx;
}

hg_class_t *nexus_hgclass_local(nexus_ctx_t nctx)
{
    return(nctx->local_hgcl);
}

hg_class_t *nexus_hgclass_remote(nexus_ctx_t nctx)
{
    return(nctx->remote_hgcl);
}

hg_context_t *nexus_hgcontext_local(nexus_ctx_t nctx)
{
    return(nctx->local_hgctx);
}

hg_context_t *nexus_hgcontext_remote(nexus_ctx_t nctx)
{
    return(nctx->remote_hgctx);
}
