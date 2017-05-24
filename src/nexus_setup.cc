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

#include "nexus_internal.h"

/*
 * Put together the remote Mercury endpoint address from bootstrap parameters.
 * Writes the server URI into *uri on success. Aborts on error.
 */
static void prepare_addr(int minport, int maxport, char *subnet, char *proto,
                         char *uri)
{
    struct ifaddrs *ifaddr, *cur;
    int family, ret, rank, size, port;
    char ip[16];
    MPI_Comm comm;

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
    if (rank == 0)
        fprintf(stdout, "Info: Using address %s\n", uri);
}

typedef struct hg_lookup_out {
    hg_return_t hret;
    hg_addr_t addr;
    int *cb_count;
} hg_lookup_out_t;

static hg_return_t hg_lookup_cb(const struct hg_cb_info *info)
{
    hg_lookup_out_t *out = (hg_lookup_out_t *)info->arg;
    *out->cb_count += 1;
    out->hret = info->ret;
    if (out->hret != HG_SUCCESS)
        out->addr = HG_ADDR_NULL;
    else
        out->addr = info->info.lookup.addr;
    return HG_SUCCESS;
}

static hg_return_t hg_lookup(nexus_ctx_t *nctx, hg_context_t *hgctx,
                             char *hgaddr, hg_addr_t *addr)
{
    hg_lookup_out_t *out = NULL;
    hg_class_t *hgcl;
    hg_return_t hret;
    int cb_count = 1;

    /* Init addr metadata */
    out = (hg_lookup_out_t *)malloc(sizeof(*out));
    if (out == NULL)
        return HG_NOMEM_ERROR;

    /* rank is set, perform lookup */
    out->cb_count = &cb_count;
    hret = HG_Addr_lookup(hgctx, &hg_lookup_cb, out, hgaddr, HG_OP_ID_IGNORE);
    if (hret != HG_SUCCESS)
        return hret;

    /* Lookup posted, enter the progress loop until finished */
    do {
        unsigned int count = 0;
        do {
            hret = HG_Trigger(hgctx, 0, 1, &count);
        } while (hret == HG_SUCCESS && count);

        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            return hret;

        hret = HG_Progress(hgctx, 100);
    } while (hret == HG_SUCCESS);

    if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
        return hret;

    if (out->hret != HG_SUCCESS)
        return out->hret;
    else
        *addr = out->addr;

    free(out);
    return HG_SUCCESS;
}

typedef struct {
    int pid;
    int hgid;
    int grank;
    int lrank;
} ldata_t;

static void discover_local_info(nexus_ctx_t *nctx)
{
    int ret;
    MPI_Comm localcomm;
    char hgaddr[128];
    ldata_t ldat;
    ldata_t *hginfo;
    hg_return_t hret;

#if MPI_VERSION >= 3
    ret = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
                              MPI_INFO_NULL, &localcomm);
    if (ret != MPI_SUCCESS)
        msg_abort("MPI_Comm_split_type failed");
#else
    /* XXX: Need to find a way to deal with MPI_VERSION < 3 */
    msg_abort("Nexus needs MPI version 3 or higher");
#endif

    MPI_Comm_rank(localcomm, &(nctx->localrank));
    MPI_Comm_size(localcomm, &(nctx->localsize));

    /* Initialize local Mercury listening endpoints */
    snprintf(hgaddr, sizeof(hgaddr), "na+sm://%d/0", getpid());
    fprintf(stderr, "Initializing for %s\n", hgaddr);

    nctx->local_hgcl = HG_Init(hgaddr, HG_TRUE);
    if (!nctx->local_hgcl)
        msg_abort("HG_init failed for local endpoint");

    nctx->local_hgctx = HG_Context_create(nctx->local_hgcl);
    if (!nctx->local_hgctx)
        msg_abort("HG_Context_create failed for local endpoint");

    /* Exchange PID, ID, global rank, local rank among local ranks */
    ldat.pid = getpid();
    ldat.hgid = 0;
    ldat.grank = nctx->myrank;
    ldat.lrank = nctx->localrank;

    hginfo = (ldata_t *)malloc(sizeof(ldata_t) * (nctx->localsize));
    if (!hginfo)
        msg_abort("malloc failed");

    MPI_Allgather(&ldat, sizeof(ldata_t), MPI_BYTE, hginfo,
                  sizeof(ldata_t), MPI_BYTE, localcomm);

    for (int i = 0; i < nctx->localsize; i++) {
        hg_addr_t *localaddr;

#ifdef NEXUS_DEBUG
        fprintf(stdout, "Idx %d: pid %d, id %d, grank %d, lrank %d\n",
                i, hginfo[i].pid, hginfo[i].hgid, hginfo[i].grank,
                hginfo[i].lrank);
#endif

        localaddr = (hg_addr_t *)malloc(sizeof(hg_addr_t));
        if (!localaddr)
            msg_abort("malloc failed");

        snprintf(hgaddr, sizeof(hgaddr), "na+sm://%d/%d",
                 hginfo[i].pid, hginfo[i].hgid);

        hret = hg_lookup(nctx, nctx->local_hgctx, hgaddr, localaddr);
        if (hret != HG_SUCCESS)
            msg_abort("hg_lookup failed");

        /* Add to local map */
        nctx->lcladdrs[hginfo[i].grank] = localaddr;

#ifdef NEXUS_DEBUG
        print_hg_addr(nctx->local_hgcl, hgaddr, *localaddr);
#endif
    }

    free(hginfo);
    return;
}

static void discover_remote_info(nexus_ctx_t *nctx, char *hgaddr)
{
    nctx->remote_hgcl = HG_Init(hgaddr, HG_TRUE);
    if (!nctx->remote_hgcl)
        msg_abort("HG_Init failed for remote endpoint");

    nctx->remote_hgctx = HG_Context_create(nctx->remote_hgcl);
    if (!nctx->remote_hgctx)
        msg_abort("HG_Context_create failed for remote endpoint");
}

int nexus_bootstrap(nexus_ctx_t *nctx, int minport, int maxport,
                    char *subnet, char *proto)
{
    char hgaddr[128];

    /* Grab MPI rank info */
    MPI_Comm_rank(MPI_COMM_WORLD, &(nctx->myrank));
    MPI_Comm_size(MPI_COMM_WORLD, &(nctx->ranksize));

    discover_local_info(nctx);

    prepare_addr(minport, maxport, subnet, proto, hgaddr);
    discover_remote_info(nctx, hgaddr);

#ifdef NEXUS_DEBUG
    fprintf(stdout, "[%d] Nexus bootstrap complete:\n", nctx->myrank);
    fprintf(stdout, "[%d]\tmyrank = %d\n", nctx->myrank, nctx->myrank);
    fprintf(stdout, "[%d]\tlocalrank = %d\n", nctx->myrank, nctx->localrank);
    fprintf(stdout, "[%d]\treprank = %d\n", nctx->myrank, nctx->reprank);
    fprintf(stdout, "[%d]\tranksize = %d\n", nctx->myrank, nctx->ranksize);
    fprintf(stdout, "[%d]\tlocalsize = %d\n", nctx->myrank, nctx->localsize);
#endif /* NEXUS_DEBUG */

    return 0;
}

int nexus_destroy(nexus_ctx_t *nctx)
{
    map<int, hg_addr_t *>::iterator it;

    /* Free Mercury addresses */
    for (it = nctx->lcladdrs.begin(); it != nctx->lcladdrs.end(); ++it) {
        if (*(it->second) != HG_ADDR_NULL)
            HG_Addr_free(nctx->local_hgcl, *(it->second));
        if (it->second)
            free(it->second);
    }

    /* Destroy Mercury local endpoints */
    HG_Context_destroy(nctx->local_hgctx);
    HG_Finalize(nctx->local_hgcl);

    /* Destroy Mercury remote endpoints */
    HG_Context_destroy(nctx->remote_hgctx);
    HG_Finalize(nctx->remote_hgcl);
    return 0;
}
