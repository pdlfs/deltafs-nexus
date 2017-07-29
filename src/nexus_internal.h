/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <cstdio>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <mpi.h>
#include <string.h>

#include "deltafs-nexus_api.h"

//#define NEXUS_DEBUG

typedef std::map<int,hg_addr_t> nexus_map_t;

/*
 * Nexus library context
 */
struct nexus_ctx
{
    int grank;                  /* my global MPI rank */
    int gsize;                  /* total number of ranks */
    int gaddrsz;                /* Max size of global Hg address */

    int nodeid;                 /* global ID of node (in repconn) */
    int nodesz;                 /* total number of nodes */

    int lrank;                  /* my local MPI rank */
    int lsize;                  /* number of local ranks */
    int lroot;                  /* global rank of local root */
    int laddrsz;                /* Max size of local Hg address */

    int *local2global;          /* local rank -> global rank */
    int *rank2node;             /* rank -> node ID */
    int *node2rep;              /* node -> rep global rank */

    nexus_map_t laddrs;         /* local rank -> Hg address */
    nexus_map_t gaddrs;         /* remote node -> Hg address of our rep */

    /* MPI communicators */
    MPI_Comm localcomm;
    MPI_Comm repcomm;

    /* Mercury state */
    hg_class_t *remote_hgcl;    /* Remote Hg class */
    hg_context_t *remote_hgctx; /* Remote Hg context */
    hg_class_t *local_hgcl;     /* Local Hg class */
    hg_context_t *local_hgctx;  /* Local Hg context */
};

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

static void print_addrs(nexus_ctx_t nctx, hg_class_t *hgcl, nexus_map_t map)
{
    nexus_map_t::iterator it;
    char *addr_str = NULL;
    hg_size_t addr_size = 0;
    hg_return_t hret;

    for (it = map.begin(); it != map.end(); it++) {
        hret = HG_Addr_to_string(hgcl, NULL, &addr_size, it->second);
        if (hret != HG_SUCCESS)
            msg_abort("HG_Addr_to_string failed");

        addr_str = (char *)malloc(addr_size);
        if (addr_str == NULL)
            msg_abort("malloc failed");

        hret = HG_Addr_to_string(hgcl, addr_str, &addr_size, it->second);
        if (hret != HG_SUCCESS)
            msg_abort("HG_Addr_to_string failed");

        fprintf(stdout, "[%d] Mercury addr for rank %d: %s\n",
                nctx->grank, it->first, addr_str);
        free(addr_str);
    }
}

static void init_local_comm(nexus_ctx_t nctx)
{
    int ret;

#if MPI_VERSION >= 3
    ret = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
                              MPI_INFO_NULL, &nctx->localcomm);
    if (ret != MPI_SUCCESS)
        msg_abort("MPI_Comm_split_type failed");
#else
    /* XXX: Need to find a way to deal with MPI_VERSION < 3 */
    msg_abort("Nexus needs MPI version 3 or higher");
#endif
}

static void init_rep_comm(nexus_ctx_t nctx)
{
    int ret;

#if MPI_VERSION >= 3
    ret = MPI_Comm_split(MPI_COMM_WORLD, (nctx->grank == nctx->lroot),
                         nctx->grank, &nctx->repcomm);
    if (ret != MPI_SUCCESS)
        msg_abort("MPI_Comm_split_type failed");
#else
    /* XXX: Need to find a way to deal with MPI_VERSION < 3 */
    msg_abort("Nexus needs MPI version 3 or higher");
#endif
}
