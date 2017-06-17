/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <map>
#include <mercury.h>

typedef std::map<int,hg_addr_t> nexus_map_t;

/*
 * Nexus library context
 */
struct nexus_ctx
{
    int grank;                  /* my global MPI rank */
    int gsize;                  /* total number of ranks */

    int nodeid;                 /* global ID of node (in repconn) */
    int nodesz;                 /* total number of nodes */

    int lrank;                  /* my local MPI rank */
    int lsize;                  /* number of local ranks */
    int lroot;                  /* global rank of local root */

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

typedef struct nexus_ctx nexus_ctx_t;

/* error codes */
typedef enum {
    NX_SUCCESS = 0, /* operation succeeded */
    NX_ERROR,       /* operation resulted in error */
    NX_NOTFOUND,    /* address not found */
    NX_ISLOCAL,     /* dest is local */
    NX_SRCREP,      /* dest is srcrep */
    NX_DESTREP,     /* dest is dstrep */
    NX_INVAL,       /* invalid parameter */
    NX_DONE,        /* already at destination */
} nexus_ret_t;

/**
 * nexus_bootstrap: bootstraps the Nexus library
 * @param nexus context (to be initialized)
 * @param minimum port for initialized Mercury endpoints
 * @param maximum port for initialized Mercury endpoints
 * @param string of the network subnet to be preferred for Mercury endpoints
 * @param string of the Mercury protocol plugin to be preferred
 * @return NX_SUCCESS or an error code
 */
nexus_ret_t nexus_bootstrap(nexus_ctx_t *nctx, int minport, int maxport,
                            char *subnet, char *proto);

/**
 * Destroys the Nexus library freeing all allocated resources
 * @param nexus context
 * @return NX_SUCCESS or an error code
 */
nexus_ret_t nexus_destroy(nexus_ctx_t *nctx);

/**
 * Returns next Mercury address in route to dest or error
 * @param nexus context
 * @param MPI rank of destination
 * @param MPI rank of next hop (returned iff address is not NULL)
 * @param Mercury address of next hop (returned)
 * @return NX_SUCCESS or an error code
 */
nexus_ret_t nexus_next_hop(nexus_ctx_t *nctx, int dest,
                           int *rank, hg_addr_t *addr);
