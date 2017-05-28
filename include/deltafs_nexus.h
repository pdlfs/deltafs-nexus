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
using namespace std;

/*
 * Nexus library context
 */
struct nexus_ctx
{
    int grank;                  /* my global MPI rank */
    int gsize;                  /* total number of ranks */

    int lrank;                  /* my local MPI rank */
    int lsize;                  /* number of local ranks */
    int lroot;                  /* global rank of local root */

    int *localranks;            /* local -> global ranks */
    int *rankreps;              /* rank -> (remote) representative */
    map<int,hg_addr_t> laddrs;  /* map of local rank -> Hg address */
    map<int,hg_addr_t> gaddrs;  /* map of remote rank -> Hg address */

    /* MPI communicators */
    MPI_Comm localcomm;
    MPI_Comm repcomm;

    /* Mercury endpoint state */
    hg_class_t *remote_hgcl;    /* Remote Hg class */
    hg_context_t *remote_hgctx; /* Remote Hg context */
    hg_class_t *local_hgcl;     /* Local Hg class */
    hg_context_t *local_hgctx;  /* Local Hg context */
};

typedef struct nexus_ctx nexus_ctx_t;

typedef enum {
    NX_SUCCESS = 0, /* operation succeeded */
    NX_ERROR,       /* operation resulted in error */
    NX_NOTFOUND,    /* address not found */
    NX_INVAL,       /* invalid parameter */
    NX_DONE,        /* already at destination */
} nexus_ret_t;

/*
 * Bootstraps the Nexus library
 */
nexus_ret_t nexus_bootstrap(nexus_ctx_t *nctx, int minport, int maxport,
                            char *subnet, char *proto);

/*
 * Destroys the Nexus library freeing all allocated resources
 */
nexus_ret_t nexus_destroy(nexus_ctx_t *nctx);

#if 0
/*
 * Returns true if the rank is local to the caller
 */
bool nexus_is_local(nexus_ctx_t *nctx, int rank);
#endif

/*
 * Returns next Mercury address in route to dest or error
 */
nexus_ret_t nexus_next_hop(nexus_ctx_t *nctx, int dest, hg_addr_t *addr);
