/*
 * Copyright (c) 2017-2018, Carnegie Mellon University.
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
#pragma once

#include <errno.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "deltafs-nexus_api.h"

#define NEXUS_LOOKUP_LIMIT 64
#define NEXUS_DEBUG

typedef std::map<int, hg_addr_t> nexus_map_t;

/* nexus_hg_state: state for an opened mercury instance */
typedef struct nexus_hg_state {
  hg_class_t* hg_cl;
  hg_context_t* hg_ctx;
  int refs;
} nexus_hg_t;

/*
 * Nexus library context
 */
struct nexus_ctx {
  int grank;   /* my global MPI rank */
  int gsize;   /* total number of ranks */
  int gaddrsz; /* Max size of global Hg address */

  int nodeid; /* global ID of node (in repconn) */
  int nodesz; /* total number of nodes */

  int lrank;   /* my local MPI rank */
  int lsize;   /* number of local ranks */
  int lroot;   /* global rank of local root */
  int laddrsz; /* Max size of local Hg address */

  int* local2global; /* local rank -> global rank */
  int* rank2node;    /* rank -> node ID */
  int* node2rep;     /* node -> rep global rank */

  nexus_map_t laddrs; /* global rank -> Hg address */
  nexus_map_t gaddrs; /* remote node -> Hg address of our rep */

  MPI_Comm localcomm;
  MPI_Comm repcomm;

  nexus_hg_t* hg_remote;
  nexus_hg_t* hg_local;
};

/*
 * msg_abort: abort with a message
 */
static inline void msg_abort(const char* msg) {
  if (errno != 0) {
    fprintf(stderr, "Error: %s (%s)\n", msg, strerror(errno));
  } else {
    fprintf(stderr, "Error: %s\n", msg);
  }

  abort();
}

static void print_addrs(nexus_ctx_t nctx, hg_class_t* hgcl, nexus_map_t map) {
  nexus_map_t::iterator it;
  char* addr_str = NULL;
  hg_size_t addr_size = 0;
  hg_return_t hret;

  for (it = map.begin(); it != map.end(); it++) {
    hret = HG_Addr_to_string(hgcl, NULL, &addr_size, it->second);
    if (hret != HG_SUCCESS) msg_abort("HG_Addr_to_string failed");

    addr_str = (char*)malloc(addr_size);
    if (addr_str == NULL) msg_abort("malloc failed");

    hret = HG_Addr_to_string(hgcl, addr_str, &addr_size, it->second);
    if (hret != HG_SUCCESS) msg_abort("HG_Addr_to_string failed");

    fprintf(stderr, "[%d] Mercury addr for rank %d: %s\n", nctx->grank,
            it->first, addr_str);
    free(addr_str);
  }
}

static void init_local_comm(nexus_ctx_t nctx) {
  int ret;

#if MPI_VERSION >= 3
  ret = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
                            MPI_INFO_NULL, &nctx->localcomm);
  if (ret != MPI_SUCCESS) msg_abort("MPI_Comm_split_type failed");
#else
  /* XXX: Need to find a way to deal with MPI_VERSION < 3 */
  msg_abort("Nexus needs MPI version 3 or higher");
#endif
}

static void init_rep_comm(nexus_ctx_t nctx) {
  int ret;

#if MPI_VERSION >= 3
  ret = MPI_Comm_split(MPI_COMM_WORLD, (nctx->grank == nctx->lroot),
                       nctx->grank, &nctx->repcomm);
  if (ret != MPI_SUCCESS) msg_abort("MPI_Comm_split_type failed");
#else
  /* XXX: Need to find a way to deal with MPI_VERSION < 3 */
  msg_abort("Nexus needs MPI version 3 or higher");
#endif
}
