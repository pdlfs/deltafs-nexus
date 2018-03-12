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
  hg_context_t* hg_ctx;
  hg_class_t* hg_cl;
  int refs;
} nexus_hg_t;

/*
 * nexus_ctx: nexus internal state
 */
struct nexus_ctx {
  int grank;   /* my global rank */
  int gsize;   /* total number of ranks */
  int gaddrsz; /* max string size needed for global address */

  int nodeid; /* global id of node */
  int nodesz; /* total number of nodes */

  int lrank;   /* my local rank */
  int lsize;   /* number of local ranks */
  int lroot;   /* global rank of local root */
  int laddrsz; /* max string size needed for local address */

  int* local2global; /* local rank -> its global rank */
  int* rank2node;    /* rank -> its node id */
  int* node2rep;     /* node -> its rep's global rank */

  nexus_map_t laddrs; /* global rank -> local peer's local addresses */
  nexus_map_t gaddrs; /* remote node -> rep's remote addresses */

  MPI_Comm localcomm;
  MPI_Comm repcomm;

  nexus_hg_t* hg_remote;
  nexus_hg_t* hg_local;
};

/*
 * nx_fatal: abort with a message
 */
static inline void nx_fatal(const char* msg) {
  if (errno != 0) {
    fprintf(stderr, "NX FATAL: %s (%s)\n", msg, strerror(errno));
  } else {
    fprintf(stderr, "NX FATAL: %s\n", msg);
  }

  abort();
}

/*
 * nx_is_envset: check if an env is set
 */
static bool nx_is_envset(const char* name) {
  char* env = getenv(name);
  if (!env || !env[0]) return false;
  return strcmp(env, "0") != 0;
}

static void nx_init_localcomm(nexus_ctx_t nctx) {
#if MPI_VERSION >= 3
  int ret = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
                                MPI_INFO_NULL, &nctx->localcomm);
  if (ret != MPI_SUCCESS) {
    nx_fatal("MPI_Comm_split_type");
  }
#else
  msg_abort("MPI-3 required");
#endif
}

static void nx_init_repcomm(nexus_ctx_t nctx) {
#if MPI_VERSION >= 3
  int ret = MPI_Comm_split(MPI_COMM_WORLD, (nctx->grank == nctx->lroot),
                           nctx->grank, &nctx->repcomm);
  if (ret != MPI_SUCCESS) {
    nx_fatal("MPI_Comm_split_type");
  }
#else
  msg_abort("MPI-3 required");
#endif
}
