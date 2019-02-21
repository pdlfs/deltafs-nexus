/*
 * Copyright (c) 2017-2019, Carnegie Mellon University and
 *     Los Alamos National Laboratory.
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
  int nnodes; /* total number of nodes */

  int lrank;   /* my local rank */
  int lsize;   /* number of local ranks */
  int lroot;   /* global rank of local root */
  int laddrsz; /* max string size needed for local address */

  int* local2global; /* local rank -> the local peer's global rank */
  int* rank2node;    /* global rank -> its node id */
  int* node2rep;     /* node -> its rep's global rank */

  nexus_map_t lmap; /* local peer's global rank -> that peer's local address */
  nexus_map_t rmap; /* remote node -> its rep's remote address */

  MPI_Comm localcomm;
  MPI_Comm repcomm;

  nexus_hg_t* hg_remote;
  nexus_hg_t* hg_local;

  /* max pending hg addr lookup requests */
  int nx_limit;
};
