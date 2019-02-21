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

#include <mercury.h>
#include <map>

typedef struct nexus_ctx* nexus_ctx_t;
typedef struct nexus_iter* nexus_iter_t;

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
 * @param string of the network subnet to be preferred for Mercury endpoints
 * @param string of the Mercury protocol plugin to be preferred
 * @return nexus context or NULL on error
 */
nexus_ctx_t nexus_bootstrap(char* subnet, char* proto);

/**
 * nexus_bootstrap_uri: bootstraps the nx library using a specific server uri
 * @param uri mercury server uri for remote forwarding
 * @return nexus context or NULL on errors
 */
nexus_ctx_t nexus_bootstrap_uri(char* uri);

/**
 * Destroys the Nexus library freeing all allocated resources
 * @param nexus context
 */
void nexus_destroy(nexus_ctx_t nctx);

/**
 * Returns next Mercury address in route to dest or error
 * @param nexus context
 * @param MPI rank of destination
 * @param MPI rank of next hop (returned)
 * @param Mercury address of next hop (returned)
 * @return NX_SUCCESS or an error code
 */
nexus_ret_t nexus_next_hop(nexus_ctx_t nctx, int dest, int* rank,
                           hg_addr_t* addr);

/**
 * Blocks until all processes in the global communicator have reached this
 * routine.
 * @param nctx context
 * @return NX_SUCCESS or an error code
 */
nexus_ret_t nexus_global_barrier(nexus_ctx_t nctx);

/**
 * Return global rank of this process (assumes nexus is up)
 * @param nctx context
 * @return global rank
 */
int nexus_global_rank(nexus_ctx_t nctx);

/**
 * Return size of the global communicator (assumes nexus is up)
 * @param nctx context
 * @return size of the global communicator
 */
int nexus_global_size(nexus_ctx_t nctx);

/**
 * Blocks until all processes in the local communicator have reached this
 * routine.
 * @param nctx context
 * @return NX_SUCCESS or an error code
 */
nexus_ret_t nexus_local_barrier(nexus_ctx_t nctx);

/**
 * Return local rank of this process (assumes nexus is up)
 * @param nctx context
 * @return local rank
 */
int nexus_local_rank(nexus_ctx_t nctx);

/**
 * Return size of the local communicator (assumes nexus is up)
 * @param nctx context
 * @return size of the global communicator
 */
int nexus_local_size(nexus_ctx_t nctx);

/**
 * Sets the global rank of the process (for debug purposes)
 * @param nexus context
 * @param new MPI rank
 * @return NX_SUCCESS or an error code
 */
nexus_ret_t nexus_set_grank(nexus_ctx_t nctx, int rank);

/**
 * Return mercury class for local (na+sm)
 *
 * @param nctx context
 */
hg_class_t* nexus_hgclass_local(nexus_ctx_t nctx);

/**
 * Return mercury class for remote (bmi+tcp or something like that)
 *
 * @param nctx context
 */
hg_class_t* nexus_hgclass_remote(nexus_ctx_t nctx);

/**
 * Return mercury context for local (bmi+tcp or something like that)
 *
 * @param nctx context
 */
hg_context_t* nexus_hgcontext_local(nexus_ctx_t nctx);

/**
 * Return mercury context for remote (bmi+tcp or something like that)
 *
 * @param nctx context
 */
hg_context_t* nexus_hgcontext_remote(nexus_ctx_t nctx);

/**
 * Allocate a new iterator.  nctx must remain active while iter is
 * allocated.  must free iterator when done.
 *
 * @param nctx context
 * @param local set non-zero if you want a local map iterator
 */
nexus_iter_t nexus_iter(nexus_ctx_t nctx, int local);

/**
 * Free a previously allocated iterator
 *
 * @param nitp pointer to iterator handle (we set to null)
 */
void nexus_iter_free(nexus_iter_t* nitp);

/**
 * Return non-zero if we are at the end of the map.
 *
 * @param nit iterator handle
 */
int nexus_iter_atend(nexus_iter_t nit);

/**
 * Advance the iterator
 *
 * @param nit iterator handle
 */
void nexus_iter_advance(nexus_iter_t nit);

/**
 * Return current hgaddr of iterator
 *
 * @param nit iterator handle
 */
hg_addr_t nexus_iter_addr(nexus_iter_t nit);

/**
 * Return current global rank of iterator
 *
 * @param nit iterator handle
 */
int nexus_iter_globalrank(nexus_iter_t nit);

/**
 * Return current subrank of iterator.  subrank is 0 for local
 * maps and node number for remote maps.
 *
 * @param nit iterator handle
 */
int nexus_iter_subrank(nexus_iter_t nit);
