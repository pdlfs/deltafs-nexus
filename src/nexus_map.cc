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

#include "nexus_internal.h"

static bool nexus_is_local(nexus_ctx_t nctx, int rank, hg_addr_t *addr)
{
    nexus_map_t::iterator it;

    it = nctx->laddrs.find(rank);
    if (it != nctx->laddrs.end()) {
        *addr = it->second;
        return true;
    }

    return false;
}

static hg_addr_t nexus_get_addr(nexus_map_t map, int key)
{
    nexus_map_t::iterator it;

    it = map.find(key);
    if (it != map.end())
        return it->second;

    return HG_ADDR_NULL;
}

nexus_ret_t nexus_next_hop(nexus_ctx_t nctx, int dest,
                           int *rank, hg_addr_t *addr)
{
    int srcrep, destrep;
    int ndest;
    nexus_map_t::iterator it;

    /* If we are the dest, stop here */
    if (nctx->grank == dest)
        return NX_DONE;

    /* If dest is local, return its address */
    if (nexus_is_local(nctx, dest, addr)) {
        if (rank) *rank = dest;
        return NX_ISLOCAL;
    }

    /*
     * To find src rep get node ID for destination, modulo with
     * lsize to get lrank, and then convert local to global rank
     */
    ndest = nctx->rank2node[dest];
    srcrep = nctx->local2global[(ndest % nctx->lsize)];
    /*
     * To find dest rep get node ID for destination, and look it
     * up in node2rep
     */
    destrep = nctx->node2rep[ndest];
#ifdef NEXUS_DEBUG
    fprintf(stdout, "[%d] nexus_next_hop: ndest=%d, srcrep=%d, destrep=%d\n",
            nctx->grank, ndest, srcrep, destrep);
#endif

    /* Are we the src or the srcrep? */
    if (nctx->grank != srcrep) {
        /* We are the src. Find srcrep address and return it */
        *addr = nexus_get_addr(nctx->laddrs, srcrep);
        if (*addr == HG_ADDR_NULL) return NX_NOTFOUND;
        if (rank) *rank = srcrep;

        return NX_SRCREP;
    } else {
        /* We are the srcrep. Find destrep address and return it */
        *addr = nexus_get_addr(nctx->gaddrs, destrep);
        if (*addr == HG_ADDR_NULL) return NX_NOTFOUND;
        if (rank) *rank = destrep;

        return NX_DESTREP;
    }

    return NX_INVAL;
}

nexus_ret_t nexus_set_grank(nexus_ctx_t nctx, int rank)
{
    nctx->grank = rank;
    return NX_SUCCESS;
}

int nexus_global_rank(nexus_ctx_t nctx)
{
    return nctx->grank;
}
