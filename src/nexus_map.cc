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

#if 0
bool nexus_is_local(nexus_ctx_t *nctx, int rank)
{
    map<int,hg_addr_t>::iterator it;

    it = nctx->laddrs.find(rank);
    if (it != nctx->laddrs.end())
        return true;

    return false;
}
#endif

static hg_addr_t nexus_get_addr(nexus_ctx_t *nctx, int rank)
{
    map<int,hg_addr_t>::iterator it;

    /* Prefer local addresses when possible */
    it = nctx->laddrs.find(rank);
    if (it != nctx->laddrs.end())
        return it->second;

    it = nctx->gaddrs.find(rank);
    if (it != nctx->gaddrs.end())
        return it->second;

    return HG_ADDR_NULL;
}

nexus_ret_t nexus_next_hop(nexus_ctx_t *nctx, int dest, hg_addr_t *addr)
{
    int srcrep, destrep;
    map<int,hg_addr_t>::iterator it;

    /* If we are the dest, stop here */
    if (nctx->grank == dest)
        return NX_DONE;

    /* Find src and dest representatives */
    srcrep = nctx->localranks[(dest % nctx->lsize)];
    destrep = nctx->rankreps[dest];

    /* If we are neither the srcrep or destrep, we are the src */
    if ((nctx->grank != srcrep) &&
        (nctx->grank != destrep)) {
        /* Find the srcrep address and return it */
        *addr = nexus_get_addr(nctx, srcrep);
        if (*addr == HG_ADDR_NULL)
            return NX_NOTFOUND;
        return NX_SUCCESS;
    }

    /* If we are the srcrep, find destrep address and return it */
    if (nctx->grank == srcrep) {
        *addr = nexus_get_addr(nctx, destrep);
        if (*addr == HG_ADDR_NULL)
            return NX_NOTFOUND;
        return NX_SUCCESS;
    }

    return NX_INVAL;
}
