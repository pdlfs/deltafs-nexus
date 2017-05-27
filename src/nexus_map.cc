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

bool nexus_is_local(nexus_ctx_t *nctx, int rank)
{
    map<int,hg_addr_t>::iterator it;

    it = nctx->lcladdrs.find(rank);
    if (it != nctx->lcladdrs.end())
        return true;

    return false;
}

bool nexus_am_rep(nexus_ctx_t *nctx)
{
    if (nctx->myrank == nctx->reprank)
        return true;

    return false;
}

int nexus_get_rep(nexus_ctx_t *nctx, int rank)
{
    if (!nexus_am_rep(nctx))
        return -1;

    if (rank < 0 || rank > nctx->ranksize)
        return -1;

    return nctx->replist[rank];
}

hg_addr_t nexus_get_addr(nexus_ctx_t *nctx, int rank)
{
    map<int,hg_addr_t>::iterator it;

    /* Prefer local addresses when possible */
    it = nctx->lcladdrs.find(rank);
    if (it != nctx->lcladdrs.end())
        return it->second;

    it = nctx->rmtaddrs.find(rank);
    if (it != nctx->rmtaddrs.end())
        return it->second;

    return HG_ADDR_NULL;
}
