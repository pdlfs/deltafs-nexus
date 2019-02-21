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

#include "nexus_internal.h"

/*
 * nexus_iter encapsulates a nexus map iteration behind a C-style API
 */
struct nexus_iter {
    nexus_ctx_t nctx;                          /* context that owns iterator */
    int islocal;                               /*  is mapit for local? */
    std::map<int,hg_addr_t>::iterator mapit;   /* the C++ iterator */
};

/*
 * nexus_iter_alloc: allocate a new iterator.  nctx must remain active
 * while iter is allocated.  free iterator when done.
 */
nexus_iter_t nexus_iter(nexus_ctx_t nctx, int local) {
    nexus_iter_t nit;

    nit = new struct nexus_iter;   /* malloc */
    nit->nctx = nctx;
    nit->islocal = (local != 0);
    nit->mapit = (nit->islocal) ? nctx->lmap.begin() : nctx->rmap.begin();
    return(nit);
}

/*
 * nexus_iter_free: free a previously allocated iterator
 */
void nexus_iter_free(nexus_iter_t *nitp) {
    if (*nitp) {
        delete *nitp;
        *nitp = NULL;
    }
}

/*
 * nexus_iter_atend: return non-zero if we are at the end of the map
 */
int nexus_iter_atend(nexus_iter_t nit) {
    if (nit->islocal)
        return(nit->mapit == nit->nctx->lmap.end());
    return(nit->mapit == nit->nctx->rmap.end());
}

/*
 * nexus_iter_advance: advance the iterator
 */
void nexus_iter_advance(nexus_iter_t nit) {
    if (!nexus_iter_atend(nit))
        nit->mapit++;
}

/*
 * nexus_iter_addr: return current hgaddr of iterator
 */
hg_addr_t nexus_iter_addr(nexus_iter_t nit) {
    return(nit->mapit->second);
}

/*
 * nexus_iter_globalrank: return current global rank of iterator
 */
int nexus_iter_globalrank(nexus_iter_t nit) {
    int subrank, grank;
    subrank = nit->mapit->first;
    grank = (nit->islocal) ? subrank : nit->nctx->node2rep[subrank];
    return(grank);
}

/*
 * nexus_iter_subrank: return current subrank of iterator
 */
int nexus_iter_subrank(nexus_iter_t nit) {
    int ret;
    ret = (nit->islocal) ? 0 : nit->mapit->first;
    return(ret);
}
