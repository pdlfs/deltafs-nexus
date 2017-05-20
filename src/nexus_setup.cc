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

static void discover_local_cores(nexus_ctx_t *nctx)
{
    int ret;
    MPI_Comm localcomm;

#if MPI_VERSION >= 3
    ret = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
                              MPI_INFO_NULL, &localcomm);
    if (ret != MPI_SUCCESS)
        msg_abort("MPI_Comm_split_type failed");
#else
    /* XXX: Need to find a way to deal with MPI_VERSION < 3 */
    msg_abort("Nexus needs MPI version 3 or higher");
#endif

    MPI_Comm_rank(localcomm, &(nctx->localrank));
    MPI_Comm_size(localcomm, &(nctx->localsize));

    /* TODO: Initialize local Mercury listening endpoint */
}

int nexus_bootstrap(nexus_ctx_t *nctx, hg_class_t *hgcl, hg_context_t *hgctx)
{
    /* Grab MPI rank info */
    MPI_Comm_rank(MPI_COMM_WORLD, &(nctx->myrank));
    MPI_Comm_size(MPI_COMM_WORLD, &(nctx->ranksize));

    discover_local_cores(nctx);

#ifdef NEXUS_DEBUG
    fprintf(stdout, "[%d] Nexus bootstrap complete:\n", nctx->myrank);
    fprintf(stdout, "[%d]\tmyrank = %d\n", nctx->myrank, nctx->myrank);
    fprintf(stdout, "[%d]\tlocalrank = %d\n", nctx->myrank, nctx->localrank);
    fprintf(stdout, "[%d]\treprank = %d\n", nctx->myrank, nctx->reprank);
    fprintf(stdout, "[%d]\tranksize = %d\n", nctx->myrank, nctx->ranksize);
    fprintf(stdout, "[%d]\tlocalsize = %d\n", nctx->myrank, nctx->localsize);
#endif /* NEXUS_DEBUG */

    return 0;
}

int nexus_destroy(nexus_ctx_t *nctx)
{
    return 0;
}
