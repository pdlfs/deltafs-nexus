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

// some defines
// null pointer shim
#define SSG_NULL ((ssg_t)NULL)
// after init, rank is possibly unknown
#define SSG_RANK_UNKNOWN (-1)
// if ssg_t is gotten from another process (RPC output), then, by definition,
// the receiving entity is not part of the group
#define SSG_EXTERNAL_RANK (-2)

/* XXX: Following code was copied from ssg. Could use some optimization */
static char** setup_addr_str_list(int num_addrs, char * buf)
{
    char **ret = (char **)malloc(num_addrs * sizeof(*ret));
    if (ret == NULL) return NULL;

    ret[0] = buf;
    for (int i = 1; i < num_addrs; i++) {
        char * a = ret[i-1];
        ret[i] = a + strlen(a) + 1;   
    }

    return ret;
}

static hg_return_t ssg_resolve_rank(ssg_t s, hg_class_t *hgcl)
{
    if (s->rank == SSG_EXTERNAL_RANK || s->rank != SSG_RANK_UNKNOWN)
        return HG_SUCCESS;

    // helpers
    hg_addr_t self_addr = HG_ADDR_NULL;
    char * self_addr_str = NULL;
    const char * self_addr_substr = NULL;
    hg_size_t self_addr_size = 0;
    const char * addr_substr = NULL;
    int rank = 0;
    hg_return_t hret;

    // get my address
    hret = HG_Addr_self(hgcl, &self_addr);
    if (hret != HG_SUCCESS) goto end;
    hret = HG_Addr_to_string(hgcl, NULL, &self_addr_size, self_addr);
    if (self_addr == NULL) { hret = HG_NOMEM_ERROR; goto end;  }
    self_addr_str = (char *)malloc(self_addr_size);
    if (self_addr_str == NULL) { hret = HG_NOMEM_ERROR; goto end;  }
    hret = HG_Addr_to_string(hgcl, self_addr_str, &self_addr_size, self_addr);
    if (hret != HG_SUCCESS) goto end;

    // strstr is used here b/c there may be inconsistencies in whether the class
    // is included in the address or not (it's not in HG_Addr_to_string, it
    // should be in ssg_init_config)
    self_addr_substr = strstr(self_addr_str, "://");
    if (self_addr_substr == NULL) { hret = HG_INVALID_PARAM; goto end;  }
    self_addr_substr += 3;
    for (rank = 0; rank < s->num_addrs; rank++) {
        addr_substr = strstr(s->addr_strs[rank], "://");
        if (addr_substr == NULL) { hret = HG_INVALID_PARAM; goto end;  }
        addr_substr+= 3;
        if (strcmp(self_addr_substr, addr_substr) == 0)
            break;
    }
    if (rank == s->num_addrs) {
        hret = HG_INVALID_PARAM;
        goto end;
    }

    // success - set
    s->rank = rank;
    s->addrs[rank] = self_addr; self_addr = HG_ADDR_NULL;

end:
    if (self_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, self_addr);
    free(self_addr_str);

    return hret;
}

typedef struct ssg_lookup_out
{
    hg_return_t hret;
    hg_addr_t addr;
    int *cb_count;
} ssg_lookup_out_t;

static hg_return_t ssg_lookup_cb(const struct hg_cb_info *info)
{
    ssg_lookup_out_t *out = (ssg_lookup_out_t *)info->arg;
    *out->cb_count += 1;
    out->hret = info->ret;
    if (out->hret != HG_SUCCESS)
        out->addr = HG_ADDR_NULL;
    else
        out->addr = info->info.lookup.addr;
    return HG_SUCCESS;
}

static hg_return_t ssg_lookup(ssg_t s, hg_context_t *hgctx)
{
    // set of outputs
    ssg_lookup_out_t *out = NULL;
    int cb_count = 0;
    // "effective" rank for the lookup loop
    int eff_rank = 0;

    // set the hg class up front - need for destructing addrs
    s->hgcl = HG_Context_get_class(hgctx);
    if (s->hgcl == NULL) return HG_INVALID_PARAM;

    // perform search for my rank if not already set
    if (s->rank == SSG_RANK_UNKNOWN) {
        hg_return_t hret = ssg_resolve_rank(s, s->hgcl);
        if (hret != HG_SUCCESS) return hret;
    }

    if (s->rank == SSG_EXTERNAL_RANK) {
        // do a completely arbitrary effective rank determination to try and
        // prevent everyone talking to the same member at once
        eff_rank = (((intptr_t)hgctx)/sizeof(hgctx)) % s->num_addrs;
    } else {
        eff_rank = s->rank;
        cb_count++;
    }

    // init addr metadata
    out = (ssg_lookup_out_t *)malloc(s->num_addrs * sizeof(*out));
    if (out == NULL) return HG_NOMEM_ERROR;
    // FIXME: lookups don't have a cancellation path, so in an intermediate
    // error we can't free the memory, lest we cause a segfault

    // rank is set, perform lookup
    hg_return_t hret;
    for (int i = (s->rank != SSG_EXTERNAL_RANK); i < s->num_addrs; i++) {
        int r = (eff_rank+i) % s->num_addrs;
        out[r].cb_count = &cb_count;
        hret = HG_Addr_lookup(hgctx, &ssg_lookup_cb, &out[r],
                s->addr_strs[r], HG_OP_ID_IGNORE);
        if (hret != HG_SUCCESS) return hret;
    }

    // lookups posted, enter the progress loop until finished
    do {
        unsigned int count = 0;
        do {
            hret = HG_Trigger(hgctx, 0, 1, &count);
        } while (hret == HG_SUCCESS && count > 0);
        if (hret != HG_SUCCESS && hret != HG_TIMEOUT) return hret;
        hret = HG_Progress(hgctx, 100);
    } while (cb_count < s->num_addrs &&
            (hret == HG_SUCCESS || hret == HG_TIMEOUT));

    if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
        return hret;

    for (int i = 0; i < s->num_addrs; i++) {
        if (i != s->rank) {
            if (out[i].hret != HG_SUCCESS)
                return out[i].hret;
            else
                s->addrs[i] = out[i].addr;
        }
    }

    free(out);
    return HG_SUCCESS;
}

static ssg_t ssg_init_mpi(hg_class_t *hgcl, MPI_Comm comm)
{
    // my addr
    hg_addr_t self_addr = HG_ADDR_NULL;
    char * self_addr_str = NULL;
    hg_size_t self_addr_size = 0;
    int self_addr_size_int = 0; // for mpi-friendly conversion

    // collective helpers
    char * buf = NULL;
    int * sizes = NULL;
    int * sizes_psum = NULL;
    int comm_size = 0;
    int comm_rank = 0;

    // hg addresses
    hg_addr_t *addrs = NULL;

    // return data
    char **addr_strs = NULL;
    ssg_t s = NULL;

    // misc return codes
    hg_return_t hret;

    // get my address
    hret = HG_Addr_self(hgcl, &self_addr);
    if (hret != HG_SUCCESS) goto fini;
    hret = HG_Addr_to_string(hgcl, NULL, &self_addr_size, self_addr);
    if (self_addr == NULL) goto fini;
    self_addr_str = (char *)malloc(self_addr_size);
    if (self_addr_str == NULL) goto fini;
    hret = HG_Addr_to_string(hgcl, self_addr_str, &self_addr_size, self_addr);
    if (hret != HG_SUCCESS) goto fini;
    self_addr_size_int = (int)self_addr_size; // null char included in call

    // gather the buffer sizes
    MPI_Comm_size(comm, &comm_size);
    MPI_Comm_rank(comm, &comm_rank);
    sizes = (int *)malloc(comm_size * sizeof(*sizes));
    if (sizes == NULL) goto fini;
    sizes[comm_rank] = self_addr_size_int;
    MPI_Allgather(MPI_IN_PLACE, 0, MPI_BYTE, sizes, 1, MPI_INT, comm);

    // compute a exclusive prefix sum of the data sizes,
    // including the total at the end
    sizes_psum = (int *)malloc((comm_size+1) * sizeof(*sizes_psum));
    if (sizes_psum == NULL) goto fini;
    sizes_psum[0] = 0;
    for (int i = 1; i < comm_size+1; i++)
        sizes_psum[i] = sizes_psum[i-1] + sizes[i-1];

    // allgather the addresses
    buf = (char *)malloc(sizes_psum[comm_size]);
    if (buf == NULL) goto fini;
    MPI_Allgatherv(self_addr_str, self_addr_size_int, MPI_BYTE,
            buf, sizes, sizes_psum, MPI_BYTE, comm);

    // set the addresses
    addr_strs = setup_addr_str_list(comm_size, buf);
    if (addr_strs == NULL) goto fini;

    // init peer addresses
    addrs = (hg_addr **)malloc(comm_size*sizeof(*addrs));
    if (addrs == NULL) goto fini;
    for (int i = 0; i < comm_size; i++) addrs[i] = HG_ADDR_NULL;
    addrs[comm_rank] = self_addr;

    // set up the output
    s = (ssg_t)malloc(sizeof(*s));
    if (s == NULL) goto fini;
    s->hgcl = NULL; // set in ssg_lookup
    s->addr_strs = addr_strs; addr_strs = NULL;
    s->addrs = addrs; addrs = NULL;
    s->backing_buf = buf; buf = NULL;
    s->num_addrs = comm_size;
    s->buf_size = sizes_psum[comm_size];
    s->rank = comm_rank;
    self_addr = HG_ADDR_NULL; // don't free this on success

fini:
    if (self_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, self_addr);
    free(buf);
    free(sizes);
    free(addr_strs);
    free(addrs);
    free(self_addr_str);
    free(sizes_psum);
    return s;
}

static void ssg_finalize(ssg_t s)
{
    if (s == SSG_NULL) return;

    for (int i = 0; i < s->num_addrs; i++) {
        if (s->addrs[i] != HG_ADDR_NULL) HG_Addr_free(s->hgcl, s->addrs[i]);
    }

    free(s->backing_buf);
    free(s->addr_strs);
    free(s->addrs);
    free(s);
}

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
    hg_return_t hret;

    nctx->sctx = ssg_init_mpi(hgcl, MPI_COMM_WORLD);
    if (nctx->sctx == SSG_NULL)
        msg_abort("ssg_init_mpi failed");

    hret = ssg_lookup(nctx->sctx, hgctx);
    if (hret != HG_SUCCESS)
        msg_abort("ssg_lookup failed");

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
    ssg_finalize(nctx->sctx);
    return 0;
}
