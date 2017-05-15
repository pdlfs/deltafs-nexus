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

struct ssg
{
    hg_class_t *hgcl;
    char **addr_strs;
    hg_addr_t *addrs;
    void *backing_buf;
    int num_addrs;
    int buf_size;
    int rank;
};

// using pointer so that we can use proc (proc has to allocate in this case)
typedef struct ssg *ssg_t;

// some defines
// null pointer shim
#define SSG_NULL ((ssg_t)NULL)

nexus_ctx_t nctx;

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

int nexus_bootstrap(hg_class_t *hg_clz)
{
    ssg_t ssg;

    ssg = ssg_init_mpi(hg_clz, MPI_COMM_WORLD);
    if (ssg == SSG_NULL)
        msg_abort("ssg_lookup");

    return 0;
}

int nexus_destroy(void)
{
    return 0;
}
