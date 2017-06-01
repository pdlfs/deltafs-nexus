/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <cstdio>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <mpi.h>
#include <string.h>

#include "deltafs-nexus_api.h"

//#define NEXUS_DEBUG

/*
 * msg_abort: abort with a message
 */
static inline void msg_abort(const char* msg)
{
    if (errno != 0) {
        fprintf(stderr, "Error: %s (%s)\n", msg, strerror(errno));   
    } else {
        fprintf(stderr, "Error: %s\n", msg);
    }

    abort();
}

static void print_addrs(nexus_ctx_t *nctx, hg_class_t *hgcl,
                        std::map<int,hg_addr_t> addrmap)
{
    std::map<int,hg_addr_t>::iterator it;
    char *addr_str = NULL;
    hg_size_t addr_size = 0;
    hg_return_t hret;

    for (it = addrmap.begin(); it != addrmap.end(); it++) {
        hret = HG_Addr_to_string(hgcl, NULL, &addr_size, it->second);
        if (hret != HG_SUCCESS)
            msg_abort("HG_Addr_to_string failed");

        addr_str = (char *)malloc(addr_size);
        if (addr_str == NULL)
            msg_abort("malloc failed");

        hret = HG_Addr_to_string(hgcl, addr_str, &addr_size, it->second);
        if (hret != HG_SUCCESS)
            msg_abort("HG_Addr_to_string failed");

        fprintf(stdout, "[%d] Mercury addr for rank %d: %s\n",
                nctx->grank, it->first, addr_str);
        free(addr_str);
    }
}

static void init_local_comm(nexus_ctx_t *nctx)
{
    int ret;

#if MPI_VERSION >= 3
    ret = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
                              MPI_INFO_NULL, &nctx->localcomm);
    if (ret != MPI_SUCCESS)
        msg_abort("MPI_Comm_split_type failed");
#else
    /* XXX: Need to find a way to deal with MPI_VERSION < 3 */
    msg_abort("Nexus needs MPI version 3 or higher");
#endif
}

static void init_rep_comm(nexus_ctx_t *nctx)
{
    int ret;

#if MPI_VERSION >= 3
    ret = MPI_Comm_split(MPI_COMM_WORLD, (nctx->grank == nctx->lroot),
                         nctx->grank, &nctx->repcomm);
    if (ret != MPI_SUCCESS)
        msg_abort("MPI_Comm_split_type failed");
#else
    /* XXX: Need to find a way to deal with MPI_VERSION < 3 */
    msg_abort("Nexus needs MPI version 3 or higher");
#endif
}
