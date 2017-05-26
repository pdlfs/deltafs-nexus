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

#include "deltafs_nexus.h"

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

static void print_hg_addr(hg_class_t *hgcl, char *str, hg_addr_t hgaddr)
{
    char *addr_str = NULL;
    hg_size_t addr_size = 0;
    hg_return_t hret;

    hret = HG_Addr_to_string(hgcl, NULL, &addr_size, hgaddr);
    if (hgaddr == NULL)
        msg_abort("HG_Addr_to_string failed");

    addr_str = (char *)malloc(addr_size);
    if (addr_str == NULL)
        msg_abort("malloc failed");

    hret = HG_Addr_to_string(hgcl, addr_str, &addr_size, hgaddr);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Addr_to_string failed");

    fprintf(stdout, "Mercury address: %s => %s\n", str, addr_str);
}

static MPI_Comm get_local_comm(void)
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

    return localcomm;
}
