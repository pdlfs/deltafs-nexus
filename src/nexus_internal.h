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
#include <map>
#include <mpi.h>
using namespace std;

#include "deltafs_nexus.h"

/*
 * Nexus library context
 */
typedef struct nexus_ctx
{
    int myrank;                     /* my MPI rank */
    int reprank;                    /* my representative's rank */

    int ranksize;                   /* total number of ranks */
    int nlocal;                     /* number of ranks in my node */

    int *replist;                   /* array of rank -> representative */
    map<int, hg_addr_t> hgaddrs;    /* map of rank -> hg address */

} nexus_ctx_t;

extern nexus_ctx_t nctx;

/*
 * log: append message into a given file.
 */
static inline void
log(int fd, const char* fmt, ...)
{
    char tmp[500];
    va_list va;
    int n;
    va_start(va, fmt);
    n = vsnprintf(tmp, sizeof(tmp), fmt, va);
    n = write(fd, tmp, n);
    va_end(va);
    errno = 0;

}

#if defined(VPIC_COLOR_TERM) /* add color to message headers */
#define ABORT "\033[0;31m!!! ABORT !!!\033[0m"
#define ERROR "\033[0;31m!!! ERROR !!!\033[0m"
#define WARNING "\033[0;33m!!! WARNING !!!\033[0m"
#define INFO "\033[0;32m-INFO-\033[0m"
#else /* no color */
#define ABORT "!!! ABORT !!!"
#define ERROR "!!! ERROR !!!"
#define WARNING "!!! WARNING !!!"
#define INFO "-INFO-"
#endif

/*
 * msg_abort: abort with a message
 */
static inline void
msg_abort(const char* msg)
{
    if (errno != 0) {
        log(fileno(stderr), ABORT " %s: %s\n", msg, strerror(errno));   
    } else {
        log(fileno(stderr), ABORT " %s\n", msg);
    }

    abort();
}
