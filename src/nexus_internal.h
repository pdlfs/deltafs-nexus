/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <map>
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
