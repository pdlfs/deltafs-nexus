/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <mercury.h>

/*
 * Bootstraps all MPI ranks with the Nexus library
 */
int nexus_bootstrap(hg_class_t *hg_clz);

/*
 * Destroys the Nexus library freeing all allocated resources
 */
int nexus_destroy(void);

/*
 * Returns true if the rank is local to the caller
 */
bool nexus_is_local(int rank);

/*
 * Returns the representative MPI rank (in COMM_WORLD) in the node
 * with the provided rank
 */
int nexus_get_rep(int rank);

/*
 * Returns the Mercury address of the provided rank
 */
int nexus_get_addr(int rank, hg_addr_t *addr);
