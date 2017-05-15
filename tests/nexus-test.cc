/*
 * Copyright (c) 2017 Carnegie Mellon University.
 * George Amvrosiadis <gamvrosi@cs.cmu.edu>
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <cstdio>
#include <stdlib.h>
#include <mpi.h>

#include "deltafs_nexus.h"

int main(int argc, char **argv)
{
    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        perror("Error: MPI_Init failed");
        exit(1);
    }

    if (nexus_bootstrap()) {
        fprintf(stderr, "Error: nexus_bootstrap failed\n");
        goto error;
    }

    if (nexus_destroy()) {
        fprintf(stderr, "Error: nexus_destroy failed\n");
        goto error;
    }

    MPI_Finalize();

    exit(0);

error:
    MPI_Finalize();
    exit(1);
}
