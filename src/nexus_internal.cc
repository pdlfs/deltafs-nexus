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

#include <arpa/inet.h>
#include <assert.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <pthread.h>
#include <sys/socket.h>

#include "nexus_internal.h"

/* macro for error handling */
#define GOTO_DONE(N) do { retval = (N); goto done; } while (0)

namespace {

/*
 * xchg_dat_t: structure used to exchange mercury addressing info,
 * including url address string, over MPI.  the address buffer
 * (fixed sized to largest value needed) follows the structure.
 * after we use MPI to exchange info, we then pass this to nx_lookup_addrs()
 * to convert the url strings into hg_addr_t's.
 */
typedef struct {
  int grank;      /* a global rank */
  int idx;        /* map index (key) to store hg_addr_t under */
  char addr[];    /* address string (follows structure in memory) */
} xchg_dat_t;

/* nx_lookup_ctx: state for a nx_lookup operation */
struct nx_lookup_ctx {
  pthread_mutex_t cb_mutex;   /* syncs caller with callback */
  pthread_cond_t cb_cv;       /* caller waits for callbacks here */
  nexus_map_t* nx_map;        /* the address map we'll be updating */
  int done;                   /* num of ops done */
};

/* nx_lookup_out: arg passed to lookup callback nx_lookup_cb() */
struct nx_lookup_out {
  struct nx_lookup_ctx* ctx;  /* lookup context that owns this lookup */
  hg_return_t hret;           /* lookup return value */
  int idx;                    /* map index/key to store addr in if success */
};

/*
 * nx_lookup_cb: adress lookup callback function.  find our nx_lookup_out,
 * plug in the results of the lookup, and then notify the caller.
 */
hg_return_t nx_lookup_cb(const struct hg_cb_info* info) {
  struct nx_lookup_out* const out = (struct nx_lookup_out*)info->arg;

  pthread_mutex_lock(&out->ctx->cb_mutex);

  out->hret = info->ret;
  if (out->hret == HG_SUCCESS)
    (*(out->ctx->nx_map))[out->idx] = info->info.lookup.addr;
  out->ctx->done += 1;

  pthread_cond_signal(&out->ctx->cb_cv);
  pthread_mutex_unlock(&out->ctx->cb_mutex);

  return HG_SUCCESS;
}

/*
 * nx_lookup_addrs: look up a bunch of mercury address strings
 *
 * @param nx the nexus context we are working in
 * @param phand the progressor to use for lookups
 * @param xarr xchg array with "xsize" entries
 * @param addrsz sizeof() one entry in xarr[]
 * @param map map to put results in
 * @return 0 or -1 on failure
 */
int nx_lookup_addrs(nexus_ctx_t nx, progressor_handle_t *phand,
                    xchg_dat_t *xarr, int xsize, int addrsz,
                    nexus_map_t *map) {
  int retval = 0;               /* assume success, set to -1 on error */
  struct nx_lookup_ctx ctx;
  struct nx_lookup_out* out;    /* array: out[0 .. (xsize-1)] */
  int local, eff_offset, i;
  hg_return_t hret;
  hg_addr_t self_addr;

  pthread_mutex_init(&ctx.cb_mutex, NULL);
  pthread_cond_init(&ctx.cb_cv, NULL);
  ctx.nx_map = map;
  ctx.done = 0;

  out = (struct nx_lookup_out*)malloc(sizeof(*out) * xsize);
  if (!out) {
    fprintf(stderr, "nx_lookup_addrs: malloc failed\n");
    GOTO_DONE(-1);
  }

  /* determine if we are local, use my rank as starting point */
  local = (phand == nx->hg_local);
  eff_offset = (local) ? nx->lrank : nx->grank;
  i = 0;

  hret = HG_SUCCESS;
  pthread_mutex_lock(&ctx.cb_mutex);
  while (hret == HG_SUCCESS && i < xsize) {
    int remain = xsize - i;
    int cando = (remain < nx->nx_limit) ? remain : nx->nx_limit;

    /* start as many as we can */
    while (hret == HG_SUCCESS && cando-- > 0) {
      const int eff_i = (i + eff_offset) % xsize;

      xchg_dat_t* const xi =
          (xchg_dat_t*)(((char*)xarr) + eff_i * (sizeof(*xi) + addrsz));

      out[eff_i].hret = HG_SUCCESS;
      out[eff_i].idx = xi->idx;      /* map key to use */
      out[eff_i].ctx = &ctx;

      if (xi->grank != nx->grank) {
        pthread_mutex_unlock(&ctx.cb_mutex);

        hret = HG_Addr_lookup(mercury_progressor_hgcontext(phand),
                              &nx_lookup_cb, &out[eff_i], xi->addr,
                              HG_OP_ID_IGNORE);

        pthread_mutex_lock(&ctx.cb_mutex);
      } else {
        hret = HG_Addr_self(mercury_progressor_hgclass(phand), &self_addr);

        if (hret == HG_SUCCESS) { /* directly add address to map */
          (*(ctx.nx_map))[xi->idx] = self_addr;

          ctx.done += 1;
        }
      }

      if (hret != HG_SUCCESS) {
        out[eff_i].hret = hret;
        ctx.done += 1;
      }
      i++;
    }    /* cando */

    while (ctx.done < i) { /* XXX: ok to break out if just one slot is free? */
      pthread_cond_wait(&ctx.cb_cv, &ctx.cb_mutex);
    }

  }      /* i < xsize */
  pthread_mutex_unlock(&ctx.cb_mutex);

  hret = HG_SUCCESS;
  for (int j = 0; j < i; j++) {
    const int eff_j = (j + eff_offset) % xsize;
    if (out[eff_j].hret != HG_SUCCESS) {
      hret = out[eff_j].hret;
      break;
    }
  }
  if (hret != HG_SUCCESS) {
    fprintf(stderr, "nx_lookup_addrs: mercury lookup error %d\n", hret);
    GOTO_DONE(-1);
  }

done:
  if (out) free(out);
  pthread_cond_destroy(&ctx.cb_cv);
  pthread_mutex_destroy(&ctx.cb_mutex);
  return(retval);
}

} // namespace

/* nx_mpisetup: setup our MPI comms.  return -1 on error. */
int nx_mpisetup(nexus_ctx_t nx) {
  int color;

  /* mycomm is our global comm, get our global rank & size from it */
  if (MPI_Comm_rank(nx->mycomm, &nx->grank) != MPI_SUCCESS ||
      MPI_Comm_size(nx->mycomm, &nx->gsize) != MPI_SUCCESS) {
    fprintf(stderr, "nx_mpisetup: can't get grank/gsize\n");
    return(-1);
  }

  /* split out local procs into a new localcomm and get local rank/size */
  if (MPI_Comm_split_type(nx->mycomm, MPI_COMM_TYPE_SHARED, 0,
                          MPI_INFO_NULL, &nx->localcomm) != MPI_SUCCESS ||
      MPI_Comm_rank(nx->localcomm, &nx->lrank) != MPI_SUCCESS         ||
      MPI_Comm_size(nx->localcomm, &nx->lsize) != MPI_SUCCESS) {
    fprintf(stderr, "nx_mpisetup: comm local split failed\n");
    return(-1);
  }

  /* generate local2global[] - a mapping from local rank to global rank  */
  nx->local2global = (int *)malloc(sizeof(int) * nx->lsize);
  if (!nx->local2global) {
    fprintf(stderr, "nx_mpisetup: local2global malloc failed\n");
    return(-1);
  }
  if (MPI_Allgather(&nx->grank, 1, MPI_INT, nx->local2global, 1, MPI_INT,
                    nx->localcomm) != MPI_SUCCESS) {
    fprintf(stderr, "nx_mpisetup: local2global allgather failed\n");
    return(-1);
  }

  /* get global rank of local rank 0 (a.k.a. lroot) */
  nx->lroot = nx->local2global[0];

  /* split out the repcomm (has all local rank 0's) */
  color = (nx->grank == nx->lroot) ? 1 : MPI_UNDEFINED; /* am I in? */
  if (MPI_Comm_split(nx->mycomm, color, nx->grank, &nx->repcomm) !=
      MPI_SUCCESS) {
    fprintf(stderr, "nx_mpisetup: rep split failed\n");
    return(-1);
  }

  /* only local rank 0 gets the repcomm - get nodeid and nnodes */
  if (nx->repcomm != MPI_COMM_NULL) {
    if (MPI_Comm_rank(nx->repcomm, &nx->nodeid) != MPI_SUCCESS ||
        MPI_Comm_size(nx->repcomm, &nx->nnodes) != MPI_SUCCESS) {
      fprintf(stderr, "nx_mpisetup: local rank0 node setup failed\n");
      return(-1);
    }
  }
  /* now each local rank 0 can broadcast nodeid/nnodes to other local procs */
  if (MPI_Bcast(&nx->nodeid, 1, MPI_INT, 0, nx->localcomm) != MPI_SUCCESS ||
      MPI_Bcast(&nx->nnodes, 1, MPI_INT, 0, nx->localcomm) != MPI_SUCCESS) {
      fprintf(stderr, "nx_mpisetup: node info dist failed\n");
      return(-1);
  }

  /* generate rank2node[] - a mapping from global rank to its node id  */
  nx->rank2node = (int*)malloc(sizeof(int) * nx->gsize);
  if (!nx->rank2node) {
    fprintf(stderr, "nx_mpisetup: rank2node malloc failed\n");
    return(-1);
  }
  if (MPI_Allgather(&nx->nodeid, 1, MPI_INT, nx->rank2node, 1, MPI_INT,
                nx->mycomm) != MPI_SUCCESS) {
    fprintf(stderr, "nx_mpisetup: rank2node allgather failed\n");
    return(-1);
  }

  return(0);
}

/* nx_build_lmap: build lmap.  return -1 on error. */
int nx_build_lmap(nexus_ctx_t nx) {
  int retval = 0;               /* assume success, set to -1 on error */
  char *myaddrstr;
  int my_sz, xchg_sz, rv;
  xchg_dat_t *xitem = NULL, *xarray = NULL;

  /* get my address, its length, and use MPI to get the max size for local */
  myaddrstr = mercury_progressor_addrstring(nx->hg_local);
  my_sz = strlen(myaddrstr) + 1;
  if (MPI_Allreduce(&my_sz, &nx->laddrsz, 1, MPI_INT,
                    MPI_MAX, nx->localcomm) != MPI_SUCCESS) {
    fprintf(stderr, "nx_build_lmap: localcomm size reduce failed\n");
    return(-1);
  }
  xchg_sz = sizeof(xchg_dat_t) + nx->laddrsz;

  /* malloc buffers for MPI data exchange */
  xitem = (xchg_dat_t *)malloc(xchg_sz);
  xarray = (xchg_dat_t *)malloc(nx->lsize * xchg_sz);
  if (!xitem || !xarray) {
    fprintf(stderr, "nx_build_lmap: xchg malloc fail\n");
    GOTO_DONE(-1);
  }
  xitem->grank = nx->grank;       /* my global rank */
  xitem->idx = nx->grank;         /* use global rank as lmap key */
  strcpy(xitem->addr, myaddrstr); /* copy addr over */

  /* now exchange the local data */
  if (MPI_Allgather(xitem, xchg_sz, MPI_BYTE, xarray, xchg_sz, MPI_BYTE,
                nx->localcomm) != MPI_SUCCESS) {
    fprintf(stderr, "nx_build_lmap: mpi all gather failed\n");
    GOTO_DONE(-1);
  }

  /* now we need to fire up local mercury and do address lookups */
  if (mercury_progressor_needed(nx->hg_local) != HG_SUCCESS) {
    fprintf(stderr, "nx_build_lmap: progressor needed failed\n");
    GOTO_DONE(-1);
  }

  MPI_Barrier(nx->localcomm);
  rv = nx_lookup_addrs(nx, nx->hg_local, xarray, nx->lsize,
                       nx->laddrsz, &nx->lmap);
  if (rv < 0) {
    retval = -1;
    fprintf(stderr, "nx_build_lmap: nx_lookup_addrs failed\n");
  }
  MPI_Barrier(nx->localcomm);

  /* now we can stop mercury */
  if (mercury_progressor_idle(nx->hg_local) != HG_SUCCESS) {
    fprintf(stderr, "nx_build_lmap: progressor idle failed\n");
    GOTO_DONE(-1);
  }

done:
  if (xitem) free(xitem);
  if (xarray) free(xarray);
  return(retval);
}

/* nx_build_rmap: build rmap.  return -1 on error. */
int nx_build_rmap(nexus_ctx_t nx) {
  int retval = 0;               /* assume success, set to -1 on error */
  char *myaddrstr, *rank2addr = NULL, *addrcpy = NULL;
  int my_sz, xchg_sz, rv, *nodeinfo = NULL, *myinfo = NULL;
  int M, i, P, c;
  xchg_dat_t *xitem = NULL, *xarray = NULL;

  /* get my address, its length, and use MPI to get the max size for remote */
  myaddrstr = mercury_progressor_addrstring(nx->hg_remote);
  my_sz = strlen(myaddrstr) + 1;
  if (MPI_Allreduce(&my_sz, &nx->gaddrsz, 1, MPI_INT,
                    MPI_MAX, nx->mycomm) != MPI_SUCCESS) {
    fprintf(stderr, "nx_build_rmap: mycomm size reduce failed\n");
    return(-1);
  }

  /*
   * stop here if only one node (leave node2rep at NULL).  in this case,
   * the local mercury will do all the work.
   */
  if (nx->nnodes <= 1)
    return(0);

  /*
   * to setup the remote network, we need to know a) which remote peers we
   * should connect to, and b) what are their addresses. but to accomplish a,
   * we need to first prepare two auxiliary arrays: rank2addr which maps each
   * rank to its remote address, and nodeinfo which is going to contain the
   * manifest information for each remote node.
   *
   * our first step is to prepare the rank2addr array.
   */
  rank2addr = (char *)malloc(nx->gsize * nx->gaddrsz);
  if (!rank2addr) {
    fprintf(stderr, "nx_build_rmap: rank2addr malloc failed\n");
    GOTO_DONE(-1);
  }
  addrcpy = (char *)malloc(nx->gaddrsz);
  if (!addrcpy) {
    fprintf(stderr, "nx_build_rmap: addrcpy malloc failed\n");
    GOTO_DONE(-1);
  }
  memset(addrcpy, 0, nx->gaddrsz);
  snprintf(addrcpy, nx->gaddrsz, "%s", myaddrstr);
  if (MPI_Allgather(addrcpy, nx->gaddrsz, MPI_BYTE,
                    rank2addr, nx->gaddrsz, MPI_BYTE,
                                            nx->mycomm) != MPI_SUCCESS) {
    fprintf(stderr, "nx_build_rmap: rank2addr allgather failed\n");
    GOTO_DONE(-1);
  }
  free(addrcpy);
  addrcpy = NULL;

  /*
   * we now prepare the nodeinfo array in 4 steps:
   * 1. find max number of ranks per node
   * 2. have eachnode rep construct a (M+1)-sized array containing
   *    (#ranks, r0, r1, ...)
   * 3. have node reps all-gather their (M+1)-sized arrays over repcomm
   * 4. have each node rep broadcast the finished array (on localcomm)
   */

  /* 1. get "M" - max ranks per node */
  if (MPI_Allreduce(&nx->lsize, &M, 1, MPI_INT,
                    MPI_MAX, nx->mycomm) != MPI_SUCCESS) {
    fprintf(stderr, "nx_build_rmap: lsize allreduce failed\n");
    GOTO_DONE(-1);
  }

  /* now malloc nodeinfo */
  nodeinfo = (int *) malloc(sizeof(int) * (M + 1) * nx->nnodes);
  if (!nodeinfo) {
    fprintf(stderr, "nx_build_rmap: malloc nodeinfo failed\n");
    GOTO_DONE(-1);
  }

  /* the next two steps are for reps only */
  if (nx->repcomm != MPI_COMM_NULL) {   /* we are a rep? */
    /* 2. reps prepare their info */
    myinfo = (int *)malloc(sizeof(int) * (M + 1));
    if (!myinfo) {
      fprintf(stderr, "nx_build_rmap: malloc myinfo failed\n");
      GOTO_DONE(-1);
    }
    myinfo[0] = nx->lsize;
    for (i = 0 ; i < M ; i++)
      myinfo[i+1] = (i < nx->lsize) ? nx->local2global[i] : -1;

    /* 3. reps exchange their info over repcomm */
    if (MPI_Allgather(myinfo, M+1, MPI_INT,
                      nodeinfo, M+1, MPI_INT, nx->repcomm) != MPI_SUCCESS) {
      fprintf(stderr, "nx_build_rmap: allgather nodeinfo failed\n");
      GOTO_DONE(-1);
    }

    free(myinfo);
    myinfo = NULL;
  }

  /* 4. reps broadcast finished array on localcomm to all local procs */
  if (MPI_Bcast(nodeinfo, (M+1) * nx->nnodes * sizeof(int), MPI_BYTE, 0,
            nx->localcomm) != MPI_SUCCESS) {
    fprintf(stderr, "nx_build_rmap: malloc local bcast nodeinfo failed\n");
    GOTO_DONE(-1);
  }

  /*
   * now that we have nodeinfo[], we can build nx->node2rep[] which
   * maps node number to its rep's global rank.
   */
  nx->node2rep = (int*) malloc(sizeof(int) * nx->nnodes);
  if (!nx->node2rep) {
    fprintf(stderr, "nx_build_rmap: malloc node2rep failed\n");
    GOTO_DONE(-1);
  }

  /*
   * the representative for a node is one whose local rank is equal to our
   * node's id modulo the remote node's lsize.
   */
  for (i = 0 ; i < nx->nnodes ; i++) {   /* foreach node i */
#define IDX(i) (i * (M+1))
    nx->node2rep[i] = nodeinfo[IDX(i) + 1 + (nx->nodeid % nodeinfo[IDX(i)])];
#undef IDX
  }

  /* done with nodeinfo[] */
  free(nodeinfo);
  nodeinfo = NULL;

  /*
   * we distribute 'nnodes' remote nodes among 'lsize' local procs.
   *
   * P = max possible number of peers we can be responsible for
   */
  P = (nx->nnodes / nx->lsize) + ((nx->nnodes % nx->lsize) ? 1 : 0);

  /*
   * setup for the address lookups
   */
  xarray = (xchg_dat_t*)malloc(P * (sizeof(*xarray) + nx->gaddrsz));
  if (!xarray) {
    fprintf(stderr, "nx_build_rmap: malloc xarray failed\n");
    GOTO_DONE(-1);
  }

  /*
   * a node (node, not a rank) is a peer iff its node id modulo our node's
   * lsize is equal to our lrank.
   */
  i = nx->lrank;    /* i is a node number */
  c = 0;            /* current entry# in xarray */
  while (i < nx->nnodes) {
#define NADDR(x, y, s) (&x[y * s])

    char *raddr;

    /* skip ourselves */
    if (i == nx->nodeid) {
      i += nx->lsize;
      continue;
    }

    raddr = NADDR(rank2addr, nx->node2rep[i], nx->gaddrsz);

    xitem = (xchg_dat_t *)(((char*)xarray) +
                                     c * (sizeof(*xitem) + nx->gaddrsz));
    xitem->idx = i;   /* use node id as address indexes */
    xitem->grank = nx->node2rep[i];
    strcpy(xitem->addr, raddr);

    i += nx->lsize;
    c++;

#undef NADDR
  }

  free(rank2addr);
  rank2addr = NULL;

  /* now we need to fire up remote mercury and do address lookups */
  if (mercury_progressor_needed(nx->hg_remote) != HG_SUCCESS) {
    fprintf(stderr, "nx_build_rmap: progressor needed failed\n");
    GOTO_DONE(-1);
  }


  /* lookup addresses if we've got any */
  MPI_Barrier(nx->mycomm);
  if (c != 0) {
    rv = nx_lookup_addrs(nx, nx->hg_remote, xarray, c, nx->gaddrsz, &nx->rmap);
    if (rv < 0)
      fprintf(stderr, "nx_build_rmap: nx_lookup_addrs failed\n");
  }
  MPI_Barrier(nx->mycomm);

  /* now we can stop mercury */
  if (mercury_progressor_idle(nx->hg_remote) != HG_SUCCESS) {
    fprintf(stderr, "nx_build_rmap: progressor idle failed\n");
    GOTO_DONE(-1);
  }

  free(xarray);
  xarray = NULL;

done:
  if (xarray) free(xarray);
  if (rank2addr) free(rank2addr);
  if (addrcpy) free(addrcpy);
  if (nodeinfo) free(nodeinfo);
  if (myinfo) free(myinfo);

  return(retval);
}

/* nx_destroy: do the actual work of disposing of an nctx */
void nx_destroy(nexus_ctx_t nctx, int do_barrier) {
  nexus_map_t::iterator it;
  hg_class_t *cls;

  if (nctx->hg_local) {
    cls = mercury_progressor_hgclass(nctx->hg_local);
    for (it = nctx->lmap.begin(); it != nctx->lmap.end(); ++it) {
      if (it->second != HG_ADDR_NULL) {
        HG_Addr_free(cls, it->second);
      }
    }
    mercury_progressor_freehandle(nctx->hg_local);
  }

  if (nctx->localcomm != MPI_COMM_NULL) {
    if (do_barrier)
      MPI_Barrier(nctx->localcomm);
    MPI_Comm_free(&nctx->localcomm);
  }

  if (nctx->hg_remote) {
    cls = mercury_progressor_hgclass(nctx->hg_remote);
    for (it = nctx->rmap.begin(); it != nctx->rmap.end(); ++it) {
      if (it->second != HG_ADDR_NULL) {
        HG_Addr_free(cls, it->second);
      }
    }
    mercury_progressor_freehandle(nctx->hg_remote);
  }

  MPI_Barrier(nctx->mycomm);
  if (nctx->repcomm != MPI_COMM_NULL) {
    if (do_barrier)
      MPI_Barrier(nctx->repcomm);
    MPI_Comm_free(&nctx->repcomm);
  }

  if (nctx->local2global) free(nctx->local2global);
  if (nctx->rank2node) free(nctx->rank2node);
  if (nctx->node2rep) free(nctx->node2rep);
  delete nctx;
}
