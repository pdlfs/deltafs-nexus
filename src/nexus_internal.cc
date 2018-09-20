/*
 * Copyright (c) 2017-2018, Carnegie Mellon University.
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

#define DEFAULT_NX_LIMIT 4
#define TMPADDRSZ 64

namespace {
typedef struct {
  int grank;
  int idx;
  char addr[];
} xchg_dat_t;

/* nx_fatal: abort with a message */
void nx_fatal(const char* msg) {
  if (errno != 0)
    fprintf(stderr, "NX FATAL: %s (%s)\n", msg, strerror(errno));
  else
    fprintf(stderr, "NX FATAL: %s\n", msg);

  abort();
}

/* nx_is_envset: check if a given env is set */
bool nx_is_envset(const char* name) {
  char* env = getenv(name);
  if (env == NULL || env[0] == 0) return false;
  return strcmp(env, "0") != 0;
}

struct bgthread_dat {
  hg_context_t* hgctx;
  nexus_ctx_t nctx;
  int bgdone;
};

/*
 * network support pthread. Need to call progress to push the network and then
 * trigger to run the callback.
 */
void* nx_bgthread(void* arg) {
  struct bgthread_dat* const bgdat = (struct bgthread_dat*)arg;
  hg_return_t hret;
  int r = bgdat->nctx->grank;
#ifdef NEXUS_DEBUG
  fprintf(stderr, "NX-%d: BG UP\n", r);
#endif

  while (!bgdat->bgdone) {
    unsigned int count = 0;
    do {
      hret = HG_Trigger(bgdat->hgctx, 0, 1, &count);
    } while (hret == HG_SUCCESS && count);

    if (hret != HG_SUCCESS && hret != HG_TIMEOUT) {
      nx_fatal("bg:HG_Trigger");
    }
    hret = HG_Progress(bgdat->hgctx, 100);
    if (hret != HG_SUCCESS && hret != HG_TIMEOUT) {
      nx_fatal("bg:HG_Progress");
    }
  }

#ifdef NEXUS_DEBUG
  fprintf(stderr, "NX-%d: BG DOWN\n", r);
#endif
  return NULL;
}

/*
 * nx_get_ipmatch: look at our interfaces and find an IP address that
 * matches our spec...  aborts on failure.
 */
void nx_get_ipmatch(char* subnet, char* i, int ilen) {
  struct ifaddrs *ifaddr, *cur;
  int family;

  /* Query local socket layer to get our IP addr */
  if (getifaddrs(&ifaddr) == -1) nx_fatal("getifaddrs failed");

  for (cur = ifaddr; cur != NULL; cur = cur->ifa_next) {
    if (cur->ifa_addr != NULL) {
      family = cur->ifa_addr->sa_family;

      if (family == AF_INET) {
        if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in), i, ilen,
                        NULL, 0, NI_NUMERICHOST) == -1)
          nx_fatal("getnameinfo failed");

        if (strncmp(subnet, i, strlen(subnet)) == 0) break;
      }
    }
  }

  if (cur == NULL) nx_fatal("no ip addr");

  freeifaddrs(ifaddr);
}

/*
 * Put together the remote Mercury endpoint address from bootstrap parameters.
 * Writes the server URI into *uri on success. Aborts on error.
 */
void nx_prepare_addr(nexus_ctx_t nctx, char* subnet, char* proto, char* uri) {
  char ip[16];
  int lcv, port, so, n;
  struct sockaddr_in addr;
  socklen_t addr_len = sizeof(addr);

  nx_get_ipmatch(subnet, ip, sizeof(ip)); /* fill ip w/req'd local IP */

  if (strcmp(proto, "bmi+tcp") == 0) {
    /*
     * XXX: bmi+tcp HG_Addr_to_string() is broken.  if we request
     * port 0 (to let OS fill in) and later use HG_Addr_to_string()
     * to request the actual port number allocated, it still returns
     * 0 as the port number...   here's an attempt to hack around
     * this bug.
     */
    so = socket(PF_INET, SOCK_STREAM, 0);
    if (so < 0) nx_fatal("bmi:socket1");
    n = 1;
    setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n));
    for (lcv = 0; lcv < 1024; lcv++) {
      /* have each local rank try a different set of ports */
      port = 10000 + (lcv * nctx->lsize) + nctx->lrank;
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = INADDR_ANY;
      addr.sin_port = htons(port);
      n = bind(so, (struct sockaddr*)&addr, addr_len);
      if (n == 0) break;
    }
    close(so);
    if (n != 0) nx_fatal("bmi:socket2");

    sprintf(uri, "%s://%s:%d", proto, ip, port);

  } else {
    /* set port 0, let OS fill it, collect later w/HG_Addr_to_string */
    sprintf(uri, "%s://%s:0", proto, ip);
  }
}

struct nx_lookup_ctx {
  /* the address map we'll be updating */
  nexus_map_t* nx_map;
  /* num of ops done */
  int done;

  pthread_mutex_t cb_mutex;
  pthread_cond_t cb_cv;
};

struct nx_lookup_out {
  struct nx_lookup_ctx* ctx;
  hg_return_t hret;
  int idx;
};

hg_return_t nx_lookup_cb(const struct hg_cb_info* info) {
  struct nx_lookup_out* const out = (struct nx_lookup_out*)info->arg;
  out->hret = info->ret;

  pthread_mutex_lock(&out->ctx->cb_mutex);

  if (out->hret == HG_SUCCESS)
    (*(out->ctx->nx_map))[out->idx] = info->info.lookup.addr;
  out->ctx->done += 1;

  pthread_cond_signal(&out->ctx->cb_cv);
  pthread_mutex_unlock(&out->ctx->cb_mutex);

  return HG_SUCCESS;
}

hg_return_t nx_lookup_addrs(nexus_ctx_t nctx, nexus_hg_t* hg, xchg_dat_t* xarr,
                            int xsize, int addrsz, nexus_map_t* map) {
  struct nx_lookup_out* out;
  hg_addr_t self_addr;
  hg_return_t hret;

  struct nx_lookup_ctx ctx;
  out = (struct nx_lookup_out*)malloc(sizeof(*out) * xsize);
  if (out == NULL) return HG_NOMEM_ERROR;

  hret = HG_SUCCESS;
  pthread_mutex_init(&ctx.cb_mutex, NULL);
  pthread_cond_init(&ctx.cb_cv, NULL);
  ctx.nx_map = map;
  ctx.done = 0;

  /* determine if we are local, use my rank as starting point */
  const int local = (hg == nctx->hg_local);
  int eff_offset = (local) ? nctx->lrank : nctx->grank;
  int i = 0;

  pthread_mutex_lock(&ctx.cb_mutex);

  while (hret == HG_SUCCESS && i < xsize) {
    int remain = xsize - i;
    int cando = remain < nctx->nx_limit ? remain : nctx->nx_limit;

    /* start as many as we can */
    while (hret == HG_SUCCESS && cando-- > 0) {
      const int eff_i = (i + eff_offset) % xsize;

      xchg_dat_t* const xi =
          (xchg_dat_t*)(((char*)xarr) + eff_i * (sizeof(*xi) + addrsz));

      out[eff_i].hret = HG_SUCCESS;
      out[eff_i].idx = xi->idx;
      out[eff_i].ctx = &ctx;

      if (xi->grank != nctx->grank) {
        pthread_mutex_unlock(&ctx.cb_mutex);

        hret = HG_Addr_lookup(hg->hg_ctx, &nx_lookup_cb, &out[eff_i], xi->addr,
                              HG_OP_ID_IGNORE);

        pthread_mutex_lock(&ctx.cb_mutex);
      } else {
        hret = HG_Addr_self(hg->hg_cl, &self_addr);

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
    }

    while (ctx.done < i) { /* XXX: ok to break out if just one slot is free? */
      pthread_cond_wait(&ctx.cb_cv, &ctx.cb_mutex);
    }
  }

  pthread_mutex_unlock(&ctx.cb_mutex);

  hret = HG_SUCCESS;
  for (int j = 0; j < i; j++) {
    const int eff_j = (j + eff_offset) % xsize;
    if (out[eff_j].hret != HG_SUCCESS) {
      hret = out[eff_j].hret;
      break;
    }
  }

  free(out);

  pthread_cond_destroy(&ctx.cb_cv);
  pthread_mutex_destroy(&ctx.cb_mutex);
  return hret;
}

/*
 * nx_dump_addrs: dump a given map to stderr.
 */
void nx_dump_addrs(nexus_ctx_t nctx, nexus_hg_t* hg, nexus_map_t* map) {
  char addr[TMPADDRSZ];
  hg_return_t hret;

  const char* map_name = (map == &nctx->lmap) ? "lmap" : "rmap";
  nexus_map_t::iterator it = map->begin();

  for (; it != map->end(); ++it) {
    hg_size_t addr_sz = sizeof(addr);
    hret = HG_Addr_to_string(hg->hg_cl, addr, &addr_sz, it->second);
    if (hret != HG_SUCCESS) strcpy(addr, "n/a");
    fprintf(stderr, "NX-%d: %s[%d]=%s\n", nctx->grank, map_name, it->first,
            addr);
  }
}

void nx_init_localcomm(nexus_ctx_t nctx) {
  if (MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
                          MPI_INFO_NULL, &nctx->localcomm) != MPI_SUCCESS)
    nx_fatal("MPI_Comm_split_type");
  if (MPI_Comm_rank(nctx->localcomm, &nctx->lrank) != MPI_SUCCESS)
    nx_fatal("MPI_Comm_rank");
  if (MPI_Comm_size(nctx->localcomm, &nctx->lsize) != MPI_SUCCESS)
    nx_fatal("MPI_Comm_size");
}

void nx_init_repcomm(nexus_ctx_t nctx) {
  int color = (nctx->grank == nctx->lroot) ? 1 : MPI_UNDEFINED;
  if (MPI_Comm_split(MPI_COMM_WORLD, color, nctx->grank, &nctx->repcomm) !=
      MPI_SUCCESS)
    nx_fatal("MPI_Comm_split");
  if (nctx->repcomm != MPI_COMM_NULL) {
    if (MPI_Comm_rank(nctx->repcomm, &nctx->nodeid) != MPI_SUCCESS)
      nx_fatal("MPI_Comm_rank");
    if (MPI_Comm_size(nctx->repcomm, &nctx->nnodes) != MPI_SUCCESS)
      nx_fatal("MPI_Comm_size");
  }
}

/*
 * nx_setup_local_via_remote: setup the local network substrate by reusing the
 * remote substrate. must be called after nx_setup_local and nx_discover_remote.
 */
void nx_setup_local_via_remote(nexus_ctx_t nctx) {
  hg_addr_t addr_self;
  hg_size_t addr_sz;
  int ret, my_sz, xchg_sz;
  char addr[TMPADDRSZ];
  xchg_dat_t *xitm, *xarr;
  pthread_t bgthread; /* network bg thread */
  struct bgthread_dat bgarg;
  hg_return_t hret;

  assert(nctx->hg_remote != NULL);
  assert(nctx->hg_local == NULL);
  nctx->hg_local = nctx->hg_remote;
  nctx->hg_local->refs++;

  /* fetch self-address */
  hret = HG_Addr_self(nctx->hg_local->hg_cl, &addr_self);
  if (hret != HG_SUCCESS) {
    nx_fatal("lo:HG_Addr_self");
  }
  addr_sz = sizeof(addr);
  hret = HG_Addr_to_string(nctx->hg_local->hg_cl, addr, &addr_sz, addr_self);
  if (hret != HG_SUCCESS) {
    nx_fatal("lo:HG_Addr_to_string");
  }
  HG_Addr_free(nctx->hg_local->hg_cl, addr_self);
  my_sz = strlen(addr) + 1;

  /* determine the max address size for local comm */
  MPI_Allreduce(&my_sz, &nctx->laddrsz, 1, MPI_INT, MPI_MAX, nctx->localcomm);
  xchg_sz = sizeof(xchg_dat_t) + nctx->laddrsz;

  /* exchange address and rank info within the local comm */
  xitm = (xchg_dat_t*)malloc(xchg_sz);
  if (!xitm) nx_fatal("malloc failed");
  xitm->grank = nctx->grank;
  xitm->idx = xitm->grank; /* use granks as address indexes */
  strcpy(xitm->addr, addr);

  xarr = (xchg_dat_t*)malloc(nctx->lsize * xchg_sz);
  if (!xarr) nx_fatal("malloc failed");

  MPI_Allgather(xitm, xchg_sz, MPI_BYTE, xarr, xchg_sz, MPI_BYTE,
                nctx->localcomm);

  /* lookup and store all mercury addresses */
  bgarg.hgctx = nctx->hg_local->hg_ctx;
  bgarg.bgdone = 0;
  bgarg.nctx = nctx;

  ret = pthread_create(&bgthread, NULL, nx_bgthread, (void*)&bgarg);
  if (ret != 0) nx_fatal("pthread_create");

  MPI_Barrier(nctx->localcomm);

  hret = nx_lookup_addrs(nctx, nctx->hg_local, xarr, nctx->lsize, nctx->laddrsz,
                         &nctx->lmap);
  if (hret != HG_SUCCESS) {
    nx_fatal("lo:HG_Addr_lookup");
  }

#ifdef NEXUS_DEBUG
  nx_dump_addrs(nctx, nctx->hg_local, &nctx->lmap);
#endif

  MPI_Barrier(nctx->localcomm);

  bgarg.bgdone = 1;
  pthread_join(bgthread, NULL);

  free(xarr);
  free(xitm);
}

/*
 * nx_setup_local: setup the local network substrate with a local-optimized
 * communication mechanism (e.g. shared memory). if the local mechanism if
 * bypassed (NEXUS_BYPASS_LOCAL), the local substrate will be built on top of
 * the remote substrate and will only be initialized partially at this point. as
 * a result, nx_setup_local_via_remote must be called later to finish the
 * remaining work after nx_discover_remote is done.
 */
void nx_setup_local(nexus_ctx_t nctx) {
  char* nx_alt_proto;
  int ret, my_sz, xchg_sz;
  char addr[TMPADDRSZ];
  xchg_dat_t *xitm, *xarr;
  pthread_t bgthread; /* network bg thread */
  struct bgthread_dat bgarg;
  hg_return_t hret;

  nx_init_localcomm(nctx);
  memset(addr, 0, sizeof(addr));
  nctx->hg_local = NULL;

  /* first, we map each local rank to its global rank */
  nctx->local2global = (int*)malloc(sizeof(int) * nctx->lsize);
  if (!nctx->local2global) nx_fatal("malloc failed");

  MPI_Allgather(&nctx->grank, 1, MPI_INT, nctx->local2global, 1, MPI_INT,
                nctx->localcomm);

  /* set the local root */
  nctx->lroot = nctx->local2global[0];

  /* next, we build the local network */
  if (nx_is_envset("NEXUS_BYPASS_LOCAL")) return;
  snprintf(addr, sizeof(addr), "na+sm://%d/0", getpid());

  /* switch to an alternate local protocol if requested */
  nx_alt_proto = getenv("NEXUS_ALT_LOCAL");
  if (nx_alt_proto && strcmp(nx_alt_proto, "na+sm") != 0)
    snprintf(addr, sizeof(addr), "%s://127.0.0.1:%d", nx_alt_proto,
             19000 + nctx->lrank);

#ifdef NEXUS_DEBUG
  fprintf(stderr, "NX-%d: LOCAL %s\n", nctx->grank, addr);
#endif

  my_sz = strlen(addr) + 1;
  nctx->hg_local = (nexus_hg_t*)malloc(sizeof(nexus_hg_t));
  memset(nctx->hg_local, 0, sizeof(nexus_hg_t));
  nctx->hg_local->refs = 1;
  nctx->hg_local->hg_cl = HG_Init(addr, HG_TRUE);
  if (!nctx->hg_local->hg_cl) {
    nx_fatal("lo:HG_Init");
  }
  nctx->hg_local->hg_ctx = HG_Context_create(nctx->hg_local->hg_cl);
  if (!nctx->hg_local->hg_ctx) {
    nx_fatal("lo:HG_Context_create");
  }

  /* determine the max address size for local comm */
  MPI_Allreduce(&my_sz, &nctx->laddrsz, 1, MPI_INT, MPI_MAX, nctx->localcomm);
  xchg_sz = sizeof(xchg_dat_t) + nctx->laddrsz;

  /* exchange address and rank info within the local comm */
  xitm = (xchg_dat_t*)malloc(xchg_sz);
  if (!xitm) nx_fatal("malloc failed");
  xitm->grank = nctx->grank;
  xitm->idx = xitm->grank; /* use granks as address indexes */
  strcpy(xitm->addr, addr);

  xarr = (xchg_dat_t*)malloc(nctx->lsize * xchg_sz);
  if (!xarr) nx_fatal("malloc failed");

  MPI_Allgather(xitm, xchg_sz, MPI_BYTE, xarr, xchg_sz, MPI_BYTE,
                nctx->localcomm);

  /* lookup and store all mercury addresses */
  bgarg.hgctx = nctx->hg_local->hg_ctx;
  bgarg.bgdone = 0;
  bgarg.nctx = nctx;

  ret = pthread_create(&bgthread, NULL, nx_bgthread, (void*)&bgarg);
  if (ret != 0) nx_fatal("pthread_create");

  MPI_Barrier(nctx->localcomm);

  hret = nx_lookup_addrs(nctx, nctx->hg_local, xarr, nctx->lsize, nctx->laddrsz,
                         &nctx->lmap);
  if (hret != HG_SUCCESS) {
    nx_fatal("lo:HG_Addr_lookup");
  }

#ifdef NEXUS_DEBUG
  nx_dump_addrs(nctx, nctx->hg_local, &nctx->lmap);
#endif

  MPI_Barrier(nctx->localcomm);

  bgarg.bgdone = 1;
  pthread_join(bgthread, NULL);

  free(xarr);
  free(xitm);
}

void nx_find_remote_addrs(nexus_ctx_t nctx, char* myaddr) {
  int my_sz, i, j, M, c, P;
  int *myinfo, *nodeinfo;
  char* rank2addr;
  xchg_dat_t *xitm, *xarr;
  hg_return_t hret;

  /* if we're alone stop here */
  if (nctx->nnodes == 1) return;
  my_sz = strlen(myaddr) + 1;
#define NADDR(x, y, s) (&x[y * s])

  /*
   * To find our peer on each node, we need to construct a list of ranks per
   * node. This is tricky with collectives if we want to support heterogeneous
   * nodes. We do this in five steps:
   * - we all-gather a rank => address array
   * - find max number of ranks per node, M
   * - local roots construct (M+1)-sized array of (# ranks, r0, r1, ...)
   * - local roots all-gather (M+1)-sized arrays (on repcomm)
   * - local roots broadcast finished array (on localcomm)
   */

  /* first, we map ranks to their string addresses */
  MPI_Allreduce(&my_sz, &nctx->gaddrsz, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
  rank2addr = (char*)malloc(nctx->gsize * nctx->gaddrsz);
  if (!rank2addr) nx_fatal("malloc failed");

  MPI_Allgather(myaddr, nctx->gaddrsz, MPI_BYTE, rank2addr, nctx->gaddrsz,
                MPI_BYTE, MPI_COMM_WORLD);
#ifdef NEXUS_DEBUG
  for (i = 0; i < nctx->gsize; i++)
    fprintf(stderr, "NX-%d: rank2addr[%d]=%s\n", nctx->grank, i,
            NADDR(rank2addr, i, nctx->gaddrsz));
#endif

  /* find the max number of ranks per node (i.e., max PPN) */
  MPI_Allreduce(&nctx->lsize, &M, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
  /* XXX: we could replace the MPI_Allreduce() above with a smaller
   * MPI_Allreduce() followed by an MPI_Bcast(), but we think MPI itself will do
   * this internally */
  nodeinfo = (int*)malloc(sizeof(int) * (M + 1) * nctx->nnodes);
  if (!nodeinfo) nx_fatal("malloc failed");

  /* prepare my info (node representatives only) */
  if (nctx->repcomm != MPI_COMM_NULL) {
    myinfo = (int*)malloc(sizeof(int) * (M + 1));
    if (!myinfo) nx_fatal("malloc failed");

    for (i = 0; i < M; i++)
      myinfo[i + 1] = (i < nctx->lsize) ? nctx->local2global[i] : -1;
    myinfo[0] = nctx->lsize;

    /* exchange node information */
    MPI_Allgather(myinfo, M + 1, MPI_INT, nodeinfo, M + 1, MPI_INT,
                  nctx->repcomm);

    free(myinfo);
  }

  /* broadcast so everyone knows it */
  MPI_Bcast(nodeinfo, (M + 1) * nctx->nnodes * sizeof(int), MPI_BYTE, 0,
            nctx->localcomm);
#ifdef NEXUS_DEBUG
  for (j = 0; j < nctx->nnodes; j++)
    for (i = 1; i < M + 1; i++)
      fprintf(stderr, "NX-%d: nodelists[%d][%d]=%d\n", nctx->grank, j, i - 1,
              nodelists[j * (M + 1) + i]);
#endif

  /*
   * Time to construct an array of peer addresses. We'll do this in two steps:
   * - find total number of peers, which is ceil(# nodes / # local ranks)
   * - construct array of peer addresses and look them all up
   */

  /* Step 1: find total number of peers */
  P = (nctx->nnodes / nctx->lsize) + ((nctx->nnodes % nctx->lsize) ? 1 : 0);

  xarr = (xchg_dat_t*)malloc(P * (sizeof(*xarr) + nctx->gaddrsz));
  if (!xarr) nx_fatal("malloc failed");

  /* Keep info on global ranks of remote reps in node2rep */
  nctx->node2rep = (int*)malloc(sizeof(int) * nctx->nnodes);
  if (!nctx->node2rep) nx_fatal("malloc failed");

  for (i = 0; i < nctx->nnodes; i++) {
    int idx = i * (M + 1);
    int remote_lsize = nodeinfo[idx];
    nctx->node2rep[i] = nodeinfo[idx + 1 + (nctx->nodeid % remote_lsize)];
  }

  /*
   * We will communicate with a node, if its node ID modulo our node's rank
   * size is equal to our local-rank. Of each node we communicate with, we
   * will msg the remote local-rank that is equal to our node's ID modulo
   * the remote node's rank size.
   */
  i = nctx->lrank;
  c = 0;
  while (i < nctx->nnodes) {
    int remote_grank, remote_lsize;
    char* remote_addr;
    int idx = i * (M + 1);

    /* skip ourselves */
    if (i == nctx->nodeid) {
      i += nctx->lsize;
      continue;
    }

    remote_lsize = nodeinfo[idx];
    remote_grank = nodeinfo[idx + 1 + (nctx->nodeid % remote_lsize)];
    remote_addr = NADDR(rank2addr, remote_grank, nctx->gaddrsz);

    xitm = (xchg_dat_t*)(((char*)xarr) + c * (sizeof(*xitm) + nctx->gaddrsz));
    strncpy(xitm->addr, remote_addr, nctx->gaddrsz);
    xitm->idx = i; /* use node id as address indexes */
    xitm->grank = remote_grank;

    i += nctx->lsize;
    c++;
  }

#undef NADDR
  /* Get rid of arrays we don't need anymore */
  free(rank2addr);
  free(nodeinfo);

  /* lookup addresses */
  if (c != 0) {
#ifdef NEXUS_DEBUG
    for (i = 0; i < c; i++) {
      xchg_dat_t* xi =
          (xchg_dat_t*)(((char*)xarr) + i * (sizeof(*xi) + nctx->gaddrsz));
      fprintf(stderr, "NX-%d: remote-peer[%d]=%d (addr=%s)\n", nctx->grank,
              xi->idx, xi->grank, xi->addr);
    }
#endif

    hret = nx_lookup_addrs(nctx, nctx->hg_remote, xarr, c, nctx->gaddrsz,
                           &nctx->rmap);
    if (hret != HG_SUCCESS) {
      nx_fatal("net:HG_Addr_lookup");
    }
  }

  free(xarr);
}

/*
 * nx_discover_remote: detect remote peers and setup the remote network
 * substrate for multi-hop routing.
 */
void nx_discover_remote(nexus_ctx_t nctx, char* hgaddr_in) {
  hg_addr_t addr_self;
  hg_size_t addr_sz;
  char addr[TMPADDRSZ];
  pthread_t bgthread; /* network bg thread */
  struct bgthread_dat bgarg;
  hg_return_t hret;
  int ret;

  nx_init_repcomm(nctx);
  MPI_Bcast(&nctx->nodeid, 1, MPI_INT, 0, nctx->localcomm);
  MPI_Bcast(&nctx->nnodes, 1, MPI_INT, 0, nctx->localcomm);

  /* build the map of global rank to node id */
  nctx->rank2node = (int*)malloc(sizeof(int) * nctx->gsize);
  if (!nctx->rank2node) nx_fatal("malloc failed");

  MPI_Allgather(&nctx->nodeid, 1, MPI_INT, nctx->rank2node, 1, MPI_INT,
                MPI_COMM_WORLD);

#ifdef NEXUS_DEBUG
  fprintf(stderr, "NX-%d: REMOTE %s\n", nctx->grank, hgaddr_in);
#endif

  nctx->hg_remote = (nexus_hg_t*)malloc(sizeof(nexus_hg_t));
  memset(nctx->hg_remote, 0, sizeof(nexus_hg_t));
  nctx->hg_remote->refs = 1;
  nctx->hg_remote->hg_cl = HG_Init(hgaddr_in, HG_TRUE);
  if (!nctx->hg_remote->hg_cl) {
    nx_fatal("net:HG_Init");
  }
  nctx->hg_remote->hg_ctx = HG_Context_create(nctx->hg_remote->hg_cl);
  if (!nctx->hg_remote->hg_ctx) {
    nx_fatal("net:HG_Context_create");
  }

  /* fetch self-address */
  hret = HG_Addr_self(nctx->hg_remote->hg_cl, &addr_self);
  if (hret != HG_SUCCESS) {
    nx_fatal("net:HG_Addr_self");
  }
  addr_sz = sizeof(addr);
  hret = HG_Addr_to_string(nctx->hg_remote->hg_cl, addr, &addr_sz, addr_self);
  if (hret != HG_SUCCESS) {
    nx_fatal("net:HG_Addr_to_string");
  }
  HG_Addr_free(nctx->hg_remote->hg_cl, addr_self);

  /* lookup and store mercury addresses */
  if (nctx->nnodes != 1) { /* only do it when we aren't alone */
    bgarg.hgctx = nctx->hg_remote->hg_ctx;
    bgarg.bgdone = 0;
    bgarg.nctx = nctx;

    ret = pthread_create(&bgthread, NULL, nx_bgthread, (void*)&bgarg);
    if (ret != 0) nx_fatal("pthread_create");

    MPI_Barrier(MPI_COMM_WORLD);

    nx_find_remote_addrs(nctx, addr);
#ifdef NEXUS_DEBUG
    nx_dump_addrs(nctx, nctx->hg_remote, &nctx->rmap);
#endif

    MPI_Barrier(MPI_COMM_WORLD);
    bgarg.bgdone = 1;

    pthread_join(bgthread, NULL);
  }
}

nexus_ctx_t nx_bootstrap_internal(char* uri, char* subnet, char* proto) {
  nexus_ctx_t nctx;
  char addr[TMPADDRSZ]; /* server uri buffer */
  char* env;

  nctx = new nexus_ctx;

  env = getenv("NEXUS_LOOKUP_LIMIT");
  if (env == NULL || env[0] == 0) {
    nctx->nx_limit = DEFAULT_NX_LIMIT;
  } else {
    nctx->nx_limit = atoi(env);
    if (nctx->nx_limit <= 0) {
      nctx->nx_limit = 1;
    }
  }

  nctx->localcomm = MPI_COMM_NULL;
  nctx->repcomm = MPI_COMM_NULL;
  nctx->rank2node = NULL;
  nctx->local2global = NULL;
  nctx->node2rep = NULL;

  MPI_Comm_rank(MPI_COMM_WORLD, &nctx->grank);
  MPI_Comm_size(MPI_COMM_WORLD, &nctx->gsize);

  nx_setup_local(nctx);

  if (!nctx->grank)
    fprintf(stdout, "NX: LOCAL %s (NX-LIMIT=%d)\n",
            nctx->hg_local ? "DONE" : "VIA REMOTE", nctx->nx_limit);

  if (!uri) {
    nx_prepare_addr(nctx, subnet, proto, addr);
    uri = addr;
  }
  nx_discover_remote(nctx, uri);
  if (!nctx->hg_local) {
    nx_setup_local_via_remote(nctx);
  }

  if (!nctx->grank) fprintf(stdout, "NX: REMOTE DONE\n");

#ifdef NEXUS_DEBUG
  fprintf(stderr, "NX-%d: ALL DONE\n", nctx->grank);
#endif

  return nctx;
}
}  // namespace

nexus_ctx_t nexus_bootstrap_uri(char* uri) {
  return nx_bootstrap_internal(uri, NULL, NULL);
}

nexus_ctx_t nexus_bootstrap(char* subnet, char* proto) {
  return nx_bootstrap_internal(NULL, subnet, proto);
}

void nexus_destroy(nexus_ctx_t nctx) {
  nexus_map_t::iterator it;

  for (it = nctx->lmap.begin(); it != nctx->lmap.end(); ++it) {
    if (it->second != HG_ADDR_NULL) {
      HG_Addr_free(nctx->hg_local->hg_cl, it->second);
    }
  }

  MPI_Barrier(nctx->localcomm);
  MPI_Comm_free(&nctx->localcomm);
  assert(nctx->hg_local->refs > 0);
  nctx->hg_local->refs--;
  if (nctx->hg_local->refs == 0) {
    HG_Context_destroy(nctx->hg_local->hg_ctx);
    HG_Finalize(nctx->hg_local->hg_cl);
    free(nctx->hg_local);
  }

  for (it = nctx->rmap.begin(); it != nctx->rmap.end(); ++it) {
    if (it->second != HG_ADDR_NULL) {
      HG_Addr_free(nctx->hg_remote->hg_cl, it->second);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  if (nctx->repcomm != MPI_COMM_NULL) {
    MPI_Comm_free(&nctx->repcomm);
  }
  assert(nctx->hg_remote->refs > 0);
  nctx->hg_remote->refs--;
  if (nctx->hg_remote->refs == 0) {
    HG_Context_destroy(nctx->hg_remote->hg_ctx);
    HG_Finalize(nctx->hg_remote->hg_cl);
    free(nctx->hg_remote);
  }

  if (nctx->node2rep) free(nctx->node2rep);
  free(nctx->local2global);
  free(nctx->rank2node);
  delete nctx;
}

hg_context_t* nexus_hgcontext_local(nexus_ctx_t nctx) {
  return nctx->hg_local->hg_ctx;
}

hg_class_t* nexus_hgclass_local(nexus_ctx_t nctx) {
  return nctx->hg_local->hg_cl;
}

hg_context_t* nexus_hgcontext_remote(nexus_ctx_t nctx) {
  return nctx->hg_remote->hg_ctx;
}

hg_class_t* nexus_hgclass_remote(nexus_ctx_t nctx) {
  return nctx->hg_remote->hg_cl;
}
