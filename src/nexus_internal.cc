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

#define TMPADDRSZ 64

namespace {
typedef struct {
  int grank;
  int idx;
  char addr[];
} xchg_dat_t;

typedef struct {
  hg_context_t* hgctx;
  int bgdone;
} bgthread_dat_t;

/*
 * network support pthread. Need to call progress to push the network and then
 * trigger to run the callback.
 */
void* nx_bgthread(void* arg) {
  bgthread_dat_t* bgdat = (bgthread_dat_t*)arg;
  hg_return_t hret;

#ifdef NEXUS_DEBUG
  fprintf(stdout, "Network thread running\n");
#endif

  /* while (not done sending or not done recving */
  while (!bgdat->bgdone) {
    unsigned int count = 0;

    do {
      hret = HG_Trigger(bgdat->hgctx, 0, 1, &count);
    } while (hret == HG_SUCCESS && count);

    if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
      nx_fatal("nexus_bgthread: HG_Trigger failed");

    hret = HG_Progress(bgdat->hgctx, 100);
    if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
      nx_fatal("nexus_bgthread: HG_Progress failed");
  }

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
      n = bind(so, (struct sockaddr*)&addr, sizeof(addr));
      if (n == 0) break;
    }
    close(so);
    if (n != 0) nx_fatal("bmi:socket2");

    sprintf(uri, "%s://%s:%d", proto, ip, port);

  } else {
    /* set port 0, let OS fill it, collect later w/HG_Addr_to_string */
    sprintf(uri, "%s://%s:0", proto, ip);
  }

#ifdef NEXUS_DEBUG
  fprintf(stdout, "Info: Using address %s\n", uri);
#endif
}

struct hg_lookup_ctx {
  /* The address map we'll be updating */
  nexus_map_t* map;

  /* State to track progress */
  int count;
  pthread_mutex_t cb_mutex;
  pthread_cond_t cb_cv;
} lkp;

typedef struct hg_lookup_out {
  hg_return_t hret;
  int idx;
} hg_lookup_out_t;

hg_return_t nx_lookup_cb(const struct hg_cb_info* info) {
  hg_lookup_out_t* out = (hg_lookup_out_t*)info->arg;
  out->hret = info->ret;

  pthread_mutex_lock(&lkp.cb_mutex);

  /* Add address to map */
  if (out->hret != HG_SUCCESS)
    (*(lkp.map))[out->idx] = HG_ADDR_NULL;
  else
    (*(lkp.map))[out->idx] = info->info.lookup.addr;

  lkp.count += 1;
  pthread_cond_signal(&lkp.cb_cv);
  pthread_mutex_unlock(&lkp.cb_mutex);

  return HG_SUCCESS;
}

hg_return_t nx_lookup_addrs(nexus_ctx_t nctx, hg_context_t* hgctx,
                            hg_class_t* hgcl, xchg_dat_t* xarr, int xsize,
                            int addrsz, nexus_map_t* map) {
  hg_lookup_out_t* out = NULL;
  hg_return_t hret;
  hg_addr_t self_addr;
  int cur_i, eff_size;

  lkp.count = 0;
  lkp.map = map;

  out = (hg_lookup_out_t*)malloc(sizeof(*out) * xsize);
  if (out == NULL) return HG_NOMEM_ERROR;

  pthread_mutex_init(&lkp.cb_mutex, NULL);
  pthread_cond_init(&lkp.cb_cv, NULL);

  pthread_mutex_lock(&lkp.cb_mutex);

  cur_i = 0;
send_again:
  eff_size = (((xsize - cur_i) < NEXUS_LOOKUP_LIMIT) ? (xsize - cur_i)
                                                     : NEXUS_LOOKUP_LIMIT);
  /* Post a batch of lookups */
  for (int i = 0; i < eff_size; i++) {
    int eff_i = ((nctx->grank + i) % eff_size) + cur_i;
    xchg_dat_t* xi =
        (xchg_dat_t*)(((char*)xarr) + eff_i * (sizeof(*xi) + addrsz));

    /* Populate out struct */
    out[eff_i].hret = HG_SUCCESS;
    out[eff_i].idx = xi->idx;

#ifdef NEXUS_DEBUG
    fprintf(stderr, "[%d] Idx %d: addr %s, idx %d, grank %d (xsize = %d)\n",
            nctx->grank, eff_i, xi->addr, xi->idx, xi->grank, xsize);
#endif

    if (xi->grank != nctx->grank) {
      hret = HG_Addr_lookup(hgctx, &nx_lookup_cb, &out[eff_i], xi->addr,
                            HG_OP_ID_IGNORE);
    } else {
      hret = HG_Addr_self(hgcl, &self_addr);

      /* Add address to map */
      if (hret != HG_SUCCESS)
        (*(lkp.map))[xi->idx] = HG_ADDR_NULL;
      else
        (*(lkp.map))[xi->idx] = self_addr;

      lkp.count += 1;
    }

    if (hret != HG_SUCCESS) {
      pthread_mutex_unlock(&lkp.cb_mutex);
      goto err;
    }
  }

  cur_i += eff_size;

/* Lookup posted, wait until finished */
wait_again:
  if (lkp.count < cur_i) {
    pthread_cond_wait(&lkp.cb_cv, &lkp.cb_mutex);
    if (lkp.count < cur_i) goto wait_again;
  }
  pthread_mutex_unlock(&lkp.cb_mutex);

  if (cur_i < xsize) goto send_again;

  hret = HG_SUCCESS;
  for (int i = 0; i < xsize; i++) {
    if (out[i].hret != HG_SUCCESS) {
      hret = out[i].hret;
      goto err;
    }
  }

err:
  pthread_cond_destroy(&lkp.cb_cv);
  pthread_mutex_destroy(&lkp.cb_mutex);
  free(out);
  return hret;
}

/*
 * nx_dump_addrs: dump a given map to stderr.
 */
void nx_dump_addrs(nexus_ctx_t nctx, hg_class_t* hgcl, const char* map_name,
                   nexus_map_t* map) {
  char addr[TMPADDRSZ];
  hg_size_t addr_sz = 0;
  hg_return_t hret;

  nexus_map_t::iterator it = map->begin();
  for (; it != map->end(); ++it) {
    addr_sz = sizeof(addr);
    hret = HG_Addr_to_string(hgcl, addr, &addr_sz, it->second);
    if (hret != HG_SUCCESS) {
      strcpy(addr, "n/a");
    }
    fprintf(stderr, "NX-%d: %s[%d]=%s\n", nctx->grank, map_name, it->first,
            addr);
  }
}

/*
 * nx_setup_local_via_remote: setup the local network substrate by reusing the
 * remote substrate. must be called after nx_setup_local and nx_discover_remote.
 */
void nx_setup_local_via_remote(nexus_ctx_t nctx) {
  int ret, buf_sz;
  hg_addr_t addr_self;
  hg_size_t addr_sz;
  char addr[TMPADDRSZ];
  hg_return_t hret;
  xchg_dat_t *xitm, *xarr;
  pthread_t bgthread; /* network bg thread */
  bgthread_dat_t bgarg;

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

  /* determine the max address size for local comm */
  buf_sz = strlen(addr) + 1;
  MPI_Allreduce(&buf_sz, &nctx->laddrsz, 1, MPI_INT, MPI_MAX, nctx->localcomm);
  buf_sz = sizeof(xchg_dat_t) + nctx->laddrsz;

  /* exchange addresses, global, and local ranks in local comm */
  xitm = (xchg_dat_t*)malloc(buf_sz);
  if (!xitm) nx_fatal("malloc failed");
  xitm->grank = nctx->grank;
  xitm->idx = nctx->lrank;
  strcpy(xitm->addr, addr);

  xarr = (xchg_dat_t*)malloc(nctx->lsize * buf_sz);
  if (!xarr) nx_fatal("malloc failed");

  assert(nctx->local2global != NULL);

  MPI_Allgather(xitm, buf_sz, MPI_BYTE, xarr, buf_sz, MPI_BYTE,
                nctx->localcomm);

  /* verify the map of local to global ranks, also verify the local root */
  for (int i = 0; i < nctx->lsize; i++) {
    xchg_dat_t* xi = (xchg_dat_t*)(((char*)xarr) + i * buf_sz);

    assert(nctx->local2global[xi->idx] == xi->grank);
    if (xi->idx == 0) assert(nctx->lroot == xi->grank);

    /* for lookups our index is the grank */
    xi->idx = xi->grank;
  }

  /* pre-lookup and cache all mercury addresses */
  if (nctx->hg_local != NULL) {
    bgarg.hgctx = nctx->hg_local->hg_ctx;
    bgarg.bgdone = 0;

    ret = pthread_create(&bgthread, NULL, nx_bgthread, (void*)&bgarg);
    if (ret != 0) nx_fatal("pthread_create");

    MPI_Barrier(nctx->localcomm);

    hret = nx_lookup_addrs(nctx, nctx->hg_local->hg_ctx, nctx->hg_local->hg_cl,
                           xarr, nctx->lsize, nctx->laddrsz, &nctx->laddrs);
    if (hret != HG_SUCCESS) {
      nx_fatal("lo:HG_Addr_lookup");
    }

#ifdef NEXUS_DEBUG
    nx_dump_addrs(nctx, nctx->hg_local->hg_cl, "lmap", &nctx->laddrs);
#endif

    MPI_Barrier(nctx->localcomm);

    bgarg.bgdone = 1;
    pthread_join(bgthread, NULL);
  }

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
  int ret, buf_sz;
  char addr[TMPADDRSZ];
  xchg_dat_t *xitm, *xarr;
  hg_return_t hret;
  pthread_t bgthread; /* network bg thread */
  bgthread_dat_t bgarg;

  nx_init_localcomm(nctx);
  MPI_Comm_rank(nctx->localcomm, &nctx->lrank);
  MPI_Comm_size(nctx->localcomm, &nctx->lsize);
  memset(addr, 0, sizeof(addr));
  nctx->hg_local = NULL;

  if (!nx_is_envset("NEXUS_BYPASS_LOCAL")) {
    /* use an alternate local protocol if requested */
    nx_alt_proto = getenv("NEXUS_ALT_LOCAL");
    if (nx_alt_proto && strcmp(nx_alt_proto, "na+sm") != 0) {
      snprintf(addr, sizeof(addr), "%s://127.0.0.1:%d", nx_alt_proto,
               19000 + nctx->lrank);
    } else {
      snprintf(addr, sizeof(addr), "na+sm://%d/0", getpid());
    }

#ifdef NEXUS_DEBUG
    fprintf(stderr, "NX-%d: LO %s\n", nctx->grank, addr);
#endif

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
  }

  /* determine the max address size for local comm */
  buf_sz = strlen(addr) + 1;
  MPI_Allreduce(&buf_sz, &nctx->laddrsz, 1, MPI_INT, MPI_MAX, nctx->localcomm);
  buf_sz = sizeof(xchg_dat_t) + nctx->laddrsz;

  /* exchange addresses, global, and local ranks in local comm */
  xitm = (xchg_dat_t*)malloc(buf_sz);
  if (!xitm) nx_fatal("malloc failed");
  xitm->grank = nctx->grank;
  xitm->idx = nctx->lrank;
  strcpy(xitm->addr, addr);

  xarr = (xchg_dat_t*)malloc(nctx->lsize * buf_sz);
  if (!xarr) nx_fatal("malloc failed");

  nctx->local2global = (int*)malloc(sizeof(int) * nctx->lsize);
  if (!nctx->local2global) nx_fatal("malloc failed");

  MPI_Allgather(xitm, buf_sz, MPI_BYTE, xarr, buf_sz, MPI_BYTE,
                nctx->localcomm);

  /* build the map of local to global ranks, also decide the local root */
  for (int i = 0; i < nctx->lsize; i++) {
    xchg_dat_t* xi = (xchg_dat_t*)(((char*)xarr) + i * buf_sz);

    nctx->local2global[xi->idx] = xi->grank;
    if (xi->idx == 0) nctx->lroot = xi->grank;

    /* for lookups our index is the grank */
    xi->idx = xi->grank;
  }

  /* pre-lookup and cache all mercury addresses */
  if (nctx->hg_local != NULL) {
    bgarg.hgctx = nctx->hg_local->hg_ctx;
    bgarg.bgdone = 0;

    ret = pthread_create(&bgthread, NULL, nx_bgthread, (void*)&bgarg);
    if (ret != 0) nx_fatal("pthread_create");

    MPI_Barrier(nctx->localcomm);

    hret = nx_lookup_addrs(nctx, nctx->hg_local->hg_ctx, nctx->hg_local->hg_cl,
                           xarr, nctx->lsize, nctx->laddrsz, &nctx->laddrs);
    if (hret != HG_SUCCESS) {
      nx_fatal("lo:HG_Addr_lookup");
    }

#ifdef NEXUS_DEBUG
    nx_dump_addrs(nctx, nctx->hg_local->hg_cl, "lmap", &nctx->laddrs);
#endif

    MPI_Barrier(nctx->localcomm);

    bgarg.bgdone = 1;
    pthread_join(bgthread, NULL);
  }

  free(xarr);
  free(xitm);
}

#define NADDR(x, y, s) (&x[y * s])

void nx_find_remote_addrs(nexus_ctx_t nctx, char* myaddr) {
  int i, npeers, maxnodesz, maxpeers;
  int *msgdata, *nodelists;
  char* rank2addr;
  xchg_dat_t* paddrs;
  hg_return_t hret;

  /* If we're alone stop here */
  if (nctx->nodesz == 1) return;

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

  /* Step 1: build rank => address array */
  i = strlen(myaddr) + 1;
  MPI_Allreduce(&i, &nctx->gaddrsz, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

  rank2addr = (char*)malloc(nctx->gsize * nctx->gaddrsz);
  if (!rank2addr) nx_fatal("malloc failed");

  MPI_Allgather(myaddr, nctx->gaddrsz, MPI_BYTE, rank2addr, nctx->gaddrsz,
                MPI_BYTE, MPI_COMM_WORLD);
#ifdef NEXUS_DEBUG
  for (i = 0; i < nctx->gsize; i++)
    fprintf(stderr, "[%d] rank2addr[%d] = %s\n", nctx->grank, i,
            NADDR(rank2addr, i, nctx->gaddrsz));
#endif

  /* Step 2: find max number of ranks per node */
  MPI_Allreduce(&nctx->lsize, &maxnodesz, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

  nodelists = (int*)malloc(sizeof(int) * (maxnodesz + 1) * nctx->nodesz);
  if (!nodelists) nx_fatal("malloc failed");

  if (nctx->repcomm == MPI_COMM_NULL) goto nonroot;

  /* Step 3: construct local node list */
  msgdata = (int*)malloc(sizeof(int) * (maxnodesz + 1));
  if (!msgdata) nx_fatal("malloc failed");

  i = 1;
  msgdata[0] = nctx->lsize;
  for (nexus_map_t::iterator it = nctx->laddrs.begin();
       it != nctx->laddrs.end(); it++)
    msgdata[i++] = it->first;

  /* Step 4: exchange local node lists */
  MPI_Allgather(msgdata, maxnodesz + 1, MPI_INT, nodelists, maxnodesz + 1,
                MPI_INT, nctx->repcomm);

  free(msgdata);
nonroot:
  /* Step 5: broadcast finished node lists array */
  MPI_Bcast(nodelists, (maxnodesz + 1) * nctx->nodesz * sizeof(int), MPI_BYTE,
            0, nctx->localcomm);
#ifdef NEXUS_DEBUG
  for (i = 0; i < ((maxnodesz + 1) * nctx->nodesz); i++)
    fprintf(stderr, "[%d] nodelists[%d] = %d\n", nctx->grank, i, nodelists[i]);
#endif

  /*
   * Time to construct an array of peer addresses. We'll do this in two steps:
   * - find total number of peers, which is ceil(# nodes / # local ranks)
   * - construct array of peer addresses and look them all up
   */

  /* Step 1: find total number of peers */
  maxpeers =
      (nctx->nodesz / nctx->lsize) + ((nctx->nodesz % nctx->lsize) ? 1 : 0);

  paddrs = (xchg_dat_t*)malloc(maxpeers * (sizeof(*paddrs) + nctx->gaddrsz));
  if (!paddrs) nx_fatal("malloc failed");

  /* Keep info on global ranks of remote reps in node2rep */
  nctx->node2rep = (int*)malloc(sizeof(int) * nctx->nodesz);
  if (!nctx->node2rep) nx_fatal("malloc failed");

  for (i = 0; i < nctx->nodesz; i++) {
    int idx = i * (maxnodesz + 1);
    int remote_lsize = nodelists[idx];
    nctx->node2rep[i] = nodelists[idx + 1 + (nctx->nodeid % remote_lsize)];
  }

  /*
   * We will communicate with a node, if its node ID modulo our node's rank
   * size is equal to our local-rank. Of each node we communicate with, we
   * will msg the remote local-rank that is equal to our node's ID modulo
   * the remote node's rank size.
   */
  i = nctx->lrank;
  npeers = 0;
  while (i < nctx->nodesz) {
    int idx = i * (maxnodesz + 1);
    int remote_grank, remote_lsize;
    char* remote_addr;
    xchg_dat_t* ppeer;

    /* Skip ourselves */
    if (i == nctx->nodeid) {
      i += nctx->lsize;
      continue;
    }

    remote_lsize = nodelists[idx];
    remote_grank = nodelists[idx + 1 + (nctx->nodeid % remote_lsize)];
    remote_addr = NADDR(rank2addr, remote_grank, nctx->gaddrsz);

    ppeer = (xchg_dat_t*)(((char*)paddrs) +
                          npeers * (sizeof(*ppeer) + nctx->gaddrsz));
    strncpy(ppeer->addr, remote_addr, nctx->gaddrsz);
    ppeer->idx = i; /* we index by node ID */
    ppeer->grank = remote_grank;

    i += nctx->lsize;
    npeers++;
  }

  /* Get rid of arrays we don't need anymore */
  free(rank2addr);
  free(nodelists);

  /* No peers? Stop here. */
  if (!npeers) {
    free(paddrs);
    return;
  }

#ifdef NEXUS_DEBUG
  for (i = 0; i < npeers; i++) {
    xchg_dat_t* ppeer =
        (xchg_dat_t*)(((char*)paddrs) + i * (sizeof(*ppeer) + nctx->gaddrsz));

    fprintf(stderr, "[%d] i = %d, idx = %d, grank = %d, addr = %s\n",
            nctx->grank, i, ppeer->idx, ppeer->grank, ppeer->addr);
  }
#endif

  /* Step 2: lookup peer addresses */
  hret = nx_lookup_addrs(nctx, nctx->hg_remote->hg_ctx, nctx->hg_remote->hg_cl,
                         paddrs, npeers, nctx->gaddrsz, &nctx->gaddrs);
  if (hret != HG_SUCCESS) {
    nx_fatal("lookup_addrs failed");
  }

#ifdef NEXUS_DEBUG
  fprintf(stderr, "NX-%d: npeers=%d\n", nctx->grank, npeers);
#endif

  free(paddrs);
}

/*
 * nx_discover_remote: detect remote peers and setup the remote network
 * substrate for multi-hop routing.
 */
void nx_discover_remote(nexus_ctx_t nctx, char* hgaddr_in) {
  int ret;
  hg_addr_t addr_self;
  hg_size_t addr_sz;
  char addr[TMPADDRSZ];
  hg_return_t hret;
  pthread_t bgthread; /* network bg thread */
  bgthread_dat_t bgarg;

  nx_init_repcomm(nctx);
  if (nctx->repcomm != MPI_COMM_NULL) {
    MPI_Comm_rank(nctx->repcomm, &nctx->nodeid);
    MPI_Comm_size(nctx->repcomm, &nctx->nodesz);
  } else {
    assert(nctx->grank != nctx->lroot);
  }

  MPI_Bcast(&nctx->nodeid, 1, MPI_INT, 0, nctx->localcomm);
  MPI_Bcast(&nctx->nodesz, 1, MPI_INT, 0, nctx->localcomm);

  /* build the map of global rank to node id */
  nctx->rank2node = (int*)malloc(sizeof(int) * nctx->gsize);
  if (!nctx->rank2node) nx_fatal("malloc failed");

  MPI_Allgather(&nctx->nodeid, 1, MPI_INT, nctx->rank2node, 1, MPI_INT,
                MPI_COMM_WORLD);

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

  /* pre-lookup and cache mercury addresses */
  if (nctx->hg_remote != NULL) {
    bgarg.hgctx = nctx->hg_remote->hg_ctx;
    bgarg.bgdone = 0;

    ret = pthread_create(&bgthread, NULL, nx_bgthread, (void*)&bgarg);
    if (ret != 0) nx_fatal("pthread_create");

    MPI_Barrier(MPI_COMM_WORLD);

    nx_find_remote_addrs(nctx, addr);
#ifdef NEXUS_DEBUG
    nx_dump_addrs(nctx, nctx->hg_remote->hg_cl, "rmap", &nctx->gaddrs);
#endif

    MPI_Barrier(MPI_COMM_WORLD);
    bgarg.bgdone = 1;

    pthread_join(bgthread, NULL);
  }
}

nexus_ctx_t nx_bootstrap_internal(char* uri, char* subnet, char* proto) {
  nexus_ctx_t nctx = NULL;
  char addr[TMPADDRSZ]; /* server uri buffer */

  nctx = new nexus_ctx;

  MPI_Comm_rank(MPI_COMM_WORLD, &nctx->grank);
  MPI_Comm_size(MPI_COMM_WORLD, &nctx->gsize);

  nx_setup_local(nctx);

  if (!nctx->grank) fprintf(stdout, "NX: LOCAL DONE\n");

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
  fprintf(stdout, "[%d] grank = %d, lrank = %d, gsize = %d, lsize = %d\n",
          nctx->grank, nctx->grank, nctx->lrank, nctx->gsize, nctx->lsize);
#endif /* NEXUS_DEBUG */

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

  for (it = nctx->laddrs.begin(); it != nctx->laddrs.end(); it++) {
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

  for (it = nctx->gaddrs.begin(); it != nctx->gaddrs.end(); it++) {
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
