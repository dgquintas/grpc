/*
 *
 * Copyright 2016, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include "src/core/ext/client_config/lb_policy_factory.h"
#include "src/core/ext/client_config/lb_policy_registry.h"
#include "src/core/ext/client_config/resolver_registry.h"
#include "src/core/ext/lb_policy/grpclb/grpclb.h"
#include "src/core/ext/lb_policy/grpclb/load_balancer_api.h"

#include <string.h>

#include <grpc/grpc.h>
#include <grpc/byte_buffer.h>
#include <grpc/byte_buffer_reader.h>
#include <grpc/support/alloc.h>
#include <grpc/support/string_util.h>
#include <grpc/support/host_port.h>
#include <stdio.h>

#define GRPC_TIMEOUT_SECONDS_TO_DEADLINE(x)  \
  gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC), \
               gpr_time_from_seconds((long)((x)), GPR_TIMESPAN))

typedef struct pending_pick {
  struct pending_pick *next;
  grpc_pollset *pollset;
  grpc_metadata_batch *initial_metadata;
  grpc_connected_subchannel **target;
  grpc_closure *on_complete;
} pending_pick;

static int parse_ipv4(const char *host_port, struct sockaddr_storage *addr,
                      size_t *len) {
  char *host;
  char *port;
  int port_num;
  int result = 0;
  struct sockaddr_in *in = (struct sockaddr_in *)addr;

  if (*host_port == '/') ++host_port;
  if (!gpr_split_host_port(host_port, &host, &port)) {
    return 0;
  }

  memset(in, 0, sizeof(*in));
  *len = sizeof(*in);
  in->sin_family = AF_INET;
  if (inet_pton(AF_INET, host, &in->sin_addr) == 0) {
    gpr_log(GPR_ERROR, "invalid ipv4 address: '%s'", host);
    goto done;
  }

  if (port != NULL) {
    if (sscanf(port, "%d", &port_num) != 1 || port_num < 0 ||
        port_num > 65535) {
      gpr_log(GPR_ERROR, "invalid ipv4 port: '%s'", port);
      goto done;
    }
    in->sin_port = htons((uint16_t)port_num);
  } else {
    gpr_log(GPR_ERROR, "no port given for ipv4 scheme");
    goto done;
  }

  result = 1;
done:
  gpr_free(host);
  gpr_free(port);
  return result;
}

static grpc_lb_policy *create_rr(grpc_exec_ctx *exec_ctx,
                                 grpc_grpclb_serverlist *serverlist,
                                 grpc_client_channel_factory *sc_factory) {
  grpc_subchannel **subchannels;
  size_t num_addrs = serverlist->num_servers;
  struct sockaddr_storage *addrs =
      gpr_malloc(sizeof(struct sockaddr_storage) * num_addrs);
  size_t *addrs_len = gpr_malloc(sizeof(*addrs_len) * num_addrs);

  size_t i;
  char *hostport;
  int errors_found = 0;
  grpc_subchannel_args args;
  grpc_lb_policy_args rr_policy_args;

  if (serverlist == NULL) {
    return NULL;
  }

  for (i = 0; i < num_addrs; i++) {
    gpr_asprintf(&hostport, "%s:%d", serverlist->servers[i]->ip_address,
                 serverlist->servers[i]->port);
    if (!parse_ipv4(hostport, &addrs[i], &addrs_len[i])) {
      errors_found = 1;
    }
    gpr_free(hostport);
    if (errors_found) break;
  }

  subchannels = gpr_malloc(sizeof(grpc_subchannel *) * num_addrs);
  for (i = 0; i < num_addrs; i++) {
    memset(&args, 0, sizeof(args));
    args.addr = (struct sockaddr *)(&addrs[i]);
    args.addr_len = addrs_len[i];
    subchannels[i] =
        grpc_subchannel_factory_create_subchannel(exec_ctx, sc_factory, &args);
  }

  rr_policy_args.num_subchannels = num_addrs;
  rr_policy_args.subchannels = subchannels;
  return grpc_lb_policy_create("round_robin", &rr_policy_args);
}

typedef struct {
  /** base policy: must be first */
  grpc_lb_policy base;
  /** all our subchannels */
  grpc_subchannel **subchannels;
  size_t num_subchannels;

  /** mutex protecting remaining members */
  gpr_mu mu;

  /** are we shut down? */
  int shutdown;

  /** list of picks that are waiting on connectivity */
  pending_pick *pending_picks;

  grpc_lb_policy *pick_first;
  grpc_connected_subchannel *pf_pick; /* the subchannel picked by pick_fist */
  grpc_closure pf_pick_cb;  /* invoked for all pf action */
  grpc_connectivity_state pf_conn_state;
  grpc_closure pf_conn_state_changed_cb;

  grpc_client_channel_factory *client_channel_factory;
} glb_lb_policy;


void glb_destroy(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol) {
  glb_lb_policy *p = (glb_lb_policy *)pol;
  GRPC_LB_POLICY_UNREF(exec_ctx, p->pick_first, "glb_destroy");
  GPR_ASSERT(p->pending_picks == NULL);
  gpr_mu_destroy(&p->mu);
  gpr_free(p);
}

void glb_shutdown(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol) {
  glb_lb_policy *p = (glb_lb_policy *)pol;
  pending_pick *pp;
  gpr_mu_lock(&p->mu);
  p->shutdown = 1;
  pp = p->pending_picks;
  p->pending_picks = NULL;
  gpr_mu_unlock(&p->mu);
  while (pp != NULL) {
    pending_pick *next = pp->next;
    *pp->target = NULL;
    grpc_exec_ctx_enqueue(exec_ctx, pp->on_complete, true);
    gpr_free(pp);
    pp = next;
  }
}

static void glb_cancel_pick(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol,
                            grpc_connected_subchannel **target) {
  glb_lb_policy *p = (glb_lb_policy *)pol;
  gpr_mu_lock(&p->mu);
  grpc_lb_policy_cancel_pick(exec_ctx, p->pick_first, target);
  /* XXX: do we also need to cancel RR? */
  gpr_mu_unlock(&p->mu);
}

void glb_exit_idle(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol) {
  glb_lb_policy *p = (glb_lb_policy *)pol;
  gpr_mu_lock(&p->mu);
  grpc_lb_policy_exit_idle(exec_ctx, p->pick_first);
  gpr_mu_unlock(&p->mu);
}

static void add_pending_pick(pending_pick **root, grpc_pollset *pollset,
                             grpc_metadata_batch *initial_metadata,
                             grpc_connected_subchannel **target,
                             grpc_closure *on_complete) {
  pending_pick *pp = gpr_malloc(sizeof(*pp));
  pp->next = *root;
  pp->pollset = pollset;
  pp->target = target;
  pp->initial_metadata = initial_metadata;
  pp->on_complete = on_complete;
  *root = pp;
}

int glb_pick(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol,
             grpc_pollset *pollset, grpc_metadata_batch *initial_metadata,
             grpc_connected_subchannel **target, grpc_closure *on_complete) {
  glb_lb_policy *p = (glb_lb_policy *)pol;
  gpr_mu_lock(&p->mu);

  /* save a reference to the pick's data. It may be needed for the potential
   * RR
   * pick */
  add_pending_pick(&p->pending_picks, pollset, initial_metadata, target,
                   on_complete);

  /* get the first input subchannel that connects into p->pf_pick */
  grpc_lb_policy_pick(exec_ctx, p->pick_first, pollset, initial_metadata,
                      &p->pf_pick /* target */,
                      &p->pf_pick_cb /* pf_pick_cb(pfpa) */);
  gpr_mu_unlock(&p->mu);
  return 0; /* picking is always delayed */
}

static grpc_connectivity_state glb_check_connectivity(grpc_exec_ctx *exec_ctx,
                                                      grpc_lb_policy *pol) {
  glb_lb_policy *p = (glb_lb_policy *)pol;
  grpc_connectivity_state st;
  gpr_mu_lock(&p->mu);
  st = grpc_lb_policy_check_connectivity(exec_ctx, p->pick_first);
  gpr_mu_unlock(&p->mu);
  return st;
}

static void glb_ping_one(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol,
                        grpc_closure *closure) {
  glb_lb_policy *p = (glb_lb_policy *)pol;

  /* get the LB stream for the current pick */
  lb_stream *lbs = pts_find(p->pts, p->pf_pick);
  if (lbs == NULL) { /* not an LB server */
    grpc_lb_policy_ping_one(exec_ctx, p->pick_first, closure);
  } else { /* ping over the round robin from the LB server */
    grpc_lb_policy_ping_one(exec_ctx, lbs->rr, closure);
  }
}

void glb_notify_on_state_change(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol,
                                grpc_connectivity_state *current,
                                grpc_closure *notify) {
  glb_lb_policy *p = (glb_lb_policy *)pol;
  gpr_mu_lock(&p->mu);
  grpc_lb_policy_notify_on_state_change(exec_ctx, p->pick_first, current,
                                        notify);
  gpr_mu_unlock(&p->mu);
}

static const grpc_lb_policy_vtable glb_lb_policy_vtable = {
    glb_destroy, glb_shutdown, glb_pick, glb_cancel_pick, glb_ping_one, glb_exit_idle,
    glb_check_connectivity, glb_notify_on_state_change};

static void glb_factory_ref(grpc_lb_policy_factory *factory) {}

static void glb_factory_unref(grpc_lb_policy_factory *factory) {}

static grpc_lb_policy *glb_create(grpc_lb_policy_factory *factory,
                                  grpc_lb_policy_args *args) {
  grpc_lb_policy_args pf_args;
  glb_lb_policy *p = gpr_malloc(sizeof(*p));
  GPR_ASSERT(args->num_subchannels > 0);
  memset(p, 0, sizeof(*p));
  grpc_lb_policy_init(&p->base, &glb_lb_policy_vtable);
  p->subchannels =
      gpr_malloc(sizeof(grpc_subchannel *) * args->num_subchannels);
  p->num_subchannels = args->num_subchannels;
  memcpy(p->subchannels, args->subchannels,
         sizeof(grpc_subchannel *) * args->num_subchannels);

  /* Create the pick first policy that'll choose the subchannel to probe for
   * LB
   * support, or return as the actual pick if no LB service exists. */
  memset(&pf_args, 0, sizeof(grpc_lb_policy_args));
  pf_args.subchannels = args->subchannels;
  pf_args.num_subchannels = args->num_subchannels;
  p->pick_first = grpc_lb_policy_create("pick_first", &pf_args);
  p->pf_conn_state = GRPC_CHANNEL_IDLE;

  p->pts = pts_create();
  GPR_ASSERT(args->subchannel_factory != NULL);
  p->subchannel_factory = args->subchannel_factory;

  grpc_closure_init(&p->pf_pick_cb, pf_pick_cb, p);

  gpr_mu_init(&p->mu);
  return &p->base;
}

static const grpc_lb_policy_factory_vtable glb_factory_vtable = {
    glb_factory_ref, glb_factory_unref, glb_create, "grpclb"};

static grpc_lb_policy_factory glb_lb_policy_factory = {&glb_factory_vtable};

grpc_lb_policy_factory *grpc_glb_lb_factory_create() {
  return &glb_lb_policy_factory;
}
