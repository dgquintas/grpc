/*
 *
 * Copyright 2015, Google Inc.
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

#include "src/core/client_config/lb_policies/grpclb.h"
#include "src/core/client_config/lb_policies/nanopb/pb_encode.h"
#include "src/core/client_config/lb_policies/nanopb/pb_decode.h"
#include "src/core/client_config/lb_policy_registry.h"
#include "src/core/client_config/resolver_factory.h"
#include "src/core/client_config/lb_policy.h"
#include "src/core/channel/client_uchannel.h"

#include <grpc/support/alloc.h>

typedef struct pending_pick {
  struct pending_pick *next;
  grpc_pollset *pollset;
  grpc_subchannel **target;
  grpc_closure *on_complete;
} pending_pick;

typedef struct {
  /** base policy: must be first */
  grpc_lb_policy base;
  /** all our subchannels */
  grpc_subchannel **subchannels;
  size_t num_subchannels;

  /* to be used on the addresses returned by the LB service */
  grpc_subchannel_factory *subchannel_factory;

  grpc_closure connectivity_changed;

  /** mutex protecting remaining members */
  gpr_mu mu;

  /** the selected channel */
  grpc_subchannel *selected;

  grpc_subchannel *first_picked;

  /** are we shutting down? */
  int shutdown;

  int started_picking;

  /** which subchannel are we watching? This will be chosen by the round_robin
   * policy (potentially) based on the response from the LB service */
  size_t checking_subchannel;

  /** what is the connectivity of that channel? */
  grpc_connectivity_state checking_connectivity;

  /** list of picks that are waiting on connectivity */
  pending_pick *pending_picks;

  /** our connectivity state tracker */
  grpc_connectivity_state_tracker state_tracker;

  /* XXX */
  grpc_lb_policy *pick_first;

  /* XXX */
  grpc_lb_policy *round_robin;

  grpc_subchannel *rr_pick;

  grpc_closure picked_lb_candidate_cb;

  grpc_completion_queue *urpc_cq;

} grpclb_lb_policy;

/*static void del_interested_parties_locked(grpc_exec_ctx *exec_ctx,
                                          grpclb_lb_policy *p) {
  pending_pick *pp;
  for (pp = p->pending_picks; pp; pp = pp->next) {
    grpc_subchannel_del_interested_party(
        exec_ctx, p->subchannels[p->checking_subchannel], pp->pollset);
  }
}

static void add_interested_parties_locked(grpc_exec_ctx *exec_ctx,
                                          grpclb_lb_policy *p) {
  pending_pick *pp;
  for (pp = p->pending_picks; pp; pp = pp->next) {
    grpc_subchannel_add_interested_party(
        exec_ctx, p->subchannels[p->checking_subchannel], pp->pollset);
  }
}*/


typedef struct decode_serverlist_arg {
  int first_pass;
  int i;
  size_t num_servers;
  grpc_grpclb_server** servers;
} decode_serverlist_arg;

/* invoked once for every Server in ServerList */
static bool decode_serverlist(pb_istream_t *stream, const pb_field_t *field,
                              void **arg) {
  decode_serverlist_arg *dec_arg = *arg;
  if (dec_arg->first_pass != 0) { /* first pass */
    grpc_grpclb_server server;
    if (!pb_decode(stream, grpc_lb_v0_Server_fields,
                   &server)) {
      return false;
    }
    dec_arg->num_servers++;
  } else { /* second pass */
    grpc_grpclb_server *server = gpr_malloc(sizeof(grpc_grpclb_server));
    GPR_ASSERT(dec_arg->num_servers > 0);
    if (dec_arg->i == 0) { /* first iteration of second pass */
      dec_arg->servers = gpr_malloc(sizeof(grpc_grpclb_server*) * dec_arg->num_servers);
    }
    if (!pb_decode(stream, grpc_lb_v0_Server_fields,
                   server)) {
      return false;
    }
    dec_arg->servers[dec_arg->i++] = server;
  }

  return true;
}

grpc_grpclb_request *grpc_grpclb_request_create(const char* lb_service_name) {
  grpc_grpclb_request *req = gpr_malloc(sizeof(grpc_grpclb_request));

  req->has_client_stats = 0; /* TODO(dgq) */
  req->has_initial_request = 1;
  /* FIXME: magic number */
  strncpy(req->initial_request.name, lb_service_name, 128);
  return req;
}

gpr_slice grpc_grpclb_request_encode(const grpc_grpclb_request* request) {
  size_t encoded_length;
  pb_ostream_t sizestream;
  pb_ostream_t outputstream;
  gpr_slice slice;
  memset(&sizestream, 0, sizeof(pb_ostream_t));
  pb_encode(&sizestream, grpc_lb_v0_LoadBalanceRequest_fields, request);
  encoded_length = sizestream.bytes_written;

  slice = gpr_slice_malloc(encoded_length);
  outputstream = pb_ostream_from_buffer(GPR_SLICE_START_PTR(slice), encoded_length);
  GPR_ASSERT(
      pb_encode(&outputstream,
                grpc_lb_v0_LoadBalanceRequest_fields,
                request) != 0);
  return slice;
}

void grpc_grpclb_request_destroy(grpc_grpclb_request *request) {
  gpr_free(request);
}

grpc_grpclb_response *grpc_grpclb_response_parse(const char* encoded_response) {
  bool status;
  pb_istream_t stream = pb_istream_from_buffer((uint8_t *)encoded_response,
                                               strlen(encoded_response));
  grpc_grpclb_response *res = gpr_malloc(sizeof(grpc_grpclb_response));
  memset(res, 0, sizeof(*res));
  status = pb_decode(
      &stream, grpc_lb_v0_LoadBalanceResponse_fields,
      res);
  GPR_ASSERT(status == true);
  return res;
}

grpc_grpclb_serverlist grpc_grpclb_response_parse_serverlist(
    const char* encoded_response) {
  grpc_grpclb_serverlist sl;
  bool status;
  decode_serverlist_arg arg;
  pb_istream_t stream = pb_istream_from_buffer((uint8_t *)encoded_response,
                                               strlen(encoded_response));
  pb_istream_t stream_at_start = stream;
  grpc_grpclb_response *res = gpr_malloc(sizeof(grpc_grpclb_response));
  memset(res, 0, sizeof(*res));
  memset(&arg, 0, sizeof(decode_serverlist_arg));

  res->server_list.servers.funcs.decode = decode_serverlist;
  res->server_list.servers.arg = &arg;
  arg.first_pass = 1;
  status = pb_decode(
      &stream, grpc_lb_v0_LoadBalanceResponse_fields,
      res);
  GPR_ASSERT(status == true);
  GPR_ASSERT(arg.num_servers > 0);

  arg.first_pass = 0;
  status = pb_decode(
      &stream_at_start, grpc_lb_v0_LoadBalanceResponse_fields,
      res);
  GPR_ASSERT(status == true);
  GPR_ASSERT(arg.servers != NULL);

  sl.num_servers = arg.num_servers;
  sl.servers = arg.servers;
  if (res->server_list.has_expiration_interval) {
    sl.expiration_interval = res->server_list.expiration_interval;
  }
  return sl;
}

void grpc_grpclb_response_destroy(grpc_grpclb_response *response) {
  gpr_free(response);
}


static void glb_destroy(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol) {
  grpclb_lb_policy *p = (grpclb_lb_policy *)pol;
  GRPC_LB_POLICY_UNREF(exec_ctx, p->pick_first, "glb_destroy");
  GRPC_LB_POLICY_UNREF(exec_ctx, p->round_robin, "glb_destroy");
  grpc_connectivity_state_destroy(exec_ctx, &p->state_tracker);
  gpr_free(p->subchannels);
  gpr_mu_destroy(&p->mu);
  gpr_free(p);
}

static void glb_shutdown(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol) {
  grpclb_lb_policy *p = (grpclb_lb_policy *)pol;
  gpr_mu_lock(&p->mu);
  p->shutdown = 1;
  grpc_lb_policy_shutdown(exec_ctx, p->pick_first);
  grpc_lb_policy_shutdown(exec_ctx, p->round_robin);
  grpc_connectivity_state_set(exec_ctx, &p->state_tracker,
                              GRPC_CHANNEL_FATAL_FAILURE, "shutdown");
  gpr_mu_unlock(&p->mu);
}

static void start_picking(grpc_exec_ctx *exec_ctx, grpclb_lb_policy *p) {
  p->started_picking = 1;
  GRPC_LB_POLICY_REF(&p->base, "glb_connectivity");

}

static void glb_exit_idle(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol) {
  grpclb_lb_policy *p = (grpclb_lb_policy *)pol;
  gpr_mu_lock(&p->mu);
  if (!p->started_picking) {
    start_picking(exec_ctx, p);
  }
  gpr_mu_unlock(&p->mu);
}

/*static*/ void picked_from_rr(grpc_exec_ctx *exec_ctx, void *arg,
                                int iomgr_success) {
  grpclb_lb_policy *p = arg;
  /* should be in p->rr_pick. This is what this lb should return. */

  p->selected = p->rr_pick;
}


/*static*/ void check_for_lb_response(grpclb_lb_policy *p) {
  /* si recibimos respuesta y no es UNIMPLEMENTED, crear subchannels mediante la
   * p->subchannel_factory, instancia una round_robin policy y hacer un pick
   * sobre esta. De este palo:
   * for (i = 0; i < addresses->naddrs; i++) {
   *   memset(&args, 0, sizeof(args));
   *   args.addr = (struct sockaddr *)(addresses->addrs[i].addr);
   *   args.addr_len = (size_t)addresses->addrs[i].len;
   *   subchannels[i] = grpc_subchannel_factory_create_subchannel(
   *       exec_ctx, r->subchannel_factory, &args);
   * }
   *
   * */

}

static void probe_for_lb_service(grpc_subchannel *sc) {
  /* construct uchannel from sc */
  grpc_channel_args args;
  grpc_channel *uchannel;
  uchannel = grpc_client_uchannel_create(sc, &args);
  grpc_client_uchannel_set_subchannel(uchannel, sc);

  /* assemble request */

  /* send lb request. is there a way to have a callback? I guess the completion
   * queue i need to associate to the call... i could check it as part of
   * whenever anything happens. See check_for_lb_response */
}

/* func behind p->picked_lb_candidate_cb */
static void picked_from_pf(grpc_exec_ctx *exec_ctx, void *arg,
                                int iomgr_success) {
  grpclb_lb_policy *p = arg;
  /* test p->first_picked for LB servicing */
  probe_for_lb_service(p->first_picked);
}

static void glb_pick(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol,
             grpc_pollset *pollset, grpc_metadata_batch *initial_metadata,
             grpc_subchannel **target, grpc_closure *on_complete) {
  /* XXX:
   * - fw-dear el pick a pf_pick.
   * - Una vez se tenga el subchannel, meterlo en un microchannel y hacer el rpc
   *   palero.
   * - parsear la respuesta del rpc palero
   * - crear subchannels a partir de los servers retornados y pasarselos a
   *   round-robin. Si no hay server retornados, usar el mismo subchannel que se
   *   paso a pick first.
   *
   * */

  grpclb_lb_policy *p = (grpclb_lb_policy *)pol;
  pending_pick *pp;
  gpr_mu_lock(&p->mu);

  if (p->selected) {
    gpr_mu_unlock(&p->mu);
    *target = p->selected;
    grpc_exec_ctx_enqueue(exec_ctx, on_complete, 1);
  } else {
    if (!p->started_picking) {
      p->started_picking = 1;
      grpc_lb_policy_pick(exec_ctx, p->pick_first, pollset, initial_metadata,
                          &p->first_picked, &p->picked_lb_candidate_cb);
    }
    grpc_subchannel_add_interested_party(
        exec_ctx, p->subchannels[p->checking_subchannel], pollset);
    pp = gpr_malloc(sizeof(*pp));
    pp->next = p->pending_picks;
    pp->pollset = pollset;
    pp->target = target;
    pp->on_complete = on_complete;
    p->pending_picks = pp;
    gpr_mu_unlock(&p->mu);
  }
}

static void glb_broadcast(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol,
                         grpc_transport_op *op) {
  grpclb_lb_policy *p = (grpclb_lb_policy *)pol;
  size_t i;
  size_t n;
  grpc_subchannel **subchannels;
  grpc_subchannel *selected;

  gpr_mu_lock(&p->mu);
  n = p->num_subchannels;
  subchannels = gpr_malloc(n * sizeof(*subchannels));
  selected = p->selected;
  if (selected) {
    GRPC_SUBCHANNEL_REF(selected, "glb_broadcast_to_selected");
  }
  for (i = 0; i < n; i++) {
    subchannels[i] = p->subchannels[i];
    GRPC_SUBCHANNEL_REF(subchannels[i], "glb_broadcast");
  }
  gpr_mu_unlock(&p->mu);

  for (i = 0; i < n; i++) {
    if (selected == subchannels[i]) continue;
    grpc_subchannel_process_transport_op(exec_ctx, subchannels[i], op);
    GRPC_SUBCHANNEL_UNREF(exec_ctx, subchannels[i], "glb_broadcast");
  }
  if (p->selected) {
    grpc_subchannel_process_transport_op(exec_ctx, selected, op);
    GRPC_SUBCHANNEL_UNREF(exec_ctx, selected, "glb_broadcast_to_selected");
  }
  gpr_free(subchannels);
}

static grpc_connectivity_state glb_check_connectivity(grpc_exec_ctx *exec_ctx,
                                                     grpc_lb_policy *pol) {
  grpclb_lb_policy *p = (grpclb_lb_policy *)pol;
  grpc_connectivity_state st;
  gpr_mu_lock(&p->mu);
  st = grpc_connectivity_state_check(&p->state_tracker);
  gpr_mu_unlock(&p->mu);
  return st;
}

static void glb_notify_on_state_change(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol,
                               grpc_connectivity_state *current,
                               grpc_closure *notify) {
  grpclb_lb_policy *p = (grpclb_lb_policy *)pol;
  gpr_mu_lock(&p->mu);
  grpc_connectivity_state_notify_on_state_change(exec_ctx, &p->state_tracker,
                                                 current, notify);
  gpr_mu_unlock(&p->mu);
}

static void glb_connectivity_changed(grpc_exec_ctx *exec_ctx, void *arg,
                                    int iomgr_success) {
  /* XXX lo gordo. hay que estar al tanto de la conectivity del subchannel que
   * se pasa a pick_first asi como de la del selected.
   *
   * Si cambia la del de pick first, hacer otro pick
   *
   * Si cambia el selected, hacer otro pick del rr.
   *
   * */
}

static const grpc_lb_policy_vtable grpclb_lb_policy_vtable = {
    glb_destroy, glb_shutdown, glb_pick, glb_exit_idle, glb_broadcast,
    glb_check_connectivity, glb_notify_on_state_change};

static void grpclb_factory_ref(grpc_lb_policy_factory *factory) {}

static void grpclb_factory_unref(grpc_lb_policy_factory *factory) {}

static grpc_lb_policy *create_grpclb(grpc_lb_policy_factory *factory,
                                     grpc_lb_policy_args *args) {
  /* XXX args necesita:
   * - subchannels
   * - num_subchannels
   * - subchannel_factory igual que un resolver (de hecho, el del resolver que
   *   instancia esta broza supongo que valdria */
  grpc_lb_policy_args pf_policy_args;

  grpclb_lb_policy *p = gpr_malloc(sizeof(*p));
  GPR_ASSERT(args->num_subchannels > 0);
  memset(p, 0, sizeof(*p));
  grpc_lb_policy_init(&p->base, &grpclb_lb_policy_vtable);

  memset(&pf_policy_args, 0, sizeof(grpc_lb_policy_args));

  /* XXX: construct pf_policy_args from args. The args to RR depend on what's
   * returned by the LB service */
  pf_policy_args.subchannels = args->subchannels;
  pf_policy_args.num_subchannels = args->num_subchannels;

  p->pick_first = grpc_lb_policy_create("pick_first", &pf_policy_args);
  grpc_closure_init(&p->picked_lb_candidate_cb, picked_from_pf, p);

  p->subchannels =
      gpr_malloc(sizeof(grpc_subchannel *) * args->num_subchannels);
  p->num_subchannels = args->num_subchannels;
  grpc_connectivity_state_init(&p->state_tracker, GRPC_CHANNEL_IDLE,
                               "grpclb");
  memcpy(p->subchannels, args->subchannels,
         sizeof(grpc_subchannel *) * args->num_subchannels);
  grpc_closure_init(&p->connectivity_changed, glb_connectivity_changed, p);
  gpr_mu_init(&p->mu);
  return &p->base;
}


static const grpc_lb_policy_factory_vtable grpclb_factory_vtable = {
    grpclb_factory_ref, grpclb_factory_unref, create_grpclb,
    "grpclb"};

static grpc_lb_policy_factory grpclb_lb_policy_factory = {
    &grpclb_factory_vtable};

grpc_lb_policy_factory *grpc_grpclb_lb_factory_create() {
  return &grpclb_lb_policy_factory;
}
