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
#include "src/core/client_config/resolver_registry.h"

#include <grpc/byte_buffer.h>
#include <grpc/byte_buffer_reader.h>
#include <grpc/support/thd.h>
#include <grpc/support/alloc.h>
#include <grpc/support/host_port.h>
#include <grpc/support/host_port.h>
#include <grpc/support/string_util.h>
#include <stdio.h>

typedef struct pending_pick {
  struct pending_pick *next;
  grpc_pollset *pollset;
  grpc_subchannel **target;
  grpc_metadata_batch *md;
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

  /* XXX */
  grpc_lb_policy *pick_first_policy;
  grpc_subchannel *pf_sc;
  grpc_closure pf_cb;

  /** are we shutting down? */
  int shutdown;

  /** which subchannel are we watching? This will be chosen by the round_robin
   * policy (potentially) based on the response from the LB service */
  size_t checking_subchannel;

  /** what is the connectivity of that channel? */
  grpc_connectivity_state checking_connectivity;

  /** list of picks that are waiting on connectivity */
  pending_pick *pending_picks;

  /** our connectivity state tracker */
  grpc_connectivity_state_tracker state_tracker;


  grpc_channel *uchannel;
  gpr_timespec urpc_deadline;
  grpc_grpclb_request* urpc_request;
  grpc_grpclb_response* urpc_response;
  grpc_grpclb_serverlist serverlist;
  int urpc_thread_started;

} grpclb_lb_policy;

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

grpc_grpclb_response *grpc_grpclb_response_parse(gpr_slice encoded_response) {
  bool status;
  pb_istream_t stream =
      pb_istream_from_buffer(GPR_SLICE_START_PTR(encoded_response),
                             GPR_SLICE_LENGTH(encoded_response));
  grpc_grpclb_response *res = gpr_malloc(sizeof(grpc_grpclb_response));
  memset(res, 0, sizeof(*res));
  status = pb_decode(
      &stream, grpc_lb_v0_LoadBalanceResponse_fields,
      res);
  GPR_ASSERT(status == true);
  return res;
}

grpc_grpclb_serverlist grpc_grpclb_response_parse_serverlist(
    gpr_slice encoded_response) {
  grpc_grpclb_serverlist sl;
  bool status;
  decode_serverlist_arg arg;
  pb_istream_t stream =
      pb_istream_from_buffer(GPR_SLICE_START_PTR(encoded_response),
                             GPR_SLICE_LENGTH(encoded_response));
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
  GRPC_LB_POLICY_UNREF(exec_ctx, p->pick_first_policy, "glb_destroy");
  /*GRPC_LB_POLICY_UNREF(exec_ctx, p->round_robin_policy, "glb_destroy");*/
  grpc_connectivity_state_destroy(exec_ctx, &p->state_tracker);
  gpr_free(p->subchannels);
  gpr_mu_destroy(&p->mu);
  gpr_free(p);
}

static void glb_shutdown(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol) {
  grpclb_lb_policy *p = (grpclb_lb_policy *)pol;
  gpr_mu_lock(&p->mu);
  p->shutdown = 1;
  grpc_lb_policy_shutdown(exec_ctx, p->pick_first_policy);
  /*grpc_lb_policy_shutdown(exec_ctx, p->round_robin_policy);*/
  grpc_connectivity_state_set(exec_ctx, &p->state_tracker,
                              GRPC_CHANNEL_FATAL_FAILURE, "shutdown");
  gpr_mu_unlock(&p->mu);
}

static void glb_exit_idle(grpc_exec_ctx *exec_ctx, grpc_lb_policy *pol) {
  /* XXX ??? */
  /*grpclb_lb_policy *p = (grpclb_lb_policy *)pol;
  gpr_mu_lock(&p->mu);
  if (!p->started_picking) {
    p->started_picking = 1;
    GRPC_LB_POLICY_REF(&p->base, "glb_connectivity");
  }
  gpr_mu_unlock(&p->mu);*/
}

static int parse_ipv4(const char* host_port, struct sockaddr_storage *addr,
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
    in->sin_port = htons((gpr_uint16)port_num);
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
                                 grpc_subchannel_factory *sc_factory) {
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

  /* let's assume we've gotten a response */
  grpc_subchannel **subchannels;
  size_t num_addrs = serverlist->num_servers;
  struct sockaddr_storage *addrs =
      gpr_malloc(sizeof(struct sockaddr_storage) * num_addrs);
  size_t *addrs_len = gpr_malloc(sizeof(*addrs_len) * num_addrs);

  /*const char *serverlist[] = {"127.0.0.1:2001", "127.0.0.1:2002",
                              "127.0.0.1:2003"};*/
  size_t i;
  char *hostport; /* XXX */
  int errors_found = 0;
  grpc_subchannel_args args;
  grpc_lb_policy_args rr_policy_args;

  for (i = 0; i < num_addrs; i++) {
    gpr_asprintf(&hostport, "%s:%d", serverlist->servers[i]->ip_address,
                                    serverlist->servers[i]->port);
    if (!parse_ipv4(hostport, &addrs[i], &addrs_len[i])) {
      errors_found = 1; /* GPR_TRUE */
    }
    gpr_free(hostport);
    if (errors_found) break;
  }

  subchannels = gpr_malloc(sizeof(grpc_subchannel *) * num_addrs);
  /* ahora paso las putas addrs a la subchannel factory */
  for (i = 0; i < num_addrs; i++) {
    memset(&args, 0, sizeof(args));
    args.addr = (struct sockaddr *)(&addrs[i]);
    args.addr_len = addrs_len[i];
    subchannels[i] = grpc_subchannel_factory_create_subchannel(
        exec_ctx, sc_factory, &args);
  }

  rr_policy_args.num_subchannels = num_addrs;
  rr_policy_args.subchannels = subchannels;
  /* si todo lo anterior funciona XD, ahora es cuestion de instanciar una
   * rr_policy y pasarle los subchannels */
  return grpc_lb_policy_create("round_robin", &rr_policy_args);
}

static void check_for_lb_response(grpc_exec_ctx *exec_ctx, void *arg,
                                  int success) {
  grpc_lb_policy *rr;
  pending_pick *pp;
  grpclb_lb_policy *p = arg;

  /* XXX: locks. and leaks */
  rr = create_rr(exec_ctx, &p->serverlist, p->subchannel_factory);

  pp = p->pending_picks;
  while (pp != NULL) {
    pending_pick *next = pp->next;
    grpc_lb_policy_pick(exec_ctx, rr, pp->pollset, pp->md, pp->target,
                        pp->on_complete);
    pp = next;
  }
}

static void *tag(gpr_intptr t) { return (void *)t; }
/* launch in a new thread */
static void urpc_loop(void *arg) {
  grpclb_lb_policy *p = (grpclb_lb_policy *)arg;
  /* p needs to have the uchannel, the request slice and the deadline */
  grpc_completion_queue *cq = grpc_completion_queue_create(NULL);
  grpc_channel *uchannel;
  gpr_timespec deadline;
  grpc_op ops[6];
  grpc_op *op;
  grpc_call *c;
  grpc_metadata_array initial_metadata_recv;
  grpc_metadata_array trailing_metadata_recv;
  grpc_metadata_array request_metadata_recv;
  grpc_call_details call_details;
  grpc_status_code status;
  grpc_call_error error;
  char *details = NULL;
  size_t details_capacity = 0;
  grpc_byte_buffer *response_payload_recv = NULL;
  grpc_event ev;
  grpc_exec_ctx exec_ctx = GRPC_EXEC_CTX_INIT;

  gpr_mu_lock(&p->mu);
  uchannel = p->uchannel;
  /*deadline = p->urpc_deadline;*/
  deadline = gpr_inf_future(GPR_CLOCK_MONOTONIC);
  gpr_mu_unlock(&p->mu);

  /* XXX: tenemos que pasar el target, en vez de usar localhost aqui */
  c = grpc_channel_create_call(uchannel, NULL, GRPC_PROPAGATE_DEFAULTS, cq,
                               "/helloworld.Greeter/SayHello", "localhost",
                               deadline, NULL);
  GPR_ASSERT(c);
  grpc_metadata_array_init(&initial_metadata_recv);
  grpc_metadata_array_init(&trailing_metadata_recv);
  grpc_metadata_array_init(&request_metadata_recv);
  grpc_call_details_init(&call_details);

  op = ops;
  op->op = GRPC_OP_SEND_INITIAL_METADATA;
  op->data.send_initial_metadata.count = 0;
  op->flags = 0;
  op->reserved = NULL;
  op++;

  op->op = GRPC_OP_RECV_INITIAL_METADATA;
  op->data.recv_initial_metadata = &initial_metadata_recv;
  op->flags = 0;
  op->reserved = NULL;
  op++;

  op->op = GRPC_OP_RECV_STATUS_ON_CLIENT;
  op->data.recv_status_on_client.trailing_metadata = &trailing_metadata_recv;
  op->data.recv_status_on_client.status = &status;
  op->data.recv_status_on_client.status_details = &details;
  op->data.recv_status_on_client.status_details_capacity = &details_capacity;
  op->flags = 0;
  op->reserved = NULL;
  op++;
  error = grpc_call_start_batch(c, ops, (size_t)(op - ops), tag(1), NULL);
  GPR_ASSERT(GRPC_CALL_OK == error);
  gpr_log(GPR_INFO, "initial req at urpc_loop");
  /*ev = grpc_completion_queue_next(cq, deadline, NULL);
  GPR_ASSERT(ev.tag == tag(1));*/

  while (p->shutdown == 0) {
    grpc_byte_buffer *request_payload;
    grpc_grpclb_request *request;
    grpc_grpclb_response *response;
    gpr_slice encoded_request;

    gpr_mu_lock(&p->mu);
    request = p->urpc_request;
    gpr_mu_unlock(&p->mu);

    encoded_request = grpc_grpclb_request_encode(request);
    request_payload = grpc_raw_byte_buffer_create(&encoded_request, 1);

    op = ops;
    op->op = GRPC_OP_SEND_MESSAGE;
    op->data.send_message = request_payload;
    op->flags = 0;
    op->reserved = NULL;
    op++;

    op->op = GRPC_OP_RECV_MESSAGE;
    op->data.recv_message = &response_payload_recv;
    op->flags = 0;
    op->reserved = NULL;
    op++;
    error = grpc_call_start_batch(c, ops, (size_t)(op - ops), tag(2), NULL);
    GPR_ASSERT(GRPC_CALL_OK == error);
    gpr_log(GPR_INFO, "waiting for lb response at urpc_loop");
    ev = grpc_completion_queue_next(cq, deadline, NULL);
    GPR_ASSERT(ev.tag == tag(2));
    if (ev.success && response_payload_recv != NULL) {
      grpc_byte_buffer_reader reader;
      grpc_grpclb_serverlist serverlist;

      gpr_slice incoming;
      grpc_byte_buffer_reader_init(&reader, response_payload_recv);
      /*GPR_ASSERT(grpc_byte_buffer_reader_next(&reader, &incoming) == 0);*/
      grpc_byte_buffer_reader_next(&reader, &incoming);
      response = grpc_grpclb_response_parse(incoming);
      GPR_ASSERT(response->has_server_list);
      serverlist = grpc_grpclb_response_parse_serverlist(incoming);
      gpr_slice_unref(incoming);
      grpc_byte_buffer_reader_destroy(&reader);
      gpr_mu_lock(&p->mu);
      p->serverlist = serverlist;
      gpr_mu_unlock(&p->mu);
      grpc_exec_ctx_enqueue(&exec_ctx,
                            grpc_closure_create(check_for_lb_response, p), 1);
      grpc_exec_ctx_flush(&exec_ctx);
    }
    /*GPR_ASSERT(byte_buffer_eq_string(response_payload_recv, "\x0a\x04pong"));*/
  }

  /* shutting down */
  op = ops;
  op->op = GRPC_OP_SEND_CLOSE_FROM_CLIENT;
  op->flags = 0;
  op->reserved = NULL;
  op++;

  error = grpc_call_start_batch(c, ops, (size_t)(op - ops), tag(3), NULL);
  GPR_ASSERT(GRPC_CALL_OK == error);
  grpc_completion_queue_next(cq, deadline, NULL);
  /*cq_expect_completion(cqv, tag(1), 1);
  cq_expect_completion(cqv, tag(3), 1);
  cq_verify(cqv);
*/

  grpc_metadata_array_destroy(&initial_metadata_recv);
  grpc_metadata_array_destroy(&trailing_metadata_recv);
  grpc_metadata_array_destroy(&request_metadata_recv);
  grpc_call_details_destroy(&call_details);

  grpc_call_destroy(c);
}

static void next_lb(grpclb_lb_policy *p) {
  if (p->urpc_thread_started == 0) {
    gpr_thd_id tid;
    gpr_thd_options options = gpr_thd_options_default();
    p->urpc_thread_started = 1;

    gpr_thd_new(&tid, urpc_loop, p, &options);
  }
}



static void probe_for_lb_service(grpc_exec_ctx *exec_ctx, grpclb_lb_policy *p) {
  /* construct uchannel from sc */
  grpc_channel_args args;
  grpc_subchannel *sc = p->pf_sc;

  args.num_args = 0;
  args.args = NULL;
  p->uchannel = grpc_client_uchannel_create(sc, &args);
  grpc_client_uchannel_set_subchannel(p->uchannel, sc);

  /* assemble request */
  p->urpc_request = grpc_grpclb_request_create("servicename");

  /* send */
  next_lb(p);
}

/* func behind p->picked_lb_candidate_cb */
static void picked_from_pf(grpc_exec_ctx *exec_ctx, void *arg,
                                int iomgr_success) {
  grpclb_lb_policy *p = arg;
  /* test p->first_picked for LB servicing */
  probe_for_lb_service(exec_ctx, p);
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

  /* Save a reference to the pick's data. It may be needed for the potential RR
   * pick */
  pp = gpr_malloc(sizeof(*pp));
  pp->next = p->pending_picks;
  pp->pollset = pollset;
  pp->target = target;
  pp->md = initial_metadata;
  pp->on_complete = on_complete;
  p->pending_picks = pp;

  /* get the first input subchannel that connects into p->pf_sc */
  grpc_lb_policy_pick(exec_ctx, p->pick_first_policy, pollset, initial_metadata,
                        &p->pf_sc, &p->pf_cb);

  gpr_mu_unlock(&p->mu);
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

  p->pick_first_policy = grpc_lb_policy_create("pick_first", &pf_policy_args);
  grpc_closure_init(&p->pf_cb, picked_from_pf, p);

  p->subchannels =
      gpr_malloc(sizeof(grpc_subchannel *) * args->num_subchannels);
  p->num_subchannels = args->num_subchannels;
  p->subchannel_factory = args->subchannel_factory;

  grpc_connectivity_state_init(&p->state_tracker, GRPC_CHANNEL_IDLE,
                               "grpclb");
  memcpy(p->subchannels, args->subchannels,
         sizeof(grpc_subchannel *) * args->num_subchannels);
  grpc_closure_init(&p->connectivity_changed, glb_connectivity_changed, p);
  gpr_mu_init(&p->mu);
  p->urpc_thread_started = 0;
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
