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

/* XXX 
 *
 * 1) launch server
 * 2) instantiate subchannel from subchannel factory and args with server's ip
 * in a sockaddr struct.
 * 3) wrap subchannel from 2) in microchannel
 * 4) send a request to the channel and see what happens
 */

#include <stdarg.h>
#include <string.h>
#include <stdio.h>

#include <grpc/grpc.h>
#include <grpc/support/alloc.h>
#include <grpc/support/host_port.h>
#include <grpc/support/log.h>
#include <grpc/support/time.h>
#include <grpc/support/string_util.h>

#include "src/core/channel/channel_stack.h"
#include "src/core/surface/channel.h"
#include "src/core/channel/client_channel.h"
#include "src/core/support/string.h"
#include "src/core/surface/server.h"
#include "test/core/util/test_config.h"
#include "test/core/util/port.h"
#include "test/core/end2end/cq_verifier.h"
#include "src/core/iomgr/tcp_client.h"
#include "src/core/channel/http_client_filter.h"
#include "src/core/transport/chttp2_transport.h"
#include "src/core/channel/channel_args.h"
#include "src/core/client_config/resolver_registry.h"
#include "src/core/channel/client_microchannel.h"

typedef struct servers_fixture {
  size_t num_servers;
  grpc_server **servers;
  grpc_call **server_calls;
  grpc_completion_queue *cq;
  char **servers_hostports;
  grpc_metadata_array *request_metadata_recv;
} servers_fixture;

static void *tag(gpr_intptr t) { return (void *)t; }

static gpr_timespec n_seconds_time(int n) {
  return GRPC_TIMEOUT_SECONDS_TO_DEADLINE(n);
}

static void drain_cq(grpc_completion_queue *cq) {
  grpc_event ev;
  do {
    ev = grpc_completion_queue_next(cq, n_seconds_time(5), NULL);
  } while (ev.type != GRPC_QUEUE_SHUTDOWN);
}

static servers_fixture *setup_servers(const char *server_host,
                                      const size_t num_servers) {
  servers_fixture *f = gpr_malloc(sizeof(servers_fixture));
  int *ports;
  int got_port;
  size_t i;

  f->num_servers = num_servers;
  f->server_calls = gpr_malloc(sizeof(grpc_call *) * num_servers);
  f->request_metadata_recv =
      gpr_malloc(sizeof(grpc_metadata_array) * num_servers);
  /* Create servers. */
  ports = gpr_malloc(sizeof(int *) * num_servers);
  f->servers = gpr_malloc(sizeof(grpc_server *) * num_servers);
  f->servers_hostports = gpr_malloc(sizeof(char *) * num_servers);
  f->cq = grpc_completion_queue_create(NULL);
  for (i = 0; i < num_servers; i++) {
    ports[i] = grpc_pick_unused_port_or_die();

    gpr_join_host_port(&f->servers_hostports[i], server_host, ports[i]);

    f->servers[i] = grpc_server_create(NULL, NULL);
    grpc_server_register_completion_queue(f->servers[i], f->cq, NULL);
    GPR_ASSERT((got_port = grpc_server_add_insecure_http2_port(
                    f->servers[i], f->servers_hostports[i])) > 0);
    GPR_ASSERT(ports[i] == got_port);
    grpc_server_start(f->servers[i]);
  }
  gpr_free(ports);
  return f;
}

static void teardown_servers(servers_fixture *f) {
  size_t i;
  /* Destroy server. */
  for (i = 0; i < f->num_servers; i++) {
    if (f->servers[i] == NULL) continue;
    grpc_server_shutdown_and_notify(f->servers[i], f->cq, tag(10000));
    GPR_ASSERT(grpc_completion_queue_pluck(
                   f->cq, tag(10000), GRPC_TIMEOUT_SECONDS_TO_DEADLINE(5), NULL)
                   .type == GRPC_OP_COMPLETE);
    grpc_server_destroy(f->servers[i]);
  }
  grpc_completion_queue_shutdown(f->cq);
  drain_cq(f->cq);
  grpc_completion_queue_destroy(f->cq);

  gpr_free(f->servers);

  for (i = 0; i < f->num_servers; i++) {
    gpr_free(f->servers_hostports[i]);
  }

  gpr_free(f->servers_hostports);
  gpr_free(f->request_metadata_recv);
  gpr_free(f->server_calls);
  gpr_free(f);
}



typedef struct {
  grpc_connector base;
  gpr_refcount refs;

  grpc_iomgr_closure *notify;
  grpc_connect_in_args args;
  grpc_connect_out_args *result;
} connector;

static void connector_ref(grpc_connector *con) {
  connector *c = (connector *)con;
  gpr_ref(&c->refs);
}

static void connector_unref(grpc_connector *con) {
  connector *c = (connector *)con;
  if (gpr_unref(&c->refs)) {
    gpr_free(c);
  }
}

static void connected(void *arg, grpc_endpoint *tcp) {
  connector *c = arg;
  grpc_iomgr_closure *notify;
  if (tcp != NULL) {
    c->result->transport = grpc_create_chttp2_transport(
        c->args.channel_args, tcp, c->args.metadata_context, 1);
    grpc_chttp2_transport_start_reading(c->result->transport, NULL, 0);
    GPR_ASSERT(c->result->transport);
    c->result->filters = gpr_malloc(sizeof(grpc_channel_filter *));
    c->result->filters[0] = &grpc_http_client_filter;
    c->result->num_filters = 1;
  } else {
    memset(c->result, 0, sizeof(*c->result));
  }
  notify = c->notify;
  c->notify = NULL;
  grpc_iomgr_add_callback(notify);
}

static void connector_shutdown(grpc_connector *con) {}

static void connector_connect(grpc_connector *con,
                              const grpc_connect_in_args *args,
                              grpc_connect_out_args *result,
                              grpc_iomgr_closure *notify) {
  connector *c = (connector *)con;
  GPR_ASSERT(c->notify == NULL);
  GPR_ASSERT(notify->cb);
  c->notify = notify;
  c->args = *args;
  c->result = result;
  grpc_tcp_client_connect(connected, c, args->interested_parties, args->addr,
                          args->addr_len, args->deadline);
}

static const grpc_connector_vtable connector_vtable = {
    connector_ref, connector_unref, connector_shutdown, connector_connect};

typedef struct {
  grpc_subchannel_factory base;
  gpr_refcount refs;
  grpc_mdctx *mdctx;
  grpc_channel_args *merge_args;
  grpc_channel *master;
  grpc_subchannel **sniffed_subchannel;
} subchannel_factory;

static void test_subchannel_factory_ref(grpc_subchannel_factory *scf) {
  subchannel_factory *f = (subchannel_factory *)scf;
  gpr_ref(&f->refs);
}

static void test_subchannel_factory_unref(grpc_subchannel_factory *scf) {
  subchannel_factory *f = (subchannel_factory *)scf;
  if (gpr_unref(&f->refs)) {
    GRPC_CHANNEL_INTERNAL_UNREF(f->master, "test_subchannel_factory");
    grpc_channel_args_destroy(f->merge_args);
    grpc_mdctx_unref(f->mdctx);
    gpr_free(f);
  }
}

static grpc_subchannel *test_subchannel_factory_create_subchannel(
    grpc_subchannel_factory *scf, grpc_subchannel_args *args) {
  subchannel_factory *f = (subchannel_factory *)scf;
  connector *c = gpr_malloc(sizeof(*c));
  grpc_channel_args *final_args =
      grpc_channel_args_merge(args->args, f->merge_args);
  grpc_subchannel *s;
  memset(c, 0, sizeof(*c));
  c->base.vtable = &connector_vtable;
  gpr_ref_init(&c->refs, 1);
  args->mdctx = f->mdctx;
  args->args = final_args;
  args->master = f->master;
  s = grpc_subchannel_create(&c->base, args);
  grpc_connector_unref(&c->base);
  grpc_channel_args_destroy(final_args);
  *f->sniffed_subchannel = s;
  gpr_log(GPR_DEBUG, "SNIFFED SUBCHANNEL %p", s);
  return s;
}

static const grpc_subchannel_factory_vtable test_subchannel_factory_vtable = {
    test_subchannel_factory_ref, test_subchannel_factory_unref,
    test_subchannel_factory_create_subchannel};

/* The evil twin of grpc_unsecure_channel_create. It allows the test to use the
 * custom-built sniffing subchannel_factory */
static grpc_channel *channel_create(const char *target,
                                    const grpc_channel_args *args,
                                    grpc_subchannel **sniffed_subchannel) {
  grpc_channel *channel = NULL;
  const grpc_channel_filter *filters[1];
  grpc_resolver *resolver;
  subchannel_factory *f;
  grpc_mdctx *mdctx = grpc_mdctx_create();
  filters[0] = &grpc_client_channel_filter;

  channel =
      grpc_channel_create_from_filters(target, filters, 1, args, mdctx, 1);

  f = gpr_malloc(sizeof(*f));
  f->sniffed_subchannel = sniffed_subchannel;
  f->base.vtable = &test_subchannel_factory_vtable;
  gpr_ref_init(&f->refs, 1);
  grpc_mdctx_ref(mdctx);
  f->mdctx = mdctx;
  f->merge_args = grpc_channel_args_copy(args);
  f->master = channel;
  GRPC_CHANNEL_INTERNAL_REF(f->master, "test_subchannel_factory");
  resolver = grpc_resolver_create(target, &f->base);
  if (!resolver) {
    return NULL;
  }

  grpc_client_channel_set_resolver(grpc_channel_get_channel_stack(channel),
                                   resolver);
  GRPC_RESOLVER_UNREF(resolver, "test_create");
  grpc_subchannel_factory_unref(&f->base);
  return channel;
}

/** Returns connection sequence (server indices), which must be freed */
int perform_request(servers_fixture *f, grpc_channel *client,
                    grpc_subchannel **sniffed_subchannel) {
  grpc_call *c;
  int s_idx;
  int *s_valid;
  gpr_timespec deadline;
  grpc_op ops[6];
  grpc_op *op;
  grpc_status_code status;
  char *details;
  size_t details_capacity;
  int was_cancelled;
  grpc_call_details *call_details;
  size_t i;
  grpc_event ev;
  int read_tag;
  grpc_metadata_array initial_metadata_recv;
  grpc_metadata_array trailing_metadata_recv;
  cq_verifier *cqv = cq_verifier_create(f->cq);
  grpc_connectivity_state conn_state;
  grpc_channel *microchannel;
  grpc_channel_args args;

  s_valid = gpr_malloc(sizeof(int) * f->num_servers);
  call_details = gpr_malloc(sizeof(grpc_call_details) * f->num_servers);

  /* Send a trivial request. */
  deadline = n_seconds_time(60);

  details = NULL;
  details_capacity = 0;
  was_cancelled = 2;

  grpc_metadata_array_init(&initial_metadata_recv);
  grpc_metadata_array_init(&trailing_metadata_recv);

  for (i = 0; i < f->num_servers; i++) {
    grpc_call_details_init(&call_details[i]);
  }
  memset(s_valid, 0, f->num_servers * sizeof(int));

  /* the following will block. That's ok for this test */
  conn_state =
      grpc_channel_check_connectivity_state(client, 1 /* try to connect */);
  GPR_ASSERT(conn_state == GRPC_CHANNEL_IDLE);

  /* here sniffed_subchannel should be ready to use */
  args.num_args = 0;
  args.args = NULL;
  microchannel = grpc_client_microchannel_create(*sniffed_subchannel, &args);

  c = grpc_channel_create_call(microchannel, NULL, GRPC_PROPAGATE_DEFAULTS,
                               f->cq, "/foo", "foo.test.google.fr", deadline,
                               NULL);
  GPR_ASSERT(c);

  op = ops;
  op->op = GRPC_OP_SEND_INITIAL_METADATA;
  op->data.send_initial_metadata.count = 0;
  op->flags = 0;
  op->reserved = NULL;
  op++;
  op->op = GRPC_OP_SEND_CLOSE_FROM_CLIENT;
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
  GPR_ASSERT(GRPC_CALL_OK ==
             grpc_call_start_batch(c, ops, (size_t)0, tag(1), NULL));

  /* "listen" on all servers */
  for (i = 0; i < f->num_servers; i++) {
    grpc_metadata_array_init(&f->request_metadata_recv[i]);
    if (f->servers[i] != NULL) {
      GPR_ASSERT(GRPC_CALL_OK ==
                 grpc_server_request_call(f->servers[i], &f->server_calls[i],
                                          &call_details[i],
                                          &f->request_metadata_recv[i], f->cq,
                                          f->cq, tag(1000 + (int)i)));
    }
  }

  s_idx = -1;
  while ((ev = grpc_completion_queue_next(
              f->cq, GRPC_TIMEOUT_SECONDS_TO_DEADLINE(1), NULL))
             .type != GRPC_QUEUE_TIMEOUT) {
    read_tag = ((int)(gpr_intptr)ev.tag);
    if (ev.success && read_tag >= 1000) {
      GPR_ASSERT(s_idx == -1); /* only one server must reply */
      /* only server notifications for non-shutdown events */
      s_idx = read_tag - 1000;
      s_valid[s_idx] = 1;
    }
  }

  if (s_idx >= 0) {
    op = ops;
    op->op = GRPC_OP_SEND_INITIAL_METADATA;
    op->data.send_initial_metadata.count = 0;
    op->flags = 0;
    op->reserved = NULL;
    op++;
    op->op = GRPC_OP_SEND_STATUS_FROM_SERVER;
    op->data.send_status_from_server.trailing_metadata_count = 0;
    op->data.send_status_from_server.status = GRPC_STATUS_UNIMPLEMENTED;
    op->data.send_status_from_server.status_details = "xyz";
    op->flags = 0;
    op->reserved = NULL;
    op++;
    op->op = GRPC_OP_RECV_CLOSE_ON_SERVER;
    op->data.recv_close_on_server.cancelled = &was_cancelled;
    op->flags = 0;
    op->reserved = NULL;
    op++;
    GPR_ASSERT(GRPC_CALL_OK == grpc_call_start_batch(f->server_calls[s_idx],
                                                     ops, (size_t)(op - ops),
                                                     tag(102), NULL));

    cq_expect_completion(cqv, tag(102), 1);
    cq_expect_completion(cqv, tag(1), 1);
    cq_verify(cqv);

    GPR_ASSERT(status == GRPC_STATUS_UNIMPLEMENTED);
    GPR_ASSERT(0 == strcmp(details, "xyz"));
    GPR_ASSERT(0 == strcmp(call_details[s_idx].method, "/foo"));
    GPR_ASSERT(0 == strcmp(call_details[s_idx].host, "foo.test.google.fr"));
    GPR_ASSERT(was_cancelled == 1);
  }

  for (i = 0; i < f->num_servers; i++) {
    if (s_valid[i] != 0) {
      grpc_call_destroy(f->server_calls[i]);
    }
    grpc_metadata_array_destroy(&f->request_metadata_recv[i]);
  }
  grpc_metadata_array_destroy(&initial_metadata_recv);
  grpc_metadata_array_destroy(&trailing_metadata_recv);

  cq_verifier_destroy(cqv);

  grpc_call_destroy(c);

  for (i = 0; i < f->num_servers; i++) {
    grpc_call_details_destroy(&call_details[i]);
  }
  gpr_free(details);

  gpr_free(call_details);
  gpr_free(s_valid);

  return 1;
}

void test_microchannel() {
  grpc_channel *client;
  char *client_hostport;
  char *servers_hostports_str;
  grpc_subchannel *sniffed_subchannel;
  servers_fixture *f = setup_servers("127.0.0.1", 1);

  /* Create client. */
  servers_hostports_str = gpr_strjoin_sep((const char **)f->servers_hostports,
                                          f->num_servers, ",", NULL);
  gpr_asprintf(&client_hostport, "ipv4:%s", servers_hostports_str);
  client = channel_create(client_hostport, NULL, &sniffed_subchannel);

  GPR_ASSERT(perform_request(f, client, &sniffed_subchannel) != 0);

  gpr_free(client_hostport);
  gpr_free(servers_hostports_str);

  grpc_channel_destroy(client);
  teardown_servers(f);
}

int main(int argc, char **argv) {
  grpc_test_init(argc, argv);
  grpc_init();

  test_microchannel();

  grpc_shutdown();
  return 0;
}
