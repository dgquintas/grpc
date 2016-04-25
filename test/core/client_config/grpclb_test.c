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

#include <stdarg.h>
#include <string.h>

#include <grpc/grpc.h>
#include <grpc/support/alloc.h>
#include <grpc/support/thd.h>
#include <grpc/support/host_port.h>
#include <grpc/support/log.h>
#include <grpc/support/time.h>
#include <grpc/support/string_util.h>

#include "src/core/lib/channel/channel_stack.h"
#include "src/core/lib/surface/channel.h"
#include "src/core/ext/client_config/client_channel.h"
#include "src/core/lib/support/string.h"
#include "src/core/lib/surface/server.h"
#include "test/core/util/test_config.h"
#include "test/core/util/port.h"
#include "test/core/end2end/cq_verifier.h"

typedef struct lb_server_fixture {
  grpc_server *server;
  grpc_call *server_call;
  grpc_completion_queue *cq;
  char *servers_hostport;
  grpc_metadata_array *request_metadata_recv;
  int port;
  int shutdown;
} lb_server_fixture;

static gpr_timespec n_seconds_time(int n) {
  return GRPC_TIMEOUT_SECONDS_TO_DEADLINE(n);
}

static void *tag(intptr_t t) { return (void *)t; }

#if 0
static gpr_slice build_response_payload_slice() {
  /*
  server_list {
    servers {
      ip_address: "127.0.0.1"
      port: 1234
      load_balance_token: "token1"
    }
    servers {
      ip_address: "127.0.0.2"
      port: 4321
      load_balance_token: "token2"
    }
  }
  */
  return gpr_slice_from_copied_string(
      "\x12\x30\x0a\x16\x0a\x09\x31\x32\x37\x2e\x30\x2e\x30\x2e\x31\x10\xd2\x09"
      "\x1a\x06\x74\x6f\x6b\x65\x6e\x31\x0a\x16\x0a\x09\x31\x32\x37\x2e\x30\x2e"
      "\x30\x2e\x32\x10\xe1\x21\x1a\x06\x74\x6f\x6b\x65\x6e\x32");
}
#endif

static gpr_timespec five_seconds_time(void) { return n_seconds_time(5); }
static void shutdown_server(lb_server_fixture *sf) {
  if (!sf->server) return;
  grpc_server_shutdown_and_notify(sf->server, sf->cq, tag(1000));
  GPR_ASSERT(grpc_completion_queue_pluck(
                 sf->cq, tag(1000), GRPC_TIMEOUT_SECONDS_TO_DEADLINE(5), NULL)
                 .type == GRPC_OP_COMPLETE);
  grpc_server_destroy(sf->server);
  sf->server = NULL;
}

static void drain_cq(grpc_completion_queue *cq) {
  grpc_event ev;
  do {
    ev = grpc_completion_queue_next(cq, five_seconds_time(), NULL);
  } while (ev.type != GRPC_QUEUE_SHUTDOWN);
}


void start_server(lb_server_fixture *sf) {
  grpc_call *s;
  cq_verifier *cqv = cq_verifier_create(sf->cq);
  grpc_op ops[6];
  grpc_op *op;
  grpc_metadata_array request_metadata_recv;
  grpc_call_details call_details;
  grpc_call_error error;
  int was_cancelled = 2;
  grpc_byte_buffer *request_payload_recv;
  grpc_byte_buffer *response_payload;
  int i;
  gpr_slice response_payload_slice = gpr_slice_from_copied_string("hello you");

  grpc_metadata_array_init(&request_metadata_recv);
  grpc_call_details_init(&call_details);

  error =
      grpc_server_request_call(sf->server, &s, &call_details,
                               &request_metadata_recv, sf->cq, sf->cq, tag(100));
  GPR_ASSERT(GRPC_CALL_OK == error);
  gpr_log(GPR_INFO, "Server up!");
  cq_expect_completion(cqv, tag(100), 1);
  cq_verify(cqv);
  gpr_log(GPR_INFO, "Server after tag 100");

  op = ops;
  op->op = GRPC_OP_SEND_INITIAL_METADATA;
  op->data.send_initial_metadata.count = 0;
  op->flags = 0;
  op->reserved = NULL;
  op++;
  op->op = GRPC_OP_RECV_CLOSE_ON_SERVER;
  op->data.recv_close_on_server.cancelled = &was_cancelled;
  op->flags = 0;
  op->reserved = NULL;
  op++;
  error = grpc_call_start_batch(s, ops, (size_t)(op - ops), tag(101), NULL);
  GPR_ASSERT(GRPC_CALL_OK == error);
  gpr_log(GPR_INFO, "Server after tag 101");

  for (i = 0; i < 4; i++) {
    response_payload = grpc_raw_byte_buffer_create(&response_payload_slice, 1);

    op = ops;
    op->op = GRPC_OP_RECV_MESSAGE;
    op->data.recv_message = &request_payload_recv;
    op->flags = 0;
    op->reserved = NULL;
    op++;
    error = grpc_call_start_batch(s, ops, (size_t)(op - ops), tag(102), NULL);
    GPR_ASSERT(GRPC_CALL_OK == error);
    cq_expect_completion(cqv, tag(102), 1);
    cq_verify(cqv);
    gpr_log(GPR_INFO, "Server after tag 102, iter %d", i);

    op = ops;
    op->op = GRPC_OP_SEND_MESSAGE;
    op->data.send_message = response_payload;
    op->flags = 0;
    op->reserved = NULL;
    op++;
    error = grpc_call_start_batch(s, ops, (size_t)(op - ops), tag(103), NULL);
    GPR_ASSERT(GRPC_CALL_OK == error);
    cq_expect_completion(cqv, tag(103), 1);
    cq_verify(cqv);
    gpr_log(GPR_INFO, "Server after tag 103, iter %d", i);

    grpc_byte_buffer_destroy(response_payload);
    grpc_byte_buffer_destroy(request_payload_recv);
  }

  gpr_slice_unref(response_payload_slice);

  op = ops;
  op->op = GRPC_OP_SEND_STATUS_FROM_SERVER;
  op->data.send_status_from_server.trailing_metadata_count = 0;
  op->data.send_status_from_server.status = GRPC_STATUS_UNIMPLEMENTED;
  op->data.send_status_from_server.status_details = "xyz";
  op->flags = 0;
  op->reserved = NULL;
  op++;
  error = grpc_call_start_batch(s, ops, (size_t)(op - ops), tag(104), NULL);
  GPR_ASSERT(GRPC_CALL_OK == error);

  cq_expect_completion(cqv, tag(101), 1);
  cq_expect_completion(cqv, tag(104), 1);
  cq_verify(cqv);

  grpc_call_destroy(s);

  cq_verifier_destroy(cqv);

  grpc_metadata_array_destroy(&request_metadata_recv);
  grpc_call_details_destroy(&call_details);

  shutdown_server(sf);

  grpc_completion_queue_shutdown(sf->cq);
  drain_cq(sf->cq);
  grpc_completion_queue_destroy(sf->cq);
}

void fork_server(void *arg) {
  lb_server_fixture *sf = arg;
  start_server(sf);
}

static void shutdown_client(grpc_channel *client) {
  if (!client) return;
  grpc_channel_destroy(client);
  client = NULL;
}
void perform_request(grpc_channel *client) {
  grpc_call *c;
  grpc_completion_queue *cq = grpc_completion_queue_create(NULL);
  gpr_timespec deadline = five_seconds_time();
  cq_verifier *cqv = cq_verifier_create(cq);
  grpc_op ops[6];
  grpc_op *op;
  grpc_metadata_array initial_metadata_recv;
  grpc_metadata_array trailing_metadata_recv;
  grpc_status_code status;
  grpc_call_error error;
  char *details = NULL;
  size_t details_capacity = 0;
  grpc_byte_buffer *request_payload;
  grpc_byte_buffer *response_payload_recv;
  int i;
  gpr_slice request_payload_slice = gpr_slice_from_copied_string("hello world");

  c = grpc_channel_create_call(client, NULL, GRPC_PROPAGATE_DEFAULTS, cq,
                               "/foo", "foo.test.google.fr:1234", deadline,
                               NULL);
  GPR_ASSERT(c);

  grpc_metadata_array_init(&initial_metadata_recv);
  grpc_metadata_array_init(&trailing_metadata_recv);

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

  for (i = 0; i < 4; i++) {
    request_payload = grpc_raw_byte_buffer_create(&request_payload_slice, 1);

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

    cq_expect_completion(cqv, tag(2), 1);
    cq_verify(cqv);
    gpr_log(GPR_INFO, "Client after tag 2, iter %d", i);
    gpr_log(GPR_INFO, "Client received payload: %s", response_payload_recv);

    grpc_byte_buffer_destroy(request_payload);
    grpc_byte_buffer_destroy(response_payload_recv);
  }

  gpr_slice_unref(request_payload_slice);

  op = ops;
  op->op = GRPC_OP_SEND_CLOSE_FROM_CLIENT;
  op->flags = 0;
  op->reserved = NULL;
  op++;
  error = grpc_call_start_batch(c, ops, (size_t)(op - ops), tag(3), NULL);
  GPR_ASSERT(GRPC_CALL_OK == error);

  cq_expect_completion(cqv, tag(1), 1);
  cq_expect_completion(cqv, tag(3), 1);
  cq_verify(cqv);
  gpr_log(GPR_INFO, "Client after tag 1");
  gpr_log(GPR_INFO, "Client after tag 3");

  grpc_call_destroy(c);

  cq_verifier_destroy(cqv);

  grpc_metadata_array_destroy(&initial_metadata_recv);
  grpc_metadata_array_destroy(&trailing_metadata_recv);
  gpr_free(details);

  shutdown_client(client);
  grpc_completion_queue_shutdown(cq);
  drain_cq(cq);
  grpc_completion_queue_destroy(cq);
}

static lb_server_fixture *setup_lb_server(const char *server_host) {
  lb_server_fixture *f = gpr_malloc(sizeof(lb_server_fixture));
  int got_port;

  f->cq = grpc_completion_queue_create(NULL);
  f->port = grpc_pick_unused_port_or_die();

  gpr_join_host_port(&f->servers_hostport, server_host, f->port);

  f->server = grpc_server_create(NULL, NULL);
  grpc_server_register_completion_queue(f->server, f->cq, NULL);
  GPR_ASSERT((got_port = grpc_server_add_insecure_http2_port(
                  f->server, f->servers_hostport)) > 0);
  GPR_ASSERT(f->port == got_port);
  grpc_server_start(f->server);
  f->shutdown = 0;
  return f;
}


int main(int argc, char **argv) {
  lb_server_fixture *sf;
  char *client_hostport;
  gpr_thd_id tid;
  grpc_channel *client;
  gpr_thd_options options = gpr_thd_options_default();
  gpr_thd_options_set_joinable(&options);

  grpc_test_init(argc, argv);
  grpc_init();

  sf = setup_lb_server("127.0.0.1");
  gpr_thd_new(&tid, fork_server, sf, &options);
  gpr_asprintf(&client_hostport,
               "ipv4:127.0.0.1:%d?lb_policy=grpclb&lb_enabled=1", sf->port);
  client = grpc_insecure_channel_create(client_hostport, NULL, NULL);
  perform_request(client);

  sf->shutdown = 1;
  gpr_thd_join(tid);
  gpr_free(sf->servers_hostport);
  gpr_free(sf);
  gpr_free(client_hostport);
  grpc_shutdown();
  return 0;
}
