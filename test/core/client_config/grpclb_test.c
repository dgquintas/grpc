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
#include <grpc/support/host_port.h>
#include <grpc/support/log.h>
#include <grpc/support/string_util.h>
#include <grpc/support/thd.h>
#include <grpc/support/time.h>

#include "src/core/ext/client_config/client_channel.h"
#include "src/core/lib/channel/channel_stack.h"
#include "src/core/lib/support/string.h"
#include "src/core/lib/surface/channel.h"
#include "src/core/lib/surface/server.h"
#include "test/core/end2end/cq_verifier.h"
#include "test/core/util/port.h"
#include "test/core/util/test_config.h"

#define NUM_BACKENDS 2

typedef struct client_fixture {
  grpc_channel *client;
  char *server_uri;
  grpc_completion_queue *cq;
} client_fixture;

typedef struct server_fixture {
  grpc_server *server;
  grpc_call *server_call;
  grpc_completion_queue *cq;
  char *servers_hostport;
  int port;
  bool shutdown;
  gpr_thd_id tid;
} server_fixture;

typedef struct test_fixture {
  server_fixture lb_server;
  server_fixture lb_backends[NUM_BACKENDS];
  client_fixture client;
} test_fixture;

static gpr_timespec n_seconds_time(int n) {
  return GRPC_TIMEOUT_SECONDS_TO_DEADLINE(n);
}

static void *tag(intptr_t t) { return (void *)t; }

static gpr_slice build_response_payload_slice() {
  /*
server_list {
  servers {
    ip_address: "127.0.0.1"
    port: 1234
    load_balance_token: "token1234"
  }
  servers {
    ip_address: "127.0.0.1"
    port: 1235
    load_balance_token: "token1235"
  }
}
  */
  return gpr_slice_from_copied_string(
      "\x12\x36\x0a\x19\x0a\x09\x31\x32\x37\x2e\x30\x2e\x30\x2e\x31\x10\xd2\x09"
      "\x1a\x09\x74\x6f\x6b\x65\x6e\x31\x32\x33\x34\x0a\x19\x0a\x09\x31\x32\x37"
      "\x2e\x30\x2e\x30\x2e\x31\x10\xd3\x09\x1a\x09\x74\x6f\x6b\x65\x6e\x31\x32"
      "\x33\x35");
}

static gpr_timespec five_seconds_time(void) { return n_seconds_time(5); }

static void drain_cq(grpc_completion_queue *cq) {
  grpc_event ev;
  do {
    ev = grpc_completion_queue_next(cq, five_seconds_time(), NULL);
  } while (ev.type != GRPC_QUEUE_SHUTDOWN);
}

void start_lb_server(server_fixture *sf) {
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
  gpr_slice response_payload_slice = build_response_payload_slice();

  grpc_metadata_array_init(&request_metadata_recv);
  grpc_call_details_init(&call_details);

  error = grpc_server_request_call(sf->server, &s, &call_details,
                                   &request_metadata_recv, sf->cq, sf->cq,
                                   tag(200));
  GPR_ASSERT(GRPC_CALL_OK == error);
  gpr_log(GPR_INFO, "Server[%s] up", sf->servers_hostport);
  cq_expect_completion(cqv, tag(200), 1);
  cq_verify(cqv);
  gpr_log(GPR_INFO, "Server[%s] after tag 200", sf->servers_hostport);

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
  error = grpc_call_start_batch(s, ops, (size_t)(op - ops), tag(201), NULL);
  GPR_ASSERT(GRPC_CALL_OK == error);
  gpr_log(GPR_INFO, "Server[%s] after tag 201", sf->servers_hostport);

  for (i = 0; i < 4; i++) {
    response_payload = grpc_raw_byte_buffer_create(&response_payload_slice, 1);

    op = ops;
    op->op = GRPC_OP_RECV_MESSAGE;
    op->data.recv_message = &request_payload_recv;
    op->flags = 0;
    op->reserved = NULL;
    op++;
    error = grpc_call_start_batch(s, ops, (size_t)(op - ops), tag(202), NULL);
    GPR_ASSERT(GRPC_CALL_OK == error);
    cq_expect_completion(cqv, tag(202), 1);
    cq_verify(cqv);
    gpr_log(GPR_INFO, "Server[%s] after tag 202, iter %d", sf->servers_hostport,
            i);

    op = ops;
    op->op = GRPC_OP_SEND_MESSAGE;
    op->data.send_message = response_payload;
    op->flags = 0;
    op->reserved = NULL;
    op++;
    error = grpc_call_start_batch(s, ops, (size_t)(op - ops), tag(203), NULL);
    GPR_ASSERT(GRPC_CALL_OK == error);
    cq_expect_completion(cqv, tag(203), 1);
    cq_verify(cqv);
    gpr_log(GPR_INFO, "Server[%s] after tag 203, iter %d", sf->servers_hostport,
            i);

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
  error = grpc_call_start_batch(s, ops, (size_t)(op - ops), tag(204), NULL);
  GPR_ASSERT(GRPC_CALL_OK == error);

  cq_expect_completion(cqv, tag(201), 1);
  cq_expect_completion(cqv, tag(204), 1);
  cq_verify(cqv);

  grpc_call_destroy(s);

  cq_verifier_destroy(cqv);

  grpc_metadata_array_destroy(&request_metadata_recv);
  grpc_call_details_destroy(&call_details);
}

void start_backend_server(server_fixture *sf) {
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

  error = grpc_server_request_call(sf->server, &s, &call_details,
                                   &request_metadata_recv, sf->cq, sf->cq,
                                   tag(100));
  GPR_ASSERT(GRPC_CALL_OK == error);
  gpr_log(GPR_INFO, "Server[%s] up", sf->servers_hostport);
  cq_expect_completion(cqv, tag(100), 1);
  cq_verify(cqv);
  gpr_log(GPR_INFO, "Server[%s] after tag 100", sf->servers_hostport);

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
  gpr_log(GPR_INFO, "Server[%s] after tag 101", sf->servers_hostport);

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
    gpr_log(GPR_INFO, "Server[%s] after tag 102, iter %d", sf->servers_hostport,
            i);

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
    gpr_log(GPR_INFO, "Server[%s] after tag 103, iter %d", sf->servers_hostport,
            i);

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
}

void perform_request(client_fixture *cf) {
  grpc_call *c;
  gpr_timespec deadline = five_seconds_time();
  cq_verifier *cqv = cq_verifier_create(cf->cq);
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

  c = grpc_channel_create_call(cf->client, NULL, GRPC_PROPAGATE_DEFAULTS,
                               cf->cq, "/foo", "foo.test.google.fr:1234",
                               deadline, NULL);
  gpr_log(GPR_INFO, "Call %p created", c);
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

    gpr_log(GPR_INFO, "Client before tag 2, iter %d", i);
    cq_expect_completion(cqv, tag(2), 1);
    cq_verify(cqv);
    gpr_log(GPR_INFO, "Client after tag 2, iter %d", i);

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

  cq_verify_empty(cqv);
  cq_verifier_destroy(cqv);

  grpc_metadata_array_destroy(&initial_metadata_recv);
  grpc_metadata_array_destroy(&trailing_metadata_recv);
  gpr_free(details);
}

static void setup_client(const char *server_hostport, client_fixture *cf) {
  cf->cq = grpc_completion_queue_create(NULL);
  cf->server_uri = gpr_strdup(server_hostport);
  cf->client = grpc_insecure_channel_create(cf->server_uri, NULL, NULL);
}

static void teardown_client(client_fixture *cf) {
  grpc_completion_queue_shutdown(cf->cq);
  drain_cq(cf->cq);
  grpc_completion_queue_destroy(cf->cq);
  cf->cq = NULL;
  grpc_channel_destroy(cf->client);
  cf->client = NULL;
  gpr_free(cf->server_uri);
}

static void setup_server(const char *host, server_fixture *s) {
  int assigned_port;

  s->cq = grpc_completion_queue_create(NULL);
  char *colon_idx = strchr(host, ':');
  if (colon_idx) {
    char *port_str = colon_idx + 1;
    s->port = atoi(port_str);
    s->servers_hostport = gpr_strdup(host);
  } else {
    s->port = grpc_pick_unused_port_or_die();
    gpr_join_host_port(&s->servers_hostport, host, s->port);
  }

  s->server = grpc_server_create(NULL, NULL);
  grpc_server_register_completion_queue(s->server, s->cq, NULL);
  GPR_ASSERT((assigned_port = grpc_server_add_insecure_http2_port(
                  s->server, s->servers_hostport)) > 0);
  GPR_ASSERT(s->port == assigned_port);
  grpc_server_start(s->server);
  s->shutdown = 0;
}

static void teardown_server(server_fixture *s) {
  if (!s->server) return;
  grpc_server_shutdown_and_notify(s->server, s->cq, tag(1000));
  GPR_ASSERT(grpc_completion_queue_pluck(
                 s->cq, tag(1000), GRPC_TIMEOUT_SECONDS_TO_DEADLINE(5), NULL)
                 .type == GRPC_OP_COMPLETE);
  grpc_server_destroy(s->server);
  s->server = NULL;
  grpc_completion_queue_shutdown(s->cq);
  drain_cq(s->cq);
  grpc_completion_queue_destroy(s->cq);

  s->shutdown = 1;
  gpr_thd_join(s->tid);
  gpr_log(GPR_INFO, "Server[%s] bye bye", s->servers_hostport);
  gpr_free(s->servers_hostport);
}

static void fork_server(void *arg) {
  server_fixture *sf = arg;
  start_backend_server(sf);
}

static void setup_test_fixture(test_fixture *tf) {
  gpr_thd_options options = gpr_thd_options_default();
  gpr_thd_options_set_joinable(&options);

  setup_server("127.0.0.1", &tf->lb_server);
  gpr_thd_new(&tf->lb_server.tid, fork_server, &tf->lb_server, &options);

  int backends_port_start = 1234;
  for (int i = 0; i < NUM_BACKENDS; ++i) {
    char *hostport;
    gpr_asprintf(&hostport, "127.0.0.1:%d", backends_port_start++);
    setup_server(hostport, &tf->lb_backends[i]);
    gpr_free(hostport);
    gpr_thd_new(&tf->lb_backends[i].tid, fork_server, &tf->lb_backends[i],
                &options);
  }

  char *server_uri;
  gpr_asprintf(&server_uri, "ipv4:%s?lb_policy=grpclb&lb_enabled=1",
               tf->lb_server.servers_hostport);
  setup_client(server_uri, &tf->client);
  gpr_free(server_uri);
}

static void teardown_test_fixture(test_fixture *tf) {
  teardown_server(&tf->lb_server);
  for (int i = 0; i < NUM_BACKENDS; ++i) {
    teardown_server(&tf->lb_backends[i]);
  }
  teardown_client(&tf->client);
}

int main(int argc, char **argv) {
  grpc_test_init(argc, argv);
  grpc_init();

  test_fixture tf;
  setup_test_fixture(&tf);

  perform_request(&tf.client);
  perform_request(&tf.client);

  teardown_test_fixture(&tf);
  grpc_shutdown();
  return 0;
}
