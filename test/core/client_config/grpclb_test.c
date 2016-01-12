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

#include <stdarg.h>
#include <string.h>

#include <grpc/grpc.h>
#include <grpc/support/alloc.h>
#include <grpc/support/thd.h>
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

static gpr_slice build_response_payload_slice() {
  return gpr_slice_from_copied_string(
      "\x12\x2A\x0A\x10\x0A\x09\x31\x32\x37\x2E\x30\x2E\x30\x2E\x31\x10\xB9\x60"
      "\x20\x01\x0A\x0E\x0A\x09\x31\x32\x37\x2E\x30\x2E\x30\x2E\x31\x10\xBA\x60"
      "\x1A\x06\x08\xF8\x06\x10\xE7\x07");
}

void start_server(lb_server_fixture *sf) {
  grpc_call *s;
  grpc_op ops[6];
  grpc_op *op;
  grpc_metadata_array initial_metadata_recv;
  grpc_metadata_array trailing_metadata_recv;
  grpc_metadata_array request_metadata_recv;
  grpc_call_details call_details;
  grpc_call_error error;
  char *details = NULL;
  int was_cancelled = 2;
  cq_verifier *cqv = cq_verifier_create(sf->cq);
  gpr_timespec deadline = gpr_inf_future(GPR_CLOCK_MONOTONIC);
  grpc_event ev;

  grpc_metadata_array_init(&initial_metadata_recv);
  grpc_metadata_array_init(&trailing_metadata_recv);
  grpc_metadata_array_init(&request_metadata_recv);
  grpc_call_details_init(&call_details);

  error =
      grpc_server_request_call(sf->server, &s, &call_details,
                               &request_metadata_recv, sf->cq, sf->cq, tag(100));
  GPR_ASSERT(GRPC_CALL_OK == error);
  ev = grpc_completion_queue_next(sf->cq, deadline, NULL);
  GPR_ASSERT(ev.tag == tag(100));

  /*cq_expect_completion(cqv, tag(100), 1);
  cq_verify(cqv);*/

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

  while (sf->shutdown == 0) {
    int read_tag;
    grpc_byte_buffer *request_payload_recv;
    grpc_byte_buffer *response_payload;
    gpr_slice response_payload_slice = build_response_payload_slice();
    response_payload = grpc_raw_byte_buffer_create(&response_payload_slice, 1);

    op = ops;
    op->op = GRPC_OP_RECV_MESSAGE;
    op->data.recv_message = &request_payload_recv;
    op->flags = 0;
    op->reserved = NULL;
    op++;
    error = grpc_call_start_batch(s, ops, (size_t)(op - ops), tag(102), NULL);
    GPR_ASSERT(GRPC_CALL_OK == error);
    gpr_log(GPR_INFO, "waiting at fake lb server");
    ev = grpc_completion_queue_next(sf->cq, deadline, NULL);
    read_tag = ((int)(intptr_t)ev.tag);
    gpr_log(GPR_DEBUG, "AT FAKE SERVER A: EVENT: success:%d, type:%d, tag:%d",
            ev.success, ev.type, read_tag);

    /*GPR_ASSERT(ev.tag == tag(102));*/

    op = ops;
    op->op = GRPC_OP_SEND_MESSAGE;
    op->data.send_message = response_payload;
    op->flags = 0;
    op->reserved = NULL;
    op++;
    error = grpc_call_start_batch(s, ops, (size_t)(op - ops), tag(103), NULL);
    GPR_ASSERT(GRPC_CALL_OK == error);
    ev = grpc_completion_queue_next(sf->cq, deadline, NULL);
    /*GPR_ASSERT(ev.tag == tag(103));*/
    gpr_log(GPR_DEBUG, "AT FAKE SERVER B: EVENT: success:%d, type:%d, tag:%d",
            ev.success, ev.type, read_tag);


    grpc_byte_buffer_destroy(response_payload);
    grpc_byte_buffer_destroy(request_payload_recv);
    gpr_slice_unref(response_payload_slice);

    gpr_sleep_until(gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                             gpr_time_from_seconds(5, GPR_TIMESPAN)));
  }

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

  /*cq_expect_completion(cqv, tag(101), 1);
  cq_expect_completion(cqv, tag(104), 1);
  cq_verify(cqv);*/

  grpc_call_destroy(s);

  cq_verifier_destroy(cqv);

  grpc_metadata_array_destroy(&initial_metadata_recv);
  grpc_metadata_array_destroy(&trailing_metadata_recv);
  grpc_metadata_array_destroy(&request_metadata_recv);
  grpc_call_details_destroy(&call_details);
  gpr_free(details);
}

void fork_server(void *arg) {
  lb_server_fixture *sf = arg;
  start_server(sf);
}

void perform_request(grpc_channel *client) {
  grpc_call *c;
  gpr_timespec deadline;
  grpc_op ops[6];
  grpc_op *op;
  grpc_status_code status;
  char *details;
  size_t details_capacity;
  grpc_event ev;
  int read_tag;
  grpc_metadata_array initial_metadata_recv;
  grpc_metadata_array trailing_metadata_recv;
  grpc_completion_queue *cq = grpc_completion_queue_create(NULL);

  /* Send a trivial request. */
  deadline = n_seconds_time(60);

  details = NULL;
  details_capacity = 0;

  grpc_metadata_array_init(&initial_metadata_recv);
  grpc_metadata_array_init(&trailing_metadata_recv);

  c = grpc_channel_create_call(client, NULL, GRPC_PROPAGATE_DEFAULTS, cq,
                               "/foo", "foo.test.google.fr", deadline, NULL);
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
             grpc_call_start_batch(c, ops, (size_t)(op - ops), tag(1), NULL));

  gpr_log(GPR_INFO, "waiting for whatever at client");
  ev = grpc_completion_queue_next(cq, GRPC_TIMEOUT_SECONDS_TO_DEADLINE(1000),
                                  NULL);
  read_tag = ((int)(intptr_t)ev.tag);
  gpr_log(GPR_DEBUG, "EVENT: success:%d, type:%d, tag:%d", ev.success, ev.type,
          read_tag);

  grpc_metadata_array_destroy(&initial_metadata_recv);
  grpc_metadata_array_destroy(&trailing_metadata_recv);

  grpc_call_destroy(c);

  gpr_free(details);
}

static lb_server_fixture *setup_lb_server(const char *server_host) {
  lb_server_fixture *f = gpr_malloc(sizeof(lb_server_fixture));
  int got_port;
  const size_t num_servers = 1;

  f->request_metadata_recv =
      gpr_malloc(sizeof(grpc_metadata_array) * num_servers);
  /* Create servers. */
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
  gpr_asprintf(&client_hostport, "ipv4:127.0.0.1:%d?lb_policy=grpclb", sf->port);
  client = grpc_insecure_channel_create(client_hostport, NULL, NULL);
  perform_request(client);
  grpc_channel_destroy(client);

  sf->shutdown = 1;
  gpr_thd_join(tid);
  gpr_free(sf);
  gpr_free(client_hostport);
  grpc_shutdown();
  return 0;
}
