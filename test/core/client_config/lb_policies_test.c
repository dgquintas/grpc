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

#include <string.h>

#include <grpc/grpc.h>
#include <grpc/support/alloc.h>
#include <grpc/support/host_port.h>
#include <grpc/support/log.h>
#include <grpc/support/string_util.h>

#include "src/core/surface/server.h"
#include "src/core/support/string.h"
#include "test/core/util/test_config.h"
#include "test/core/util/port.h"
#include "test/core/end2end/cq_verifier.h"

#define NUM_SERVERS 1

static void *tag(gpr_intptr t) { return (void *)t; }

static gpr_timespec n_seconds_time(int n) {
  return GRPC_TIMEOUT_SECONDS_TO_DEADLINE(n);
}

static gpr_timespec five_seconds_time(void) { return n_seconds_time(5); }

static gpr_timespec ms_from_now(int ms) {
  return GRPC_TIMEOUT_MILLIS_TO_DEADLINE(ms);
}

static void drain_cq(grpc_completion_queue *cq) {
  grpc_event ev;
  do {
    ev = grpc_completion_queue_next(cq, five_seconds_time());
  } while (ev.type != GRPC_QUEUE_SHUTDOWN);
}

void test_connect(const char *server_host, int num_servers,
                  const char *client_host, int expect_ok) {
  char *client_hostport;
  char **servers_hostports;
  char *servers_hostports_str;
  grpc_channel *client;
  grpc_server **servers;
  grpc_completion_queue **cqs;
  grpc_call *c;
  grpc_call *s;
  cq_verifier **cqvs;
  gpr_timespec deadline;
  int got_port;
  grpc_op ops[6];
  grpc_op *op;
  grpc_metadata_array initial_metadata_recv;
  grpc_metadata_array trailing_metadata_recv;
  grpc_metadata_array request_metadata_recv;
  grpc_status_code status;
  char *details = NULL;
  size_t details_capacity = 0;
  int was_cancelled = 2;
  grpc_call_details call_details;
  char *peer;
  int *ports;
  int i;

  grpc_metadata_array_init(&initial_metadata_recv);
  grpc_metadata_array_init(&trailing_metadata_recv);
  grpc_metadata_array_init(&request_metadata_recv);
  grpc_call_details_init(&call_details);

  /* Create server. */
  ports = gpr_malloc(sizeof(int*) * num_servers);
  servers = gpr_malloc(sizeof(grpc_server*) * num_servers);
  cqs = gpr_malloc(sizeof(grpc_completion_queue*) * num_servers);
  cqvs = gpr_malloc(sizeof(cq_verifier*) * num_servers);
  servers_hostports = gpr_malloc(sizeof(char*) * num_servers);
  for (i = 0; i < num_servers; i++) {
    ports[i] = grpc_pick_unused_port_or_die();

    gpr_join_host_port(&servers_hostports[i], server_host, ports[i]);

    cqs[i] = grpc_completion_queue_create();
    servers[i] = grpc_server_create(NULL);
    grpc_server_register_completion_queue(servers[i], cqs[i]);
    GPR_ASSERT((got_port = grpc_server_add_insecure_http2_port(
                    servers[i], servers_hostports[i])) > 0);
    GPR_ASSERT(ports[i] == got_port);
    grpc_server_start(servers[i]);
    cqvs[i] = cq_verifier_create(cqs[i]);
  }
  gpr_free(ports);


  /* Create client. */
  servers_hostports_str =
      gpr_strjoin_sep((const char **)servers_hostports, num_servers, ",", NULL);
  gpr_asprintf(&client_hostport, "ipv4:%s", servers_hostports_str);
  client = grpc_insecure_channel_create(client_hostport, NULL);

  gpr_log(GPR_INFO, "Testing with servers=%s client=%s (expecting %s)",
          servers_hostports_str, client_hostport,
          expect_ok ? "success" : "failure");

  gpr_free(client_hostport);
  for (i = 0; i < num_servers; i++) {
    gpr_free(servers_hostports[i]);
  }
  gpr_free(servers_hostports);
  gpr_free(servers_hostports_str);

  if (expect_ok) {
    /* Normal deadline, shouldn't be reached. */
    deadline = ms_from_now(60000);
  } else {
    /* Give up faster when failure is expected.
       BUG: Setting this to 1000 reveals a memory leak (b/18608927). */
    deadline = ms_from_now(1500);
  }

  /* Send a trivial request. */
  c = grpc_channel_create_call(client, NULL, GRPC_PROPAGATE_DEFAULTS, cqs[0],
                               "/foo", "foo.test.google.fr", deadline);
  GPR_ASSERT(c);

  op = ops;
  op->op = GRPC_OP_SEND_INITIAL_METADATA;
  op->data.send_initial_metadata.count = 0;
  op->flags = 0;
  op++;
  op->op = GRPC_OP_SEND_CLOSE_FROM_CLIENT;
  op->flags = 0;
  op++;
  op->op = GRPC_OP_RECV_INITIAL_METADATA;
  op->data.recv_initial_metadata = &initial_metadata_recv;
  op->flags = 0;
  op++;
  op->op = GRPC_OP_RECV_STATUS_ON_CLIENT;
  op->data.recv_status_on_client.trailing_metadata = &trailing_metadata_recv;
  op->data.recv_status_on_client.status = &status;
  op->data.recv_status_on_client.status_details = &details;
  op->data.recv_status_on_client.status_details_capacity = &details_capacity;
  op->flags = 0;
  op++;
  GPR_ASSERT(GRPC_CALL_OK == grpc_call_start_batch(c, ops, op - ops, tag(1)));

  if (expect_ok) {
    /* Check for a successful request. */
    GPR_ASSERT(GRPC_CALL_OK == grpc_server_request_call(
                                   servers[0], &s, &call_details,
                                   &request_metadata_recv, cqs[0], cqs[0], tag(101)));
    cq_expect_completion(cqvs[0], tag(101), 1);
    cq_verify(cqvs[0]);

    op = ops;
    op->op = GRPC_OP_SEND_INITIAL_METADATA;
    op->data.send_initial_metadata.count = 0;
    op->flags = 0;
    op++;
    op->op = GRPC_OP_SEND_STATUS_FROM_SERVER;
    op->data.send_status_from_server.trailing_metadata_count = 0;
    op->data.send_status_from_server.status = GRPC_STATUS_UNIMPLEMENTED;
    op->data.send_status_from_server.status_details = "xyz";
    op->flags = 0;
    op++;
    op->op = GRPC_OP_RECV_CLOSE_ON_SERVER;
    op->data.recv_close_on_server.cancelled = &was_cancelled;
    op->flags = 0;
    op++;
    GPR_ASSERT(GRPC_CALL_OK ==
               grpc_call_start_batch(s, ops, op - ops, tag(102)));

    cq_expect_completion(cqvs[0], tag(102), 1);
    cq_expect_completion(cqvs[0], tag(1), 1);
    cq_verify(cqvs[0]);

    peer = grpc_call_get_peer(c);
    gpr_log(GPR_DEBUG, "got peer: '%s'", peer);
    gpr_free(peer);

    GPR_ASSERT(status == GRPC_STATUS_UNIMPLEMENTED);
    GPR_ASSERT(0 == strcmp(details, "xyz"));
    GPR_ASSERT(0 == strcmp(call_details.method, "/foo"));
    GPR_ASSERT(0 == strcmp(call_details.host, "foo.test.google.fr"));
    GPR_ASSERT(was_cancelled == 1);

    grpc_call_destroy(s);
  } else {
    /* Check for a failed connection. */
    cq_expect_completion(cqvs[0], tag(1), 1);
    cq_verify(cqvs[0]);

    GPR_ASSERT(status == GRPC_STATUS_DEADLINE_EXCEEDED);
  }

  grpc_call_destroy(c);

  /* Destroy client. */
  grpc_channel_destroy(client);

  /* Destroy server. */
  for (i = 0; i < num_servers; i++) {
    cq_verifier_destroy(cqvs[i]);
    grpc_server_shutdown_and_notify(servers[i], cqs[i], tag(1000));
    GPR_ASSERT(grpc_completion_queue_pluck(cqs[i], tag(1000),
                                           GRPC_TIMEOUT_SECONDS_TO_DEADLINE(5))
                   .type == GRPC_OP_COMPLETE);
    grpc_server_destroy(servers[i]);
    grpc_completion_queue_shutdown(cqs[i]);
    drain_cq(cqs[i]);
    grpc_completion_queue_destroy(cqs[i]);
  }
  gpr_free(servers);
  gpr_free(cqs);
  gpr_free(cqvs);

  grpc_metadata_array_destroy(&initial_metadata_recv);
  grpc_metadata_array_destroy(&trailing_metadata_recv);
  grpc_metadata_array_destroy(&request_metadata_recv);

  grpc_call_details_destroy(&call_details);
  gpr_free(details);
}

int main(int argc, char **argv) {
  grpc_test_init(argc, argv);
  grpc_init();

  test_connect("127.0.0.1", 3, "127.0.0.1", 1);

  grpc_shutdown();
  return 0;
}
