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

typedef struct servers_fixture {
  size_t num_servers;
  grpc_server **servers;
  grpc_completion_queue *cq;
  char **servers_hostports;
} servers_fixture;

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
    ev = grpc_completion_queue_next(cq, five_seconds_time(), NULL);
  } while (ev.type != GRPC_QUEUE_SHUTDOWN);
}

static servers_fixture *setup_servers(const char *server_host,
                                      size_t num_servers) {
  servers_fixture *f = gpr_malloc(sizeof(servers_fixture));
  int *ports;
  int got_port;
  size_t i;

  f->num_servers = num_servers;
  /* Create servers. */
  ports = gpr_malloc(sizeof(int*) * num_servers);
  f->servers = gpr_malloc(sizeof(grpc_server*) * num_servers);
  f->servers_hostports = gpr_malloc(sizeof(char*) * num_servers);
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
    /* XXX  if (i == 2) continue; */
    grpc_server_shutdown_and_notify(f->servers[i], f->cq, tag(10000));
    GPR_ASSERT(grpc_completion_queue_pluck(f->cq, tag(10000),
                                           GRPC_TIMEOUT_SECONDS_TO_DEADLINE(5),
                                           NULL)
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

  gpr_free(f);
}

void perform_request(const servers_fixture *f, grpc_channel *client) {

#define NUM_ITERS 16
  grpc_call *c;
  grpc_call *s[NUM_ITERS];
  int s_idx;
  int s_valid[NUM_ITERS];
  gpr_timespec deadline;
  grpc_op ops[6];
  grpc_op *op;
  grpc_metadata_array initial_metadata_recv;
  grpc_metadata_array trailing_metadata_recv;
  grpc_metadata_array request_metadata_recv;
  grpc_status_code status;
  char *details;
  size_t details_capacity;
  int was_cancelled;
  grpc_call_details call_details[NUM_ITERS];
  size_t i, req_num;
  grpc_event ev;
  int read_tag;
  int connection_sequence[NUM_ITERS] = {0};

  /* Send a trivial request. */
  deadline = ms_from_now(60000);
  for (i = 0; i < f->num_servers; i++) {
    GPR_ASSERT(GRPC_CALL_OK ==
               grpc_server_request_call(f->servers[i], &s[i], &call_details[i],
                                        &request_metadata_recv, f->cq, f->cq,
                                        tag(1000 + i)));
  }


  for (req_num = 0; req_num < NUM_ITERS; req_num++) {
    cq_verifier *cqv = cq_verifier_create(f->cq);
    details = NULL;
    details_capacity = 0;
    was_cancelled = 2;

    connection_sequence[req_num] = -1;
    grpc_metadata_array_init(&initial_metadata_recv);
    grpc_metadata_array_init(&trailing_metadata_recv);
    grpc_metadata_array_init(&request_metadata_recv);

    for (i = 0; i < f->num_servers; i++) {
      grpc_call_details_init(&call_details[i]);
    }
    memset(s, 0, NUM_ITERS*sizeof(grpc_call*));
    memset(s_valid, 0, NUM_ITERS*sizeof(int));

    c = grpc_channel_create_call(client, NULL, GRPC_PROPAGATE_DEFAULTS, f->cq,
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
               grpc_call_start_batch(c, ops, op - ops, tag(1), NULL));

    while ((ev = grpc_completion_queue_next(
                f->cq, GRPC_TIMEOUT_SECONDS_TO_DEADLINE(1), NULL))
               .type != GRPC_QUEUE_TIMEOUT) {
      read_tag = ((int)(gpr_intptr)ev.tag);
      gpr_log(GPR_INFO, "EVENNNNNNNNNT success:%d, type:%d, tag:%d iter:%d", ev.success,
              ev.type, read_tag, req_num);
      if (ev.success && read_tag >= 1000) { /* only server notifications for non-shutdown events */
        s_idx = read_tag - 1000;
        s_valid[s_idx] = 1;
        connection_sequence[req_num] = s_idx;
        /* re-subscribe to server notifications */
        GPR_ASSERT(GRPC_CALL_OK ==
                   grpc_server_request_call(f->servers[s_idx], &s[s_idx],
                                            &call_details[s_idx],
                                            &request_metadata_recv, f->cq,
                                            f->cq, tag(1000 + s_idx)));
      } else {
        s_valid[s_idx] = 0;
      }
    }
    cq_verify_empty(cqv);
    if (s_valid[s_idx]) {
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
      GPR_ASSERT(GRPC_CALL_OK ==
                 grpc_call_start_batch(s[s_idx], ops, op - ops, tag(102), NULL));

      cq_expect_completion(cqv, tag(102), 1);
      cq_expect_completion(cqv, tag(1), 1);
      cq_verify(cqv);


      GPR_ASSERT(status == GRPC_STATUS_UNIMPLEMENTED);
      GPR_ASSERT(0 == strcmp(details, "xyz"));
      GPR_ASSERT(0 == strcmp(call_details[s_idx].method, "/foo"));
      GPR_ASSERT(0 == strcmp(call_details[s_idx].host, "foo.test.google.fr"));
      GPR_ASSERT(was_cancelled == 1);

      gpr_log(GPR_INFO, "PEEEEEEEEEEER %s", grpc_call_get_peer(c));
    }

    for (i = 0; i < f->num_servers; i++) {
      if (s_valid[i] != 0) grpc_call_destroy(s[i]);
    }
    cq_verifier_destroy(cqv);

    grpc_call_destroy(c);

    grpc_metadata_array_destroy(&initial_metadata_recv);
    grpc_metadata_array_destroy(&trailing_metadata_recv);
    grpc_metadata_array_destroy(&request_metadata_recv);

    for (i = 0; i < f->num_servers; i++) {
      grpc_call_details_destroy(&call_details[i]);
    }
    gpr_free(details);

    if (req_num == 5) {
      gpr_log(GPR_INFO, "KIIIIIIIIIIIIIIIIIIIIIIIIIIIIILL");
      grpc_server_shutdown_and_notify(f->servers[2], f->cq, tag(1234));
      GPR_ASSERT(grpc_completion_queue_pluck(f->cq, tag(1234),
                                             GRPC_TIMEOUT_SECONDS_TO_DEADLINE(5),
                                             NULL)
                     .type == GRPC_OP_COMPLETE);
      grpc_server_destroy(f->servers[2]);
    }
    if (req_num == 8) {
      gpr_log(GPR_INFO, "REBIIIIIIIIIIIIIRTHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH");
      f->servers[2] = grpc_server_create(NULL, NULL);
      grpc_server_register_completion_queue(f->servers[2], f->cq, NULL);
      GPR_ASSERT(grpc_server_add_insecure_http2_port(f->servers[2], f->servers_hostports[2]) > 0);
      grpc_server_start(f->servers[2]);
      GPR_ASSERT(GRPC_CALL_OK ==
                 grpc_server_request_call(f->servers[2], &s[2], &call_details[2],
                                          &request_metadata_recv, f->cq, f->cq,
                                          tag(1000 + 2)));
    }
  }

  for (i = 0; i < NUM_ITERS; i++) {
    gpr_log(GPR_INFO, "%d - %d", i, connection_sequence[i]);
  }
}

void test_connect(const servers_fixture *f,
                  const char *client_host, int expect_ok) {
  char *client_hostport;
  char *servers_hostports_str;
  grpc_channel *client;

  /* Create client. */
  servers_hostports_str = gpr_strjoin_sep((const char **)f->servers_hostports,
                                          f->num_servers, ",", NULL);
  /* FIXME: remove the ipv4 prefix */
  /*gpr_asprintf(&client_hostport, "ipv4:127.0.0.1:1234,%s", servers_hostports_str);*/
  gpr_asprintf(&client_hostport, "ipv4:%s", servers_hostports_str);
  client = grpc_insecure_channel_create(client_hostport, NULL, NULL);

  gpr_log(GPR_INFO, "Testing with servers=%s client=%s (expecting %s)",
          servers_hostports_str, client_hostport,
          expect_ok ? "success" : "failure");

  perform_request(f, client);

  gpr_free(client_hostport);
  gpr_free(servers_hostports_str);

  /* Destroy client. */
  grpc_channel_destroy(client);
}

int main(int argc, char **argv) {
  servers_fixture *f;

  grpc_test_init(argc, argv);
  grpc_init();

  f = setup_servers("127.0.0.1", 4);
  test_connect(f, "127.0.0.1", 1);
  teardown_servers(f);

  grpc_shutdown();
  return 0;
}
