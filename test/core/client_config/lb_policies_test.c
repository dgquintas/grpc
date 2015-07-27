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
#include <grpc/support/log.h>
#include <grpc/support/alloc.h>
#include <grpc/support/string_util.h>

#include "src/core/surface/server.h"
#include "src/core/support/string.h"
#include "test/core/util/test_config.h"
#include "test/core/util/port.h"

#define NUM_SERVERS 3

typedef struct fixture_data {
  grpc_server *servers[NUM_SERVERS];
  grpc_completion_queue *cqs[NUM_SERVERS];
  char *addrs[NUM_SERVERS];
} fixture_data;

static fixture_data *create_fixture(grpc_channel_args *server_args) {
  int i;
  fixture_data *f = gpr_malloc(sizeof(fixture_data));
  for (i = 0; i < NUM_SERVERS; i++) {
    int port = grpc_pick_unused_port_or_die();
    const char* host = "127.0.0.1";
    f->cqs[i] = grpc_completion_queue_create();
    f->servers[i] = grpc_server_create(server_args);
    grpc_server_register_completion_queue(f->servers[i], f->cqs[i]);
    gpr_asprintf(&f->addrs[i], "%s:%d", host, port);
    GPR_ASSERT(grpc_server_add_http2_port(f->servers[i], f->addrs[i]));
  }
  return f;
} 

static void start_servers(fixture_data *f) {
  int i;
  for (i = 0; i < NUM_SERVERS; i++) {
    grpc_server_start(f->servers[i]);
  }
}

static void *tag(gpr_intptr t) { return (void *)t; }
static void shutdown_servers(fixture_data *f) {
  int i;
  for (i = 0; i < NUM_SERVERS; i++) {
    if (f->servers[i]) {
      grpc_server_shutdown_and_notify(f->servers[i], f->cqs[i], tag(i));
      GPR_ASSERT(grpc_completion_queue_pluck(
                     f->cqs[i], tag(i), GRPC_TIMEOUT_SECONDS_TO_DEADLINE(5))
                     .type == GRPC_OP_COMPLETE);
      grpc_server_destroy(f->servers[i]);
      f->servers[i] = NULL;
    }
  }
}

static gpr_timespec n_seconds_time(int n) {
  return GRPC_TIMEOUT_SECONDS_TO_DEADLINE(n);
}

static gpr_timespec five_seconds_time(void) { return n_seconds_time(5); }

static void drain_cq(grpc_completion_queue *cq) {
  grpc_event ev;
  do {
    ev = grpc_completion_queue_next(cq, five_seconds_time());
  } while (ev.type != GRPC_QUEUE_SHUTDOWN);
}

static void destroy_fixture(fixture_data *f) {
  int i;

  shutdown_servers(f);

  for (i = 0; i < NUM_SERVERS; i++) {
    grpc_completion_queue_shutdown(f->cqs[i]);
    drain_cq(f->cqs[i]);
    grpc_completion_queue_destroy(f->cqs[i]);

    gpr_free(f->addrs[i]);
  }
  gpr_free(f);
}

static void test_ipv4_resolver(fixture_data *f) {
  char *uri, *joined_addrs;
  grpc_channel *client;
  joined_addrs =
      gpr_strjoin_sep((const char **)f->addrs, NUM_SERVERS, ",", NULL);
  gpr_asprintf(&uri, "ipv4:%s", joined_addrs);
  client = grpc_channel_create(uri, NULL);
  gpr_free(joined_addrs);
  gpr_free(uri);
  GPR_ASSERT(client);
  grpc_channel_destroy(client);
}

int main(int argc, char **argv) {
  fixture_data *f;

  grpc_test_init(argc, argv);
  grpc_init();

  f = create_fixture(NULL);
  start_servers(f);

  test_ipv4_resolver(f);

  destroy_fixture(f);

  grpc_shutdown();
  return 0;
}
