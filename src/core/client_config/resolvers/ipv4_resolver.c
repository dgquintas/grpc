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

#include "src/core/client_config/resolvers/ipv4_resolver.h"

#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

#include <grpc/support/alloc.h>
#include <grpc/support/host_port.h>
#include <grpc/support/slice.h>
#include <grpc/support/slice_buffer.h>
#include <grpc/support/string_util.h>

#include "src/core/client_config/lb_policies/pick_first.h"
#include "src/core/support/string.h"

typedef struct {
  /** base class: must be first */
  grpc_resolver base;
  /** refcount */
  gpr_refcount refs;
  /** subchannel factory */
  grpc_subchannel_factory *subchannel_factory;
  /** load balancing policy factory */
  grpc_lb_policy *(*lb_policy_factory)(grpc_subchannel **subchannels,
                                       size_t num_subchannels);

  /** the addresses that we've 'resolved' */
  struct sockaddr_in *addrs;
  int *addrs_len;
  size_t num_addrs;

  /** mutex guarding the rest of the state */
  gpr_mu mu;
  /** have we published? */
  int published;
  /** pending next completion, or NULL */
  grpc_iomgr_closure *next_completion;
  /** target config address for next completion */
  grpc_client_config **target_config;
} ipv4_resolver;

static void ipv4_destroy(grpc_resolver *r);

static void ipv4_maybe_finish_next_locked(ipv4_resolver *r);

static void ipv4_shutdown(grpc_resolver *r);
static void ipv4_channel_saw_error(grpc_resolver *r,
                                   struct sockaddr *failing_address,
                                   int failing_address_len);
static void ipv4_next(grpc_resolver *r, grpc_client_config **target_config,
                      grpc_iomgr_closure *on_complete);

static const grpc_resolver_vtable ipv4_resolver_vtable = {
    ipv4_destroy, ipv4_shutdown, ipv4_channel_saw_error, ipv4_next};

static void ipv4_shutdown(grpc_resolver *resolver) {
  ipv4_resolver *r = (ipv4_resolver *)resolver;
  gpr_mu_lock(&r->mu);
  if (r->next_completion != NULL) {
    *r->target_config = NULL;
    grpc_iomgr_add_callback(r->next_completion);
    r->next_completion = NULL;
  }
  gpr_mu_unlock(&r->mu);
}

static void ipv4_channel_saw_error(grpc_resolver *resolver, struct sockaddr *sa,
                                   int len) {}

static void ipv4_next(grpc_resolver *resolver,
                      grpc_client_config **target_config,
                      grpc_iomgr_closure *on_complete) {
  ipv4_resolver *r = (ipv4_resolver *)resolver;
  gpr_mu_lock(&r->mu);
  GPR_ASSERT(!r->next_completion);
  r->next_completion = on_complete;
  r->target_config = target_config;
  ipv4_maybe_finish_next_locked(r);
  gpr_mu_unlock(&r->mu);
}

static void ipv4_maybe_finish_next_locked(ipv4_resolver *r) {
  grpc_client_config *config;
  grpc_lb_policy *lb_policy;
  grpc_subchannel **subchannels;
  grpc_subchannel_args args;

  if (r->next_completion != NULL && !r->published) {
    size_t i;
    config = grpc_client_config_create();
    subchannels = gpr_malloc(sizeof(grpc_subchannel *) * r->num_addrs);
    for (i = 0; i < r->num_addrs; i++) {
      memset(&args, 0, sizeof(args));
      args.addr = (struct sockaddr *)&r->addrs[i];
      args.addr_len = r->addrs_len[i];
      subchannels[i] = grpc_subchannel_factory_create_subchannel(
          r->subchannel_factory, &args);
    }
    lb_policy = r->lb_policy_factory(subchannels, r->num_addrs);
    gpr_free(subchannels);
    grpc_client_config_set_lb_policy(config, lb_policy);
    GRPC_LB_POLICY_UNREF(lb_policy, "mipv4");
    r->published = 1;
    *r->target_config = config;
    grpc_iomgr_add_callback(r->next_completion);
    r->next_completion = NULL;
  }
}

static void ipv4_destroy(grpc_resolver *gr) {
  ipv4_resolver *r = (ipv4_resolver *)gr;
  gpr_mu_destroy(&r->mu);
  grpc_subchannel_factory_unref(r->subchannel_factory);
  gpr_free(r->addrs);
  gpr_free(r->addrs_len);
  gpr_free(r);
}

static void do_nothing(void *ignored) {}
static grpc_resolver *ipv4_create(
    grpc_uri *uri,
    grpc_lb_policy *(*lb_policy_factory)(grpc_subchannel **subchannels,
                                         size_t num_subchannels),
    grpc_subchannel_factory *subchannel_factory) {
  size_t i;
  ipv4_resolver *r;

  const gpr_slice path_slice =
      gpr_slice_new(uri->path, strlen(uri->path), do_nothing);

  gpr_slice_buffer path_parts;
  gpr_slice_buffer_init(&path_parts);

  if (0 != strcmp(uri->authority, "")) {
    gpr_log(GPR_ERROR, "authority based uri's not supported");
    return NULL;
  }

  r = gpr_malloc(sizeof(ipv4_resolver));
  memset(r, 0, sizeof(*r));
  gpr_ref_init(&r->refs, 1);
  gpr_mu_init(&r->mu);
  grpc_resolver_init(&r->base, &ipv4_resolver_vtable);
  r->subchannel_factory = subchannel_factory;
  r->lb_policy_factory = lb_policy_factory;

  gpr_slice_split(path_slice, ",", &path_parts);
  r->num_addrs = path_parts.count;
  r->addrs = gpr_malloc(sizeof(struct sockaddr_in) * r->num_addrs);
  r->addrs_len = gpr_malloc(sizeof(int) * r->num_addrs);

  for(i = 0; i < r->num_addrs; i++) {
    char ip_with_port[22]; /* 12 IP, 1 colon, 5 port = 21 */
    char *host, *port;
    const int part_length = GPR_SLICE_LENGTH(path_parts.slices[i]);
    strncpy(ip_with_port,
            (const char *)GPR_SLICE_START_PTR(path_parts.slices[i]), 21);
    ip_with_port[GPR_MIN(part_length, 21)] = '\0';
    if (part_length > 21) {
      gpr_log(GPR_ERROR,
              "Invalid ip:port specification for ipv4 resolver: %s. Ignoring",
              ip_with_port);
      continue;
    }
    gpr_split_host_port(ip_with_port, &host, &port);
    
    r->addrs[i].sin_family = AF_INET;
    /* TODO(dgq): use strtol instead of atoi for error detection */
    r->addrs[i].sin_port = htons(atoi(port));
    inet_pton(AF_INET, host, &(r->addrs[i].sin_addr));
    r->addrs_len[i] = sizeof(struct sockaddr_in);

    gpr_free(host);
    gpr_free(port);
  }
  gpr_slice_buffer_destroy(&path_parts);
  gpr_slice_unref(path_slice);

  grpc_subchannel_factory_ref(subchannel_factory);
  return &r->base;
}

/*
 * FACTORY
 */

static void ipv4_factory_ref(grpc_resolver_factory *factory) {}

static void ipv4_factory_unref(grpc_resolver_factory *factory) {}

static grpc_resolver *ipv4_factory_create_resolver(
    grpc_resolver_factory *factory, grpc_uri *uri,
    grpc_subchannel_factory *subchannel_factory) {
  /* TODO(dgq): change pick first to rr */
  return ipv4_create(uri, grpc_create_pick_first_lb_policy, subchannel_factory);
}

static const grpc_resolver_factory_vtable ipv4_factory_vtable = {
    ipv4_factory_ref, ipv4_factory_unref, ipv4_factory_create_resolver};
static grpc_resolver_factory ipv4_resolver_factory = {&ipv4_factory_vtable};

grpc_resolver_factory *grpc_multi_ipv4_resolver_factory_create() {
  return &ipv4_resolver_factory;
}
