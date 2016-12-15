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

#include <grpc/support/alloc.h>
#include <grpc/support/string_util.h>

#include "src/core/ext/client_channel/client_channel_factory.h"

void grpc_client_channel_factory_ref(grpc_client_channel_factory *factory) {
  factory->vtable->ref(factory);
}

void grpc_client_channel_factory_unref(grpc_exec_ctx *exec_ctx,
                                       grpc_client_channel_factory *factory) {
  factory->vtable->unref(exec_ctx, factory);
}

grpc_subchannel *grpc_client_channel_factory_create_subchannel(
    grpc_exec_ctx *exec_ctx, grpc_client_channel_factory *factory,
    const grpc_subchannel_args *args) {
  return factory->vtable->create_subchannel(exec_ctx, factory, args);
}

grpc_channel *grpc_client_channel_factory_create_channel(
    grpc_exec_ctx *exec_ctx, grpc_client_channel_factory *factory,
    const char *target, grpc_client_channel_type type,
    const grpc_channel_args *args) {
  return factory->vtable->create_client_channel(exec_ctx, factory, target, type,
                                                args);
}

static void *factory_arg_copy(void *factory) {
  grpc_client_channel_factory_ref(factory);
  return factory;
}

static void factory_arg_destroy(void *factory) {
  // TODO(roth): Remove local exec_ctx when
  // https://github.com/grpc/grpc/pull/8705 is merged.
  grpc_exec_ctx exec_ctx = GRPC_EXEC_CTX_INIT;
  grpc_client_channel_factory_unref(&exec_ctx, factory);
  grpc_exec_ctx_finish(&exec_ctx);
}

static int factory_arg_cmp(void *factory1, void *factory2) {
  if (factory1 < factory2) return -1;
  if (factory1 > factory2) return 1;
  return 0;
}

static const grpc_arg_pointer_vtable factory_arg_vtable = {
    factory_arg_copy, factory_arg_destroy, factory_arg_cmp};

grpc_arg grpc_client_channel_factory_create_channel_arg(
    grpc_client_channel_factory *factory) {
  grpc_arg arg;
  arg.type = GRPC_ARG_POINTER;
  arg.key = GRPC_ARG_CLIENT_CHANNEL_FACTORY;
  arg.value.pointer.p = factory;
  arg.value.pointer.vtable = &factory_arg_vtable;
  return arg;
}

struct grpc_channel_credentials_target_info {
  const char **server_names;
  const char **canonical_names;
  size_t capacity;
  size_t size;
};

grpc_channel_credentials_target_info *
grpc_channel_credentials_target_info_create(size_t num_names) {
  grpc_channel_credentials_target_info *target_info =
      gpr_malloc(sizeof(*target_info) * num_names);
  memset(target_info, 0, sizeof(*target_info));
  return target_info;
}

grpc_channel_credentials_target_info *
grpc_channel_credentials_target_info_create_with_default(size_t num_names) {
  grpc_channel_credentials_target_info *target_info =
      gpr_malloc(sizeof(*target_info) * num_names);
  memset(target_info, 0, sizeof(*target_info));
  return target_info;
}

bool grpc_channel_credentials_target_info_add_pair(
    grpc_channel_credentials_target_info *target_info, const char *server_name,
    const char *canonical_name) {
  if (target_info->size + 1 > target_info->capacity) {
    return false;
  }
  const size_t i = target_info->size;
  target_info->server_names[i] = gpr_strdup(server_name);
  target_info->canonical_names[i] = gpr_strdup(canonical_name);
  ++target_info->size;
  return true;
}

const char *grpc_channel_credentials_target_info_find_canonical_name(
    grpc_channel_credentials_target_info *target_info,
    const char *server_name) {
  for (size_t i = 0; i < target_info->size; ++i) {
    if (strcmp(server_name, target_info->server_names[i]) == 0) {
      return target_info->canonical_names[i];
    }
  }
  return NULL;
}

grpc_channel_credentials_target_info *grpc_channel_credentials_target_info_copy(
    grpc_channel_credentials_target_info *src) {
  grpc_channel_credentials_target_info *copy =
      grpc_channel_credentials_target_info_create(src->capacity);
  copy->capacity = src->capacity;
  copy->size = src->size;
  for (size_t i = 0; i < src->size; ++i) {
    copy->server_names[i] = gpr_strdup(src->server_names[i]);
    copy->canonical_names[i] = gpr_strdup(src->canonical_names[i]);
  }
  return copy;
}

int grpc_channel_credentials_target_info_cmp(
    const grpc_channel_credentials_target_info *a,
    const grpc_channel_credentials_target_info *b) {
  if (a->capacity > b->capacity)
    return 1;
  else if (a->capacity < b->capacity)
    return -1;
  if (a->size > b->capacity)
    return 1;
  else if (a->size < b->capacity)
    return -1;

  for (size_t i = 0; i < a->size; ++i) {
    const int server_name_cmp = strcmp(a->server_names[i], b->server_names[i]);
    if (server_name_cmp != 0) return server_name_cmp;
    const int canonical_name_cmp =
        strcmp(a->canonical_names[i], b->canonical_names[i]);
    if (canonical_name_cmp != 0) return canonical_name_cmp;
  }
  return 0;
}

void grpc_channel_credentials_target_info_destroy(
    grpc_channel_credentials_target_info *target_info) {
  for (size_t i = 0; i < target_info->size; ++i) {
    gpr_free((char *)(target_info->server_names[i]));
    gpr_free((char *)(target_info->canonical_names[i]));
  }
  target_info->capacity = target_info->size = 0;
}
