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

#include "src/core/channel/client_microchannel.h"

#include <stdio.h>
#include <string.h>

#include "src/core/channel/channel_args.h"
#include "src/core/channel/connected_channel.h"
#include "src/core/surface/channel.h"
#include "src/core/iomgr/iomgr.h"
#include "src/core/support/string.h"
#include "src/core/transport/connectivity_state.h"
#include "src/core/channel/compress_filter.h"
#include "src/core/census/grpc_filter.h"

#include <grpc/support/alloc.h>
#include <grpc/support/log.h>
#include <grpc/support/sync.h>
#include <grpc/support/useful.h>

/* Client channel implementation */

typedef struct call_data call_data;

typedef struct {
  /** metadata context for this channel */
  grpc_mdctx *mdctx;

  /** master channel - the grpc_channel instance that ultimately owns
      this channel_data via its channel stack.
      We occasionally use this to bump the refcount on the master channel
      to keep ourselves alive through an asynchronous operation. */
  grpc_channel *master;

  /** mutex protecting client configuration, including all
      variables below in this data structure */
  gpr_mu mu_config;

  /** connectivity state being tracked */
  grpc_connectivity_state_tracker state_tracker;

  /** pollset_set of interested parties in a new connection */
  grpc_pollset_set pollset_set;

  /* the subchannel wrapped by the microchannel */
  grpc_subchannel *subchannel;


  grpc_closure foo_cb;
  grpc_connectivity_state bla;
} channel_data;

typedef enum {
  CALL_CREATED,
  CALL_WAITING_FOR_SEND,
  CALL_WAITING_FOR_CALL,
  CALL_ACTIVE,
  CALL_CANCELLED
} call_state;

struct call_data {
  /* owning element */
  grpc_call_element *elem;

  gpr_mu mu_state;

  call_state state;
  gpr_timespec deadline;
  grpc_closure async_setup_task;
  grpc_transport_stream_op waiting_op;
  /* our child call stack */
  grpc_subchannel_call *subchannel_call;
  grpc_linked_mdelem status;
  grpc_linked_mdelem details;
};

static grpc_closure *merge_into_waiting_op(grpc_call_element *elem,
                                           grpc_transport_stream_op *new_op)
    GRPC_MUST_USE_RESULT;

static void handle_op_after_cancellation(grpc_exec_ctx *exec_ctx,
                                         grpc_call_element *elem,
                                         grpc_transport_stream_op *op) {
  call_data *calld = elem->call_data;
  channel_data *chand = elem->channel_data;
  if (op->send_ops) {
    grpc_stream_ops_unref_owned_objects(op->send_ops->ops, op->send_ops->nops);
    op->on_done_send->cb(exec_ctx, op->on_done_send->cb_arg, 0);
  }
  if (op->recv_ops) {
    char status[GPR_LTOA_MIN_BUFSIZE];
    grpc_metadata_batch mdb;
    gpr_ltoa(GRPC_STATUS_CANCELLED, status);
    calld->status.md =
        grpc_mdelem_from_strings(chand->mdctx, "grpc-status", status);
    calld->details.md =
        grpc_mdelem_from_strings(chand->mdctx, "grpc-message", "Cancelled");
    calld->status.prev = calld->details.next = NULL;
    calld->status.next = &calld->details;
    calld->details.prev = &calld->status;
    mdb.list.head = &calld->status;
    mdb.list.tail = &calld->details;
    mdb.garbage.head = mdb.garbage.tail = NULL;
    mdb.deadline = gpr_inf_future(GPR_CLOCK_REALTIME);
    grpc_sopb_add_metadata(op->recv_ops, mdb);
    *op->recv_state = GRPC_STREAM_CLOSED;
    op->on_done_recv->cb(exec_ctx, op->on_done_recv->cb_arg, 1);
  }
  if (op->on_consumed) {
    op->on_consumed->cb(exec_ctx, op->on_consumed->cb_arg, 0);
  }
}

typedef struct {
  grpc_closure closure;
  grpc_call_element *elem;
} waiting_call;

static void perform_transport_stream_op(grpc_exec_ctx *exec_ctx,
                                        grpc_call_element *elem,
                                        grpc_transport_stream_op *op,
                                        int continuation);

static int is_empty(void *p, int len) {
  char *ptr = p;
  int i;
  for (i = 0; i < len; i++) {
    if (ptr[i] != 0) return 0;
  }
  return 1;
}

static void foo(void *arg, int iomgr_success) {
  channel_data *chand = arg;
  grpc_connectivity_state_set(&chand->state_tracker, chand->bla,
                              "microchannel_foo");
  grpc_subchannel_notify_on_state_change(chand->subchannel, &chand->bla, &chand->foo_cb);
}

static void started_call(grpc_exec_ctx *exec_ctx, void *arg,
                         int iomgr_success) {
  call_data *calld = arg;
  grpc_transport_stream_op op;
  int have_waiting;

  gpr_mu_lock(&calld->mu_state);
  if (calld->state == CALL_CANCELLED && calld->subchannel_call != NULL) {
    memset(&op, 0, sizeof(op));
    op.cancel_with_status = GRPC_STATUS_CANCELLED;
    gpr_mu_unlock(&calld->mu_state);
    grpc_subchannel_call_process_op(exec_ctx, calld->subchannel_call, &op);
  } else if (calld->state == CALL_WAITING_FOR_CALL) {
    have_waiting = !is_empty(&calld->waiting_op, sizeof(calld->waiting_op));
    if (calld->subchannel_call != NULL) {
      calld->state = CALL_ACTIVE;
      gpr_mu_unlock(&calld->mu_state);
      if (have_waiting) {
        grpc_subchannel_call_process_op(exec_ctx, calld->subchannel_call,
                                        &calld->waiting_op);
      }
    } else {
      calld->state = CALL_CANCELLED;
      gpr_mu_unlock(&calld->mu_state);
      if (have_waiting) {
        handle_op_after_cancellation(exec_ctx, calld->elem, &calld->waiting_op);
      }
    }
  } else {
    GPR_ASSERT(calld->state == CALL_CANCELLED);
    gpr_mu_unlock(&calld->mu_state);
  }
}

static grpc_closure *merge_into_waiting_op(grpc_call_element *elem,
                                           grpc_transport_stream_op *new_op) {
  call_data *calld = elem->call_data;
  grpc_closure *consumed_op = NULL;
  grpc_transport_stream_op *waiting_op = &calld->waiting_op;
  GPR_ASSERT((waiting_op->send_ops != NULL) + (new_op->send_ops != NULL) <= 1);
  GPR_ASSERT((waiting_op->recv_ops != NULL) + (new_op->recv_ops != NULL) <= 1);
  if (new_op->send_ops != NULL) {
    waiting_op->send_ops = new_op->send_ops;
    waiting_op->is_last_send = new_op->is_last_send;
    waiting_op->on_done_send = new_op->on_done_send;
  }
  if (new_op->recv_ops != NULL) {
    waiting_op->recv_ops = new_op->recv_ops;
    waiting_op->recv_state = new_op->recv_state;
    waiting_op->on_done_recv = new_op->on_done_recv;
  }
  if (new_op->on_consumed != NULL) {
    if (waiting_op->on_consumed != NULL) {
      consumed_op = waiting_op->on_consumed;
    }
    waiting_op->on_consumed = new_op->on_consumed;
  }
  if (new_op->cancel_with_status != GRPC_STATUS_OK) {
    waiting_op->cancel_with_status = new_op->cancel_with_status;
  }
  return consumed_op;
}

static char *cmc_get_peer(grpc_exec_ctx *exec_ctx, grpc_call_element *elem) {
  call_data *calld = elem->call_data;
  channel_data *chand = elem->channel_data;
  grpc_subchannel_call *subchannel_call;
  char *result;

  gpr_mu_lock(&calld->mu_state);
  if (calld->state == CALL_ACTIVE) {
    subchannel_call = calld->subchannel_call;
    GRPC_SUBCHANNEL_CALL_REF(subchannel_call, "get_peer");
    gpr_mu_unlock(&calld->mu_state);
    result = grpc_subchannel_call_get_peer(exec_ctx, subchannel_call);
    GRPC_SUBCHANNEL_CALL_UNREF(exec_ctx, subchannel_call, "get_peer");
    return result;
  } else {
    gpr_mu_unlock(&calld->mu_state);
    return grpc_channel_get_target(chand->master);
  }
}

static void perform_transport_stream_op(grpc_exec_ctx *exec_ctx,
                                        grpc_call_element *elem,
                                        grpc_transport_stream_op *op,
                                        int continuation) {
  call_data *calld = elem->call_data;
  channel_data *chand = elem->channel_data;
  grpc_subchannel_call *subchannel_call;
  grpc_transport_stream_op op2;
  GPR_ASSERT(elem->filter == &grpc_client_microchannel_filter);
  GRPC_CALL_LOG_OP(GPR_INFO, elem, op);

  gpr_mu_lock(&calld->mu_state);
  switch (calld->state) {
    case CALL_ACTIVE:
      GPR_ASSERT(!continuation);
      subchannel_call = calld->subchannel_call;
      gpr_mu_unlock(&calld->mu_state);
      grpc_subchannel_call_process_op(exec_ctx, subchannel_call, op);
      break;
    case CALL_CANCELLED:
      gpr_mu_unlock(&calld->mu_state);
      handle_op_after_cancellation(exec_ctx, elem, op);
      break;
    case CALL_WAITING_FOR_SEND:
      GPR_ASSERT(!continuation);
      grpc_exec_ctx_enqueue(exec_ctx, merge_into_waiting_op(elem, op), 1);
      if (!calld->waiting_op.send_ops &&
          calld->waiting_op.cancel_with_status == GRPC_STATUS_OK) {
        gpr_mu_unlock(&calld->mu_state);
        break;
      }
      *op = calld->waiting_op;
      memset(&calld->waiting_op, 0, sizeof(calld->waiting_op));
      continuation = 1;
    /* fall through */
    case CALL_WAITING_FOR_CALL:
      if (!continuation) {
        if (op->cancel_with_status != GRPC_STATUS_OK) {
          calld->state = CALL_CANCELLED;
          op2 = calld->waiting_op;
          memset(&calld->waiting_op, 0, sizeof(calld->waiting_op));
          if (op->on_consumed) {
            calld->waiting_op.on_consumed = op->on_consumed;
            op->on_consumed = NULL;
          } else if (op2.on_consumed) {
            calld->waiting_op.on_consumed = op2.on_consumed;
            op2.on_consumed = NULL;
          }
          gpr_mu_unlock(&calld->mu_state);
          handle_op_after_cancellation(exec_ctx, elem, op);
          handle_op_after_cancellation(exec_ctx, elem, &op2);
        } else {
          grpc_exec_ctx_enqueue(exec_ctx, merge_into_waiting_op(elem, op), 1);
          gpr_mu_unlock(&calld->mu_state);
        }
        break;
      }
    /* fall through */
    case CALL_CREATED:
      if (op->cancel_with_status != GRPC_STATUS_OK) {
        calld->state = CALL_CANCELLED;
        gpr_mu_unlock(&calld->mu_state);
        handle_op_after_cancellation(exec_ctx, elem, op);
      } else {
        calld->waiting_op = *op;

        if (op->send_ops == NULL) {
          /* need to have some send ops before we can select the
             lb target */
          calld->state = CALL_WAITING_FOR_SEND;
          gpr_mu_unlock(&calld->mu_state);
        } else {
          /* Create subchannel call */
          grpc_pollset *pollset = calld->waiting_op.bind_pollset;
          calld->state = CALL_WAITING_FOR_CALL;
          gpr_mu_unlock(&calld->mu_state);
          grpc_iomgr_closure_init(&calld->async_setup_task, started_call, calld);
          grpc_subchannel_create_call(chand->subchannel, pollset,
                                      &calld->subchannel_call,
                                      &calld->async_setup_task);
        }
      }
      break;
  }

  if (consumed_op != NULL) {
    consumed_op->cb(consumed_op->cb_arg, 1);
  }
}

static void cmc_start_transport_stream_op(grpc_call_element *elem,
                                         grpc_transport_stream_op *op) {
  perform_transport_stream_op(elem, op, 0);
}

static void cmc_start_transport_op(grpc_channel_element *elem,
                                  grpc_transport_op *op) {
  channel_data *chand = elem->channel_data;
  grpc_iomgr_closure *on_consumed = op->on_consumed;
  op->on_consumed = NULL;

  GPR_ASSERT(op->set_accept_stream == NULL);
  GPR_ASSERT(op->bind_pollset == NULL);

  gpr_mu_lock(&chand->mu_config);
  if (op->on_connectivity_state_change != NULL) {
    grpc_connectivity_state_notify_on_state_change(
        &chand->state_tracker, op->connectivity_state,
        op->on_connectivity_state_change);
    op->on_connectivity_state_change = NULL;
    op->connectivity_state = NULL;
  }

  if (op->disconnect) {
    grpc_connectivity_state_set(&chand->state_tracker,
                                GRPC_CHANNEL_FATAL_FAILURE, "disconnect");
  }
  gpr_mu_unlock(&chand->mu_config);

  if (on_consumed) {
    grpc_iomgr_add_callback(on_consumed);
  }
}

/* Constructor for call_data */
static void cmc_init_call_elem(grpc_call_element *elem,
                           const void *server_transport_data,
                           grpc_transport_stream_op *initial_op) {
  call_data *calld = elem->call_data;

  /* TODO(ctiller): is there something useful we can do here? */
  GPR_ASSERT(initial_op == NULL);

  GPR_ASSERT(elem->filter == &grpc_client_microchannel_filter);
  GPR_ASSERT(server_transport_data == NULL);
  gpr_mu_init(&calld->mu_state);
  calld->elem = elem;
  calld->state = CALL_CREATED;
  calld->deadline = gpr_inf_future(GPR_CLOCK_REALTIME);
}

/* Destructor for call_data */
static void cmc_destroy_call_elem(grpc_call_element *elem) {
  call_data *calld = elem->call_data;
  grpc_subchannel_call *subchannel_call;

  /* if the call got activated, we need to destroy the child stack also, and
     remove it from the in-flight requests tracked by the child_entry we
     picked */
  gpr_mu_lock(&calld->mu_state);
  switch (calld->state) {
    case CALL_ACTIVE:
      subchannel_call = calld->subchannel_call;
      gpr_mu_unlock(&calld->mu_state);
      GRPC_SUBCHANNEL_CALL_UNREF(exec_ctx, subchannel_call, "client_microchannel");
      break;
    case CALL_CREATED:
    case CALL_CANCELLED:
      gpr_mu_unlock(&calld->mu_state);
      break;
    case CALL_WAITING_FOR_CALL:
    case CALL_WAITING_FOR_SEND:
      gpr_log(GPR_ERROR, "should never reach here");
      abort();
      break;
  }
}

/* Constructor for channel_data */
static void cmc_init_channel_elem(grpc_channel_element *elem, grpc_channel *master,
                              const grpc_channel_args *args,
                              grpc_mdctx *metadata_context, int is_first,
                              int is_last) {
  size_t i;
  int subchannel_pointer_arg_found = 0;
  channel_data *chand = elem->channel_data;

  memset(chand, 0, sizeof(*chand));

  grpc_iomgr_closure_init(&chand->foo_cb, foo, chand);
  GPR_ASSERT(is_last);
  GPR_ASSERT(elem->filter == &grpc_client_microchannel_filter);

  gpr_mu_init(&chand->mu_config);
  chand->mdctx = metadata_context;
  chand->master = master;
  grpc_pollset_set_init(&chand->pollset_set);
  for (i = 0; i < args->num_args; i++) {
    if (args->args[i].type == GRPC_ARG_POINTER &&
        strcmp(GRPC_MICROCHANNEL_SUBCHANNEL_ARG, args->args[i].key) == 0) {
      subchannel_pointer_arg_found = 1;
      break;
    }
  }
  GPR_ASSERT(subchannel_pointer_arg_found != 0);
  GPR_ASSERT(i < args->num_args);
  GPR_ASSERT(args->args[i].value.pointer.p != NULL);
  chand->subchannel =
      args->args[i].value.pointer.copy(args->args[i].value.pointer.p);
  gpr_log(GPR_DEBUG, "MICRO CHANNEL CREATED WITH SUBCHANNEL %p",
          chand->subchannel);
  grpc_connectivity_state_init(&chand->state_tracker, GRPC_CHANNEL_IDLE, "client_microchannel");
}

/* Destructor for channel_data */
static void cmc_destroy_channel_elem(grpc_channel_element *elem) {
  channel_data *chand = elem->channel_data;
  grpc_connectivity_state_destroy(&chand->state_tracker);
  grpc_pollset_set_destroy(&chand->pollset_set);
  GRPC_SUBCHANNEL_UNREF(chand->subchannel, "client_microchannel_destroy");
  gpr_mu_destroy(&chand->mu_config);
}

const grpc_channel_filter grpc_client_microchannel_filter = {
    cmc_start_transport_stream_op,
    cmc_start_transport_op,
    sizeof(call_data),
    cmc_init_call_elem,
    cmc_destroy_call_elem,
    sizeof(channel_data),
    cmc_init_channel_elem,
    cmc_destroy_channel_elem,
    cmc_get_peer,
    "client-microchannel",
};

grpc_connectivity_state grpc_client_microchannel_check_connectivity_state(
    grpc_channel_element *elem, int try_to_connect) {
  channel_data *chand = elem->channel_data;
  grpc_connectivity_state out;
  gpr_mu_lock(&chand->mu_config);
  out = grpc_connectivity_state_check(&chand->state_tracker);
  if (out == GRPC_CHANNEL_IDLE && try_to_connect) {
    grpc_connectivity_state_set(&chand->state_tracker, out,
                                "microchannel_connecting_changed");
    chand->bla = out;
    grpc_subchannel_notify_on_state_change(chand->subchannel, &chand->bla, &chand->foo_cb);
  }
  gpr_mu_unlock(&chand->mu_config);
  return out;
}

void grpc_client_microchannel_watch_connectivity_state(
    grpc_channel_element *elem, grpc_connectivity_state *state,
    grpc_iomgr_closure *on_complete) {
  channel_data *chand = elem->channel_data;
  gpr_mu_lock(&chand->mu_config);
  grpc_connectivity_state_notify_on_state_change(&chand->state_tracker, state,
                                                 on_complete);
  gpr_mu_unlock(&chand->mu_config);
}

grpc_pollset_set *grpc_client_microchannel_get_connecting_pollset_set(
    grpc_channel_element *elem) {
  channel_data *chand = elem->channel_data;
  return &chand->pollset_set;
}

void grpc_client_microchannel_add_interested_party(grpc_channel_element *elem,
                                                   grpc_pollset *pollset) {
  channel_data *chand = elem->channel_data;
  grpc_pollset_set_add_pollset(&chand->pollset_set, pollset);
}

void grpc_client_microchannel_del_interested_party(grpc_channel_element *elem,
                                                   grpc_pollset *pollset) {
  channel_data *chand = elem->channel_data;
  grpc_pollset_set_del_pollset(&chand->pollset_set, pollset);
}

static void *subchannel_arg_copy(void *subchannel) {
  GRPC_SUBCHANNEL_REF(subchannel, "client_microchannel_arg_copy");
  return subchannel;
}

static void subchannel_arg_destroy(void *subchannel) {
  GRPC_SUBCHANNEL_UNREF(subchannel, "client_microchannel_arg_destroy");
}

grpc_channel *grpc_client_microchannel_create(grpc_subchannel *subchannel,
                                              grpc_channel_args *args) {
  grpc_channel *channel = NULL;
#define MAX_FILTERS 3
  const grpc_channel_filter *filters[MAX_FILTERS];
  grpc_mdctx *mdctx = grpc_subchannel_get_mdctx(subchannel);
  grpc_channel *master = grpc_subchannel_get_master(subchannel);
  char* target = grpc_channel_get_target(master);
  size_t n = 0;
  grpc_arg tmp;
  grpc_channel_args *args_with_subchannel;

  grpc_mdctx_ref(mdctx);
  if (grpc_channel_args_is_census_enabled(args)) {
    filters[n++] = &grpc_client_census_filter;
  }
  filters[n++] = &grpc_compress_filter;
  filters[n++] = &grpc_client_microchannel_filter;
  GPR_ASSERT(n <= MAX_FILTERS);

  tmp.type = GRPC_ARG_POINTER;
  tmp.key = GRPC_MICROCHANNEL_SUBCHANNEL_ARG;
  tmp.value.pointer.p = subchannel;
  tmp.value.pointer.copy = subchannel_arg_copy;
  tmp.value.pointer.destroy = subchannel_arg_destroy;
  args_with_subchannel = grpc_channel_args_copy_and_add(args, &tmp, 1);

  channel = grpc_channel_create_from_filters(target, filters, n,
                                             args_with_subchannel, mdctx, 1);
  gpr_free(target);
  grpc_channel_args_destroy(args_with_subchannel);
  return channel;
}
