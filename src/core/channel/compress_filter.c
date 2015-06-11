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

#include "src/core/channel/compress_filter.h"
#include "src/core/channel/channel_args.h"
#include "src/core/compression/message_compress.h"
#include <grpc/compression.h>
#include <grpc/support/log.h>
#include <grpc/support/slice_buffer.h>

typedef struct call_data {
  gpr_slice_buffer slices;
  int remaining_slices;
  int dont_compress;  /**< whether skip compression for this specific call */
} call_data;

typedef struct channel_data {
  grpc_compression_algorithm compress_algorithm;
} channel_data;

static void compress_send_sb(grpc_compression_algorithm algorithm,
                             gpr_slice_buffer *slices) {
  gpr_slice_buffer tmp;
  grpc_msg_compress(algorithm, slices, &tmp);
  gpr_slice_buffer_swap(slices, &tmp);
  gpr_slice_buffer_destroy(&tmp);
}

static void process_send_ops(grpc_call_element *elem,
                             grpc_stream_op_buffer *send_ops) {
  call_data *calld = elem->call_data;
  channel_data *channeld = elem->channel_data;
  size_t i;

  /* buffer up slices until we've processed all the expected ones (as given by
   * GRPC_OP_BEGIN_MESSAGE) */
  for (i = 0; i < send_ops->nops; ++i) {
    grpc_stream_op *sop = &send_ops->ops[i];
    switch (sop->type) {
      case GRPC_OP_BEGIN_MESSAGE:
        calld->remaining_slices = sop->data.begin_message.length;
        calld->dont_compress =
            !!(sop->data.begin_message.flags & GRPC_WRITE_NO_COMPRESS);
        break;
      case GRPC_OP_SLICE:
        if (calld->dont_compress) return;
        GPR_ASSERT(calld->remaining_slices > 0);
        /* add to calld->slices */
        gpr_slice_buffer_add(&calld->slices, sop->data.slice);
        --(calld->remaining_slices);
        if (--(calld->remaining_slices) == 0) {
          /* compress */
          compress_send_sb(channeld->compress_algorithm, &calld->slices);
        }
        break;
      case GRPC_NO_OP:
      case GRPC_OP_METADATA:
        ;  /* fallthrough, ignore */
    }
  }

  /* at this point, calld->slices contains the *compressed* slices from
   * send_ops->ops[*]->data.slice. We now replace these input slices with the
   * compressed ones. */
  /* Compression mustn't have made things worse */
  GPR_ASSERT(calld->slices.count <= send_ops->nops);
  for (i = 0; i < send_ops->nops; ++i) {
    grpc_stream_op *sop = &send_ops->ops[i];
    switch (sop->type) {
      case GRPC_OP_SLICE:
        gpr_slice_unref(sop->data.slice);
        if (i < calld->slices.count) {
          sop->data.slice = gpr_slice_ref(calld->slices.slices[i]);
        }
        break;
      case GRPC_OP_BEGIN_MESSAGE:
      case GRPC_NO_OP:
      case GRPC_OP_METADATA:
        ;  /* fallthrough, ignore */
    }
  }
}

/* Called either:
     - in response to an API call (or similar) from above, to send something
     - a network event (or similar) from below, to receive something
   op contains type and call direction information, in addition to the data
   that is being sent or received. */
static void compress_start_transport_op(grpc_call_element *elem,
                                    grpc_transport_op *op) {
  if (op->send_ops && op->send_ops->nops > 0) {
    process_send_ops(elem, op->send_ops);
  }

  /* pass control down the stack */
  grpc_call_next_op(elem, op);
}

/* Called on special channel events, such as disconnection or new incoming
   calls on the server */
static void channel_op(grpc_channel_element *elem,
                       grpc_channel_element *from_elem, grpc_channel_op *op) {
  /* XXX anything I should care about here? */
  switch (op->type) {
    default:
      /* pass control up or down the stack depending on op->dir */
      grpc_channel_next_op(elem, op);
      break;
  }
}

/* Constructor for call_data */
static void init_call_elem(grpc_call_element *elem,
                           const void *server_transport_data,
                           grpc_transport_op *initial_op) {
  /* grab pointers to our data from the call element */
  call_data *calld = elem->call_data;

  /* initialize members */
  gpr_slice_buffer_init(&calld->slices);
  calld->dont_compress = initial_op->dont_compress;

  if (initial_op) {
    if (initial_op->send_ops && initial_op->send_ops->nops > 0) {
      process_send_ops(elem, initial_op->send_ops);
    }
  }
}

/* Destructor for call_data */
static void destroy_call_elem(grpc_call_element *elem) {
  /* grab pointers to our data from the call element */
  call_data *calld = elem->call_data;
  gpr_slice_buffer_destroy(&calld->slices);
}

/* Constructor for channel_data */
static void init_channel_elem(grpc_channel_element *elem,
                              const grpc_channel_args *args, grpc_mdctx *mdctx,
                              int is_first, int is_last) {
  channel_data *channeld = elem->channel_data;
  channeld->compress_algorithm =
      grpc_channel_args_get_compression_algorithm(args);
  /*We shouldn't be in this filter if compression is disabled. */
  GPR_ASSERT(channeld->compress_algorithm != GRPC_COMPRESS_NONE);

  /* The first and the last filters tend to be implemented differently to
     handle the case that there's no 'next' filter to call on the up or down
     path */
  GPR_ASSERT(!is_first);
  GPR_ASSERT(!is_last);
}

/* Destructor for channel data */
static void destroy_channel_elem(grpc_channel_element *elem) {
}

const grpc_channel_filter grpc_compress_filter = {
    compress_start_transport_op, channel_op, sizeof(call_data), init_call_elem,
    destroy_call_elem, sizeof(channel_data), init_channel_elem,
    destroy_channel_elem, "compress"};
