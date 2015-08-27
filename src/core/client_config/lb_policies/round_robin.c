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

#include <stdio.h>

#include "src/core/client_config/lb_policies/round_robin.h"

#include <string.h>

#include <grpc/support/alloc.h>
#include "src/core/transport/connectivity_state.h"

typedef struct pending_pick {
  struct pending_pick *next;
  grpc_pollset *pollset;
  grpc_subchannel **target;
  grpc_iomgr_closure *on_complete;
} pending_pick;

typedef struct connected_list {
  struct connected_list *next;
  struct connected_list *prev;
  grpc_subchannel *subchannel;
} connected_list;

typedef struct {
  /** The index w.r.t. p->subchannels for the associated cb */
  size_t subchannel_idx;
  void *p; /**< round_robin_lb_policy instance */
} connectivity_changed_cb_arg;

typedef struct {
  /** base policy: must be first */
  grpc_lb_policy base;
  /** all our subchannels */
  grpc_subchannel **subchannels;
  size_t num_subchannels;

  /** Callbacks, one per subchannel being watched, to be called when their
   * respective connectivity changes */
  grpc_iomgr_closure *connectivity_changed_cbs;

  /** mutex protecting remaining members */
  gpr_mu mu;
  /** have we started picking? */
  int started_picking;
  /** are we shut down? */
  int shutdown;
  /** Connectivity state of the subchannels being watched */
  grpc_connectivity_state *subchannel_connectivity;
  /** List of picks that are waiting on connectivity */
  pending_pick *pending_picks;

  /** our connectivity state tracker */
  grpc_connectivity_state_tracker state_tracker;

  /** beginning of the list */
  connected_list *connected_list;
  connected_list *connected_list_pick_head;
  connected_list *connected_list_insertion_head;

  /** XXX */
  connected_list **subchannels_to_list_elem;

  /** lastest selected subchannel, if any. NULL otherwise */
  grpc_subchannel *selected;

  connectivity_changed_cb_arg *cb_args;
} round_robin_lb_policy;

/** Returns the next subchannel from the connected list or NULL if the list is
 * empty */
static grpc_subchannel *get_next_connected_subchannel_locked(
    round_robin_lb_policy *p) {
  connected_list *selected;

  selected = p->connected_list_pick_head;
  if (selected == NULL) {
    /* circle around */
    p->connected_list_pick_head = p->connected_list;
  }
  selected = p->connected_list_pick_head;
  if (selected == NULL) {
    /* empty connected list */
    return NULL;
  }
  /* advance picking head */
  p->connected_list_pick_head = selected->next;
  return selected->subchannel;
}

/** Adds the connected subchannel \a csc to \a list.
 *
 * Inserts after p->connected_list_insertion_head. The newly inserted node
 * becomes the new insertion head.
 * */
static connected_list *add_connected_sc_locked(round_robin_lb_policy *p,
                                               grpc_subchannel *csc) {
  connected_list **insert_head = &p->connected_list_insertion_head;
  connected_list *new_elem = gpr_malloc(sizeof(connected_list));
  new_elem->subchannel = csc;
  if (p->connected_list == NULL || *insert_head == NULL) {
    /* first element */
    new_elem->next = NULL;
    new_elem->prev = NULL;
    p->connected_list = new_elem;
    p->connected_list_pick_head = new_elem;
    *insert_head = p->connected_list;
  } else {
    if ((*insert_head)->next == NULL) {
      (*insert_head)->next = new_elem;
      new_elem->next = NULL;
    } else {
      (*insert_head)->next->prev = new_elem;
      new_elem->next = (*insert_head)->next;
      (*insert_head)->next = new_elem;
    }
    new_elem->prev = (*insert_head);

    *insert_head = new_elem;
  }

  if (p->connected_list_pick_head == NULL)
  gpr_log(GPR_INFO, "ADDING %p. INSERT HEAD AT %p. PICK HEAD AT %p", csc,
          (*insert_head)->subchannel, p->connected_list_pick_head);
  else
  gpr_log(GPR_INFO, "ADDING %p. INSERT HEAD AT %p. PICK HEAD AT %p", csc,
          (*insert_head)->subchannel, p->connected_list_pick_head->subchannel);

  return new_elem;
}

/** Removes \a node from the list of connected subchannels */
static void remove_disconnected_sc_locked(round_robin_lb_policy *p,
                                          connected_list *node) {
  /* XXX: what if node is the beginning, insertion or pick point? */
  gpr_log(GPR_INFO, "BAAAAAAAAAAAAAAAR removing %p", node->subchannel);
  if (node == NULL) {
    return;
  }
  if (node == p->connected_list_pick_head) {
    p->connected_list_pick_head = node->next; /* valid even if next is null */
  }
  if (node == p->connected_list_insertion_head) {
    p->connected_list_insertion_head = node->prev; /* valid even if prev is null */
  }

  if (node->prev != NULL) {
    node->prev->next = node->next;
  }
  if (node->next != NULL) {
    node->next->prev = node->prev;
  }
  gpr_free(node);
}


static void del_interested_parties_locked(round_robin_lb_policy *p,
                                          size_t idx) {
  pending_pick *pp;
  for (pp = p->pending_picks; pp; pp = pp->next) {
    grpc_subchannel_del_interested_party(p->subchannels[idx], pp->pollset);
  }
}

/*static void add_interested_parties_locked(round_robin_lb_policy *p,
                                          size_t idx) {
  pending_pick *pp;
  for (pp = p->pending_picks; pp; pp = pp->next) {
    grpc_subchannel_add_interested_party(p->subchannels[idx], pp->pollset);
  }
}*/

void rr_destroy(grpc_lb_policy *pol) {
  round_robin_lb_policy *p = (round_robin_lb_policy *)pol;
  size_t i;
  connected_list *cl_elem;
  for (i = 0; i < p->num_subchannels; i++) {
    del_interested_parties_locked(p, i);
    GRPC_SUBCHANNEL_UNREF(p->subchannels[i], "round_robin");
  }
  gpr_free(p->connectivity_changed_cbs);
  gpr_free(p->subchannel_connectivity);

  grpc_connectivity_state_destroy(&p->state_tracker);
  gpr_free(p->subchannels);
  gpr_mu_destroy(&p->mu);

  while ( (cl_elem = p->connected_list)) {
    p->connected_list = cl_elem->next;
    cl_elem->subchannel = NULL;
    gpr_free(cl_elem);
  }
  gpr_free(p->subchannels_to_list_elem);
  gpr_free(p->cb_args);
  gpr_free(p);
}

void rr_shutdown(grpc_lb_policy *pol) {
  size_t i;
  round_robin_lb_policy *p = (round_robin_lb_policy *)pol;
  pending_pick *pp;
  gpr_mu_lock(&p->mu);

  for (i = 0; i < p->num_subchannels; i++) {
    del_interested_parties_locked(p, i);
  }

  p->shutdown = 1;
  while ((pp = p->pending_picks)) {
    p->pending_picks = pp->next;
    *pp->target = NULL;
    grpc_iomgr_add_delayed_callback(pp->on_complete, 0);
    gpr_free(pp);
  }
  grpc_connectivity_state_set(&p->state_tracker, GRPC_CHANNEL_FATAL_FAILURE,
                              "shutdown");
  gpr_mu_unlock(&p->mu);
}

static void start_picking(round_robin_lb_policy *p) {
  size_t i;
  p->started_picking = 1;

  for (i = 0; i < p->num_subchannels; i++) {
    p->subchannel_connectivity[i] = GRPC_CHANNEL_IDLE;
    grpc_subchannel_notify_on_state_change(p->subchannels[i],
                                           &p->subchannel_connectivity[i],
                                           &p->connectivity_changed_cbs[i]);
    GRPC_LB_POLICY_REF(&p->base, "round_robin_connectivity");
  }
}

void rr_exit_idle(grpc_lb_policy *pol) {
  round_robin_lb_policy *p = (round_robin_lb_policy *)pol;
  gpr_mu_lock(&p->mu);
  if (!p->started_picking) {
    start_picking(p);
  }
  gpr_mu_unlock(&p->mu);
}

void rr_pick(grpc_lb_policy *pol, grpc_pollset *pollset,
             grpc_metadata_batch *initial_metadata, grpc_subchannel **target,
             grpc_iomgr_closure *on_complete) {
  size_t i;
  round_robin_lb_policy *p = (round_robin_lb_policy *)pol;
  pending_pick *pp;
  gpr_mu_lock(&p->mu);
  /* XXX: si ya se ha encontrado resultado, devolverlo e invocar el cb asociado.
   * Esto hay que cambiarlo para que vaya leyendo de connected_list  */
  if ((p->selected = get_next_connected_subchannel_locked(p))) {
    if (p->connected_list_pick_head) {
      gpr_log(GPR_INFO, "PICKED FROM RR_PICK: %p. PICK HEAD AT %p", p->selected, p->connected_list_pick_head->subchannel);
    } else {
      gpr_log(GPR_INFO, "PICKED FROM RR_PICK: %p. PICK HEAD AT %p", p->selected, p->connected_list_pick_head);
    }
    gpr_mu_unlock(&p->mu);
    gpr_log(GPR_INFO, "(RR PICK) SETTING TARGET TO %p", p->selected);
    *target = p->selected;
    on_complete->cb(on_complete->cb_arg, 1);
  } else {
    if (!p->started_picking) {
      start_picking(p);
    }
    for (i = 0; i < p->num_subchannels; i++) {
      grpc_subchannel_add_interested_party(p->subchannels[i], pollset);
    }
    /* XXX: se anyade a la lista circular de "clientes" interesados en los
     * resultados */
    pp = gpr_malloc(sizeof(*pp));
    pp->next = p->pending_picks;
    pp->pollset = pollset;
    pp->target = target;
    pp->on_complete = on_complete;
    p->pending_picks = pp;
    gpr_mu_unlock(&p->mu);
  }
}

static void rr_connectivity_changed(void *arg, int iomgr_success) {
  connectivity_changed_cb_arg *cb_arg = arg;
  round_robin_lb_policy *p = cb_arg->p;
  /* index over p->subchannels of this cb's subchannel */
  const size_t this_idx = cb_arg->subchannel_idx;
  pending_pick *pp;

  int unref = 0;

  /* connectivity state of this cb's subchannel */
  grpc_connectivity_state *this_connectivity;

  gpr_mu_lock(&p->mu);


  this_connectivity =
      &p->subchannel_connectivity[this_idx];

  gpr_log(GPR_INFO, "\tCONNECTIVITY CHANGED FOR %d TO %d", this_idx, *this_connectivity);

  if (p->shutdown) {
    unref = 1;
  } else {
    if (p->selected == p->subchannels[this_idx]) {
      /* connectivity state of the currently selected channel has changed */
      grpc_connectivity_state_set(&p->state_tracker,
                                  p->subchannel_connectivity[this_idx],
                                  "selected_changed");
    }
    switch (*this_connectivity) {
      case GRPC_CHANNEL_READY:
        grpc_connectivity_state_set(&p->state_tracker, GRPC_CHANNEL_READY,
                                    "connecting_ready");
        /* add the newly connected subchannel to the list of connected ones.
         * Note that it goes to the "end of the line". */
        p->subchannels_to_list_elem[this_idx] =
            add_connected_sc_locked(p, p->subchannels[this_idx]);
        /* at this point we know there's at least one suitable subchannel. Go
         * ahead and pick one and notify the pending suitors in
         * p->pending_picks. This preemtively replicates rr_pick()'s actions. */
        /*p->selected = get_next_connected_subchannel_locked(p);*/
        p->selected = p->subchannels[this_idx];
        gpr_log(GPR_INFO, "SELECTED FROM RR_CON_CHANGED: %p", p->selected);
        while ((pp = p->pending_picks)) {
          p->pending_picks = pp->next;
          *pp->target = p->selected;
          gpr_log(GPR_INFO, "(CONN CHANGED) SETTING TARGET TO %p", p->selected);
          grpc_subchannel_del_interested_party(p->selected, pp->pollset);
          grpc_iomgr_add_delayed_callback(pp->on_complete, 1);
          gpr_free(pp);
        }
        grpc_subchannel_notify_on_state_change(
            p->subchannels[this_idx], this_connectivity,
            &p->connectivity_changed_cbs[this_idx]);
        break;
      case GRPC_CHANNEL_CONNECTING:
      case GRPC_CHANNEL_IDLE:
        grpc_connectivity_state_set(&p->state_tracker, *this_connectivity,
                                    "connecting_changed");
        grpc_subchannel_notify_on_state_change(
            p->subchannels[this_idx], this_connectivity,
            &p->connectivity_changed_cbs[this_idx]);
        break;
      case GRPC_CHANNEL_TRANSIENT_FAILURE:
        grpc_connectivity_state_set(&p->state_tracker,
                                    GRPC_CHANNEL_TRANSIENT_FAILURE,
                                    "connecting_transient_failure");
        /* renew state notification */
        grpc_subchannel_notify_on_state_change(
            p->subchannels[this_idx], this_connectivity,
            &p->connectivity_changed_cbs[this_idx]);

        /* remove for now if it was */
        if (p->subchannels_to_list_elem[this_idx] != NULL) {
          del_interested_parties_locked(p, this_idx);
          remove_disconnected_sc_locked(p, p->subchannels_to_list_elem[this_idx]);
          p->subchannels_to_list_elem[this_idx] = NULL;
        }

        break;
      case GRPC_CHANNEL_FATAL_FAILURE:
        if (p->subchannels_to_list_elem[this_idx] != NULL) {
          del_interested_parties_locked(p, this_idx);
          remove_disconnected_sc_locked(p, p->subchannels_to_list_elem[this_idx]);
          p->subchannels_to_list_elem[this_idx] = NULL;
        }

        GPR_SWAP(grpc_subchannel *, p->subchannels[this_idx],
                 p->subchannels[p->num_subchannels - 1]);
        p->num_subchannels--;
        GRPC_SUBCHANNEL_UNREF(p->subchannels[p->num_subchannels],
                              "round_robin");

        if (p->num_subchannels == 0) {
          grpc_connectivity_state_set(&p->state_tracker,
                                      GRPC_CHANNEL_FATAL_FAILURE,
                                      "no_more_channels");
          while ((pp = p->pending_picks)) {
            p->pending_picks = pp->next;
            *pp->target = NULL;
            grpc_iomgr_add_delayed_callback(pp->on_complete, 1);
            gpr_free(pp);
          }
          unref = 1;
        } else {
          grpc_connectivity_state_set(&p->state_tracker,
                                      GRPC_CHANNEL_TRANSIENT_FAILURE,
                                      "subchannel_failed");
        }
    }  /* switch */
  }  /* !unref */

  gpr_mu_unlock(&p->mu);

  if (unref) {
    GRPC_LB_POLICY_UNREF(&p->base, "round_robin_connectivity");
  }
}

static void rr_broadcast(grpc_lb_policy *pol, grpc_transport_op *op) {
  round_robin_lb_policy *p = (round_robin_lb_policy *)pol;
  size_t i;
  size_t n;
  grpc_subchannel **subchannels;

  gpr_mu_lock(&p->mu);
  n = p->num_subchannels;
  subchannels = gpr_malloc(n * sizeof(*subchannels));
  for (i = 0; i < n; i++) {
    subchannels[i] = p->subchannels[i];
    GRPC_SUBCHANNEL_REF(subchannels[i], "rr_broadcast");
  }
  gpr_mu_unlock(&p->mu);

  for (i = 0; i < n; i++) {
    grpc_subchannel_process_transport_op(subchannels[i], op);
    GRPC_SUBCHANNEL_UNREF(subchannels[i], "rr_broadcast");
  }
  gpr_free(subchannels);
}

static grpc_connectivity_state rr_check_connectivity(grpc_lb_policy *pol) {
  round_robin_lb_policy *p = (round_robin_lb_policy *)pol;
  grpc_connectivity_state st;
  gpr_mu_lock(&p->mu);
  st = grpc_connectivity_state_check(&p->state_tracker);
  gpr_mu_unlock(&p->mu);
  return st;
}

static void rr_notify_on_state_change(grpc_lb_policy *pol,
                                      grpc_connectivity_state *current,
                                      grpc_iomgr_closure *notify) {
  round_robin_lb_policy *p = (round_robin_lb_policy *)pol;
  gpr_mu_lock(&p->mu);
  grpc_connectivity_state_notify_on_state_change(&p->state_tracker, current,
                                                 notify);
  gpr_mu_unlock(&p->mu);
}

static const grpc_lb_policy_vtable round_robin_lb_policy_vtable = {
    rr_destroy,
    rr_shutdown,
    rr_pick,
    rr_exit_idle,
    rr_broadcast,
    rr_check_connectivity,
    rr_notify_on_state_change};

grpc_lb_policy *grpc_create_round_robin_lb_policy(grpc_subchannel **subchannels,
                                                 size_t num_subchannels) {
  size_t i;
  round_robin_lb_policy *p = gpr_malloc(sizeof(*p));
  GPR_ASSERT(num_subchannels);
  memset(p, 0, sizeof(*p));
  grpc_lb_policy_init(&p->base, &round_robin_lb_policy_vtable);
  p->subchannels = gpr_malloc(sizeof(grpc_subchannel *) * num_subchannels);
  p->num_subchannels = num_subchannels;
  grpc_connectivity_state_init(&p->state_tracker, GRPC_CHANNEL_IDLE,
                               "round_robin");
  memcpy(p->subchannels, subchannels,
         sizeof(grpc_subchannel *) * num_subchannels);

  gpr_mu_init(&p->mu);
  p->connectivity_changed_cbs =
      gpr_malloc(sizeof(grpc_iomgr_closure) * num_subchannels);
  p->subchannel_connectivity =
      gpr_malloc(sizeof(grpc_connectivity_state) * num_subchannels);

  p->cb_args =
      gpr_malloc(sizeof(connectivity_changed_cb_arg) * num_subchannels);
  for(i = 0; i < num_subchannels; i++) {
    p->cb_args[i].subchannel_idx = i;
    p->cb_args[i].p = p;
    grpc_iomgr_closure_init(&p->connectivity_changed_cbs[i],
                            rr_connectivity_changed, &p->cb_args[i]);
  }

  p->connected_list = NULL;
  p->connected_list_pick_head = NULL;
  p->connected_list_insertion_head = NULL;
  p->subchannels_to_list_elem =
      gpr_malloc(sizeof(grpc_subchannel *) * num_subchannels);
  return &p->base;
}
