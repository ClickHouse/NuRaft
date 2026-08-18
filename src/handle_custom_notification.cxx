/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.

Original Copyright:
See URL: https://github.com/datatechnology/cornerstone

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include "handle_custom_notification.hxx"

#include "buffer_serializer.hxx"
#include "callback.hxx"
#include "error_code.hxx"
#include "peer.hxx"
#include "raft_server.hxx"
#include "req_msg.hxx"
#include "resp_msg.hxx"
#include "tracer.hxx"

#include <cassert>
#include <cstring>

namespace nuraft {

// --- custom_notification_msg ---

ptr<custom_notification_msg> custom_notification_msg::deserialize(buffer& buf) {
    ptr<custom_notification_msg> ret = cs_new<custom_notification_msg>();

    buffer_serializer bs(buf);
    uint8_t version = bs.get_u8();
    (void)version;
    ret->type_ = static_cast<custom_notification_msg::type>(bs.get_u8());

    size_t buf_len = 0;
    void* ptr = bs.get_bytes(buf_len);

    if (buf_len) {
        ret->ctx_ = buffer::alloc(buf_len);
        memcpy(ret->ctx_->data_begin(), ptr, buf_len);
    } else {
        ret->ctx_ = nullptr;
    }

    return ret;
}

ptr<buffer> custom_notification_msg::serialize() const {
    //   << Format >>
    // version          1 byte
    // type             1 byte
    // ctx length (X)   4 bytes
    // ctx              X bytes

    const uint8_t CURRENT_VERSION = 0x0;

    size_t len = sizeof(uint8_t) +
                 sizeof(uint8_t) +
                 sizeof(uint32_t) +
                 ( (ctx_) ? ctx_->size() : 0 );

    ptr<buffer> ret = buffer::alloc(len);
    buffer_serializer bs(ret);
    bs.put_u8(CURRENT_VERSION);
    bs.put_u8(type_);
    if (ctx_) {
        bs.put_bytes(ctx_->data_begin(), ctx_->size());
    } else {
        bs.put_u32(0);
    }

    return ret;
}


// --- out_of_log_msg ---

ptr<out_of_log_msg> out_of_log_msg::deserialize(buffer& buf) {
    ptr<out_of_log_msg> ret = cs_new<out_of_log_msg>();

    buffer_serializer bs(buf);
    uint8_t version = bs.get_u8();
    (void)version;
    ret->start_idx_of_leader_ = bs.get_u64();
    return ret;
}

ptr<buffer> out_of_log_msg::serialize() const {
    //   << Format >>
    // version                      1 byte
    // start log index of leader    8 bytes
    size_t len = sizeof(uint8_t) + sizeof(ulong);
    ptr<buffer> ret = buffer::alloc(len);

    const uint8_t CURRENT_VERSION = 0x0;
    buffer_serializer bs(ret);
    bs.put_u8(CURRENT_VERSION);
    bs.put_u64(start_idx_of_leader_);
    return ret;
}


// --- force_vote_msg ---

ptr<force_vote_msg> force_vote_msg::deserialize(buffer& buf) {
    ptr<force_vote_msg> ret = cs_new<force_vote_msg>();
    buffer_serializer bs(buf);
    uint8_t version = bs.get_u8();
    (void)version;
    return ret;
}

ptr<buffer> force_vote_msg::serialize() const {
    //   << Format >>
    // version                      1 byte
    // ... to be added ...

    size_t len = sizeof(uint8_t);
    ptr<buffer> ret = buffer::alloc(len);

    const uint8_t CURRENT_VERSION = 0x0;
    buffer_serializer bs(ret);
    bs.put_u8(CURRENT_VERSION);
    return ret;
}


// --- full_consensus_mode_msg ---

ptr<full_consensus_mode_msg> full_consensus_mode_msg::deserialize(buffer& buf) {
    ptr<full_consensus_mode_msg> ret = cs_new<full_consensus_mode_msg>();
    buffer_serializer bs(buf);
    uint8_t version = bs.get_u8();
    (void)version;
    ret->enable_ = bs.get_u8() != 0;
    return ret;
}

ptr<buffer> full_consensus_mode_msg::serialize() const {
    //   << Format >>
    // version                      1 byte
    // enable flag                  1 byte

    size_t len = sizeof(uint8_t) + sizeof(uint8_t);
    ptr<buffer> ret = buffer::alloc(len);

    const uint8_t CURRENT_VERSION = 0x0;
    buffer_serializer bs(ret);
    bs.put_u8(CURRENT_VERSION);
    bs.put_u8(enable_ ? 1 : 0);
    return ret;
}


// --- handlers ---

ptr<resp_msg> raft_server::handle_custom_notification_req(req_msg& req) {
    ptr<resp_msg> resp = cs_new<resp_msg>( state_->get_term(),
                                           msg_type::custom_notification_response,
                                           id_,
                                           req.get_src(),
                                           log_store_->next_slot() );
    resp->accept(log_store_->next_slot());

    std::vector< ptr<log_entry> >& log_entries = req.log_entries();
    if (!log_entries.size()) {
        // Empty message, just return.
        return resp;
    }

    ptr<log_entry> msg_le = log_entries[0];
    ptr<buffer> buf = msg_le->get_buf_ptr();
    if (!buf) return resp;

    ptr<custom_notification_msg> msg = custom_notification_msg::deserialize(*buf);

    switch (msg->type_) {
    case custom_notification_msg::out_of_log_range_warning: {
        return handle_out_of_log_msg(req, msg, resp);
    }
    case custom_notification_msg::leadership_takeover: {
        return handle_leadership_takeover(req, msg, resp);
    }
    case custom_notification_msg::request_resignation: {
        return handle_resignation_request(req, msg, resp);
    }
    case custom_notification_msg::request_leadership: {
        return handle_request_leadership_request(req, msg, resp);
    }
    case custom_notification_msg::set_full_consensus_mode: {
        return handle_full_consensus_mode_request(req, msg, resp);
    }
    default:
        break;
    }

    return resp;
}

ptr<resp_msg> raft_server::handle_out_of_log_msg(req_msg& req,
                                                 ptr<custom_notification_msg> msg,
                                                 ptr<resp_msg> resp)
{
    static timer_helper msg_timer(5000000);
    int log_lv = msg_timer.timeout_and_reset() ? L_WARN : L_TRACE;

    // As it is a special form of heartbeat, need to update term.
    update_term(req.get_term());

    out_of_log_range_ = true;

    ptr<out_of_log_msg> ool_msg = out_of_log_msg::deserialize(*msg->ctx_);
    p_lv(log_lv, "this node is out of log range. leader's start index: %" PRIu64 ", "
         "my last index: %" PRIu64,
         ool_msg->start_idx_of_leader_,
         log_store_->next_slot() - 1);

    // Should restart election timer to avoid initiating false vote.
    if ( req.get_term() == state_->get_term() &&
         role_ == srv_role::follower ) {
        restart_election_timer();
    }

    cb_func::Param param(id_, leader_);
    cb_func::OutOfLogRangeWarningArgs args(ool_msg->start_idx_of_leader_);
    param.ctx = &args;
    ctx_->cb_func_.call(cb_func::OutOfLogRangeWarning, &param);

    return resp;
}

ptr<resp_msg> raft_server::handle_leadership_takeover
                           ( req_msg& req,
                             ptr<custom_notification_msg> msg,
                             ptr<resp_msg> resp )
{
    if (is_leader()) {
        p_er("got leadership takeover request from peer %d, "
             "I'm already a leader", req.get_src());
        return resp;
    }
    p_in("[LEADERSHIP TAKEOVER] got request");

    // Initiate force vote (ignoring priority).
    initiate_vote(true);

    // restart the election timer if this is not yet a leader
    if (role_ != srv_role::leader) {
        restart_election_timer();
    }

    return resp;
}

ptr<resp_msg> raft_server::handle_resignation_request
                           ( req_msg& req,
                             ptr<custom_notification_msg> msg,
                             ptr<resp_msg> resp )
{
    if (!is_leader()) {
        p_er("got resignation request from peer %d, "
             "but I'm not a leader", req.get_src());
        return resp;
    }
    p_in("[RESIGNATION REQUEST] got request");

    yield_leadership(false, req.get_src());
    return resp;
}

ptr<resp_msg> raft_server::handle_request_leadership_request
                           ( req_msg& req,
                             ptr<custom_notification_msg> msg,
                             ptr<resp_msg> resp )
{
    p_in("[REQUEST LEADER REQUEST] got request");

    request_leadership();

    return resp;
}

ptr<resp_msg> raft_server::handle_full_consensus_mode_request
                           ( req_msg& req,
                             ptr<custom_notification_msg> msg,
                             ptr<resp_msg> resp )
{
    if (!msg->ctx_) {
        p_er("[FULL CONSENSUS MODE] got request from peer %d "
             "without context", req.get_src());
        return resp;
    }

    if (req.get_term() < state_->get_term()) {
        // A request with a stale term may come from a leader that
        // has not yet found out that it was deposed. Ignoring it also
        // prevents two servers believing they are leaders from
        // re-broadcasting the mode to each other in a loop.
        p_wn("[FULL CONSENSUS MODE] got request from peer %d "
             "with stale term %" PRIu64 ", my term %" PRIu64 ", ignore it",
             req.get_src(), req.get_term(), state_->get_term());
        return resp;
    }

    // A newer term is proof that this node is behind, and that it is not the
    // leader anymore if it thought it was. Step down before doing anything
    // else, as a deposed leader must not re-broadcast with its stale term.
    update_term(req.get_term());

    ptr<full_consensus_mode_msg> fc_msg =
        full_consensus_mode_msg::deserialize(*msg->ctx_);

    if (is_leader()) {
        // A follower asks this leader to change the mode:
        // apply it and propagate to all peers.
        p_in("[FULL CONSENSUS MODE] got request from peer %d "
             "to turn full consensus mode %s",
             req.get_src(), fc_msg->enable_ ? "ON" : "OFF");
        apply_full_consensus_mode(fc_msg->enable_);
        broadcast_full_consensus_mode(fc_msg->enable_);

    } else if (req.get_src() == leader_ || leader_ == -1) {
        // Propagation from the current leader: apply locally.
        //
        // NOTE: `leader_ == -1` is accepted as well, otherwise every
        //       propagation that races with a leader change would be dropped,
        //       and the mode would systematically diverge right after every
        //       election. In that window the sender cannot be verified to be
        //       the leader, only to be in the current term, which is enough
        //       here: the mode affects availability, not safety, and it is
        //       best-effort by design.
        p_in("[FULL CONSENSUS MODE] leader %d turned "
             "full consensus mode %s",
             req.get_src(), fc_msg->enable_ ? "ON" : "OFF");
        apply_full_consensus_mode(fc_msg->enable_);

    } else {
        p_wn("[FULL CONSENSUS MODE] got request from peer %d, "
             "but this node is not a leader and the request is not "
             "from the current leader %d",
             req.get_src(), leader_.load());
    }

    return resp;
}


void raft_server::handle_custom_notification_resp(resp_msg& resp) {
    if (!resp.get_accepted()) return;

    peer_itor it = peers_.find(resp.get_src());
    if (it == peers_.end()) {
        p_in("the response is from an unknown peer %d", resp.get_src());
        return;
    }
    ptr<peer> p = it->second;

    // NOTE: Only move the next log index forward. A custom notification is not
    //       a log replication request, and its response carries the peer's
    //       current `next_slot`, which is behind the leader's view while the
    //       peer is catching up or receiving a snapshot. Moving the index
    //       backwards here would make the leader re-send log entries it
    //       already sent, or decide that the peer needs a snapshot again and
    //       restart the transfer from the beginning.
    if (resp.get_next_idx() > p->get_next_log_idx()) {
        p->set_next_log_idx(resp.get_next_idx());
        p->reset_cnt_backward_log_probe();
    }
}

} // namespace nuraft;
