/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

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

#include "peer.hxx"

#include "debugging_options.hxx"
#include "raft_server.hxx"
#include "tracer.hxx"

#include <unordered_set>

namespace nuraft {

void peer::send_req( ptr<peer> myself,
                     ptr<req_msg>& req,
                     rpc_handler& handler,
                     bool streaming )
{
    if (abandoned_) {
        p_er("peer %d has been shut down, cannot send request",
             get_config().get_id());
        return;
    }

    if (req) {
        p_ts("send req %d -> %d, type %s",
             req->get_src(),
             req->get_dst(),
             msg_type_to_string( req->get_type() ).c_str() );
    }

    ptr<rpc_result> pending = cs_new<rpc_result>(handler);
    ptr<rpc_client> rpc_local = nullptr;
    {   std::lock_guard<std::mutex> l(rpc_protector_);
        if (!rpc_) {
            // Nothing will be sent, immediately free it
            // to serve next operation.
            p_ts("rpc local is null");
            set_free();
            return;
        }
        rpc_local = rpc_;
    }

    size_t req_size_bytes = 0;
    if (req->get_type() == append_entries_request) {
        for (auto& entry: req->log_entries()) {
            req_size_bytes += entry->get_buf_ptr()->size();
        }
    }

    rpc_handler h = (rpc_handler)std::bind
                    ( &peer::handle_rpc_result,
                      this,
                      myself,
                      rpc_local,
                      req,
                      pending,
                      streaming,
                      req_size_bytes,
                      std::placeholders::_1,
                      std::placeholders::_2 );
    if (rpc_local) {
        myself->bytes_in_flight_add(req_size_bytes);
        rpc_local->send(req, h);
    }
}

// WARNING:
//   We should have the shared pointer of itself (`myself`)
//   and pointer to RPC client (`my_rpc_client`),
//   for the case when
//     1) this peer is removed before this callback function is invoked. OR
//     2) RPC client has been reset and re-connected.
void peer::handle_rpc_result( ptr<peer> myself,
                              ptr<rpc_client> my_rpc_client,
                              ptr<req_msg>& req,
                              ptr<rpc_result>& pending_result,
                              bool streaming,
                              size_t req_size_bytes,
                              ptr<resp_msg>& resp,
                              ptr<rpc_exception>& err )
{
    if (abandoned_) {
        p_in("peer %d has been shut down, ignore response.", get_config().get_id());
        return;
    }

    if (req) {
        p_ts( "resp of req %d -> %d, type %s, %s",
              req->get_src(),
              req->get_dst(),
              msg_type_to_string( req->get_type() ).c_str(),
              (err) ? err->what() : "OK" );
    }

    if (err == nilptr) {
        // Succeeded.
        {   std::lock_guard<std::mutex> l(rpc_protector_);
            // The same as below, freeing busy flag should be done
            // only if the RPC hasn't been changed.
            uint64_t cur_rpc_id = rpc_ ? rpc_->get_id() : 0;
            uint64_t given_rpc_id = my_rpc_client ? my_rpc_client->get_id() : 0;
            if (cur_rpc_id != given_rpc_id) {
                int32_t stale_resps = inc_stale_rpc_responses();
                int32_t limit = raft_server::get_raft_limits().response_limit_;
                if (stale_resps < limit) {
                    p_wn( "[EDGE CASE] got stale RPC response from %d: "
                          "current %p (%" PRIu64 "), from parameter %p (%" PRIu64 "). "
                          "will ignore this response",
                          get_config().get_id(),
                          rpc_.get(),
                          cur_rpc_id,
                          my_rpc_client.get(),
                          given_rpc_id );
                } else if (stale_resps == limit) {
                    p_wn( "[EDGE CASE] too verbose stale RPC response from peer %d, "
                          "will suppress it from now", config_->get_id() );
                }

            } else {
                // WARNING:
                //   `set_free()` should be protected by `rpc_protector_`, otherwise
                //   it may free the peer even though new RPC client is already created.
                reset_stale_rpc_responses();
                bytes_in_flight_sub(req_size_bytes);
                try_set_free(req->get_type(), streaming);
            }
        }

        reset_active_timer();
        {
            auto_lock(lock_);
            resume_hb_speed();
        }
        ptr<rpc_exception> no_except;
        resp->set_peer(myself);
        pending_result->set_result(resp, no_except);

        reconn_backoff_.reset();
        reconn_backoff_.set_duration_ms(1);

    } else {
        // Failed.

        // NOTE: Explicit failure is also treated as an activity
        //       of that connection.
        reset_active_timer();
        {
            auto_lock(lock_);
            slow_down_hb();
        }
        ptr<resp_msg> no_resp;
        pending_result->set_result(no_resp, err);

        // Destroy this connection, we MUST NOT re-use existing socket.
        // Next append operation will create a new one.
        {   std::lock_guard<std::mutex> l(rpc_protector_);
            uint64_t cur_rpc_id = rpc_ ? rpc_->get_id() : 0;
            uint64_t given_rpc_id = my_rpc_client ? my_rpc_client->get_id() : 0;
            if (cur_rpc_id == given_rpc_id) {
                rpc_.reset();
                uint64_t last_streamed_log_idx = get_last_streamed_log_idx();
                reset_stream();
                if (last_streamed_log_idx) {
                    p_in("stop stream mode for peer %d at idx: %" PRIu64 "",
                         config_->get_id(), last_streamed_log_idx);
                }
                reset_stale_rpc_responses();
                reset_bytes_in_flight();
                try_set_free(req->get_type(), streaming);

                // On disconnection, reset `snapshot_sync_is_needed` flag.
                // The first request on the next connection will re-check
                // the flag.
                set_snapshot_sync_is_needed(false);

            } else {
                // WARNING (MONSTOR-9378):
                //   RPC client has been reset before this request returns
                //   error. Those two are different instances and we
                //   SHOULD NOT reset the new one.

                // NOTE: In streaming mode, there can be lots of below errors
                //       at the same time. We should avoid verbose logs.

                int32_t stale_resps = inc_stale_rpc_responses();
                int32_t limit = raft_server::get_raft_limits().response_limit_;
                if (stale_resps < limit) {
                    p_wn( "[EDGE CASE] RPC for %d has been reset before "
                          "returning error: current %p (%" PRIu64
                          "), from parameter %p (%" PRIu64 ")",
                          config_->get_id(),
                          rpc_.get(),
                          cur_rpc_id,
                          my_rpc_client.get(),
                          given_rpc_id );
                } else if (stale_resps == limit) {
                    p_wn( "[EDGE CASE] too verbose stale RPC response from peer %d, "
                          "will suppress it from now", config_->get_id() );
                }
            }
        }
    }
}

void peer::try_set_free(msg_type type, bool streaming) {
    const static std::unordered_set<int> msg_types_to_free( {
        // msg_type::append_entries_request,
        msg_type::install_snapshot_request,
        msg_type::request_vote_request,
        msg_type::pre_vote_request,
        msg_type::leave_cluster_request,
        msg_type::custom_notification_request,
        msg_type::reconnect_request,
        msg_type::priority_change_request
    } );

    if ( msg_types_to_free.find(type) !=
                msg_types_to_free.end() ) {
        set_free();
    }

    if (type == msg_type::append_entries_request && !streaming) {
        set_free();
    }
}

bool peer::recreate_rpc(ptr<srv_config>& config,
                        context& ctx)
{
    if (abandoned_) {
        p_tr("peer %d is abandoned", config->get_id());
        return false;
    }

    ptr<rpc_client_factory> factory = nullptr;
    {   std::lock_guard<std::mutex> l(ctx.ctx_lock_);
        factory = ctx.rpc_cli_factory_;
    }
    if (!factory) {
        p_tr("client factory is empty");
        return false;
    }

    std::lock_guard<std::mutex> l(rpc_protector_);

    bool backoff_timer_disabled =
        debugging_options::get_instance()
        .disable_reconn_backoff_.load(std::memory_order_relaxed);
    if (backoff_timer_disabled) {
        p_tr("reconnection back-off timer is disabled");
    }

    // To avoid too frequent reconnection attempt,
    // we use exponential backoff (x2) from 1 ms to heartbeat interval.
    if (backoff_timer_disabled || reconn_backoff_.timeout()) {
        reconn_backoff_.reset();
        size_t new_duration_ms = reconn_backoff_.get_duration_us() / 1000;
        new_duration_ms = std::min( hb_interval_, (int32)new_duration_ms * 2 );
        if (!new_duration_ms) new_duration_ms = 1;
        reconn_backoff_.set_duration_ms(new_duration_ms);

        rpc_ = factory->create_client(config->get_endpoint());
        p_ts("%p reconnect peer %d", rpc_.get(), get_config().get_id());

        // WARNING:
        //   A reconnection attempt should be treated as an activity,
        //   hence reset timer.
        reset_active_timer();

        reset_stream();
        reset_bytes_in_flight();
        set_free();
        set_manual_free();
        return true;

    } else {
        p_ts("skip reconnect this time");
    }
    return false;
}

void peer::shutdown() {
    // Should set the flag to block all incoming requests.
    abandoned_ = true;

    // Cut off all shared pointers related to ASIO and Raft server.
    scheduler_.reset();
    {   // To guarantee atomic reset
        // (race between send_req()).
        std::lock_guard<std::mutex> l(rpc_protector_);
        rpc_.reset();
    }
    hb_task_.reset();
}

} // namespace nuraft;

