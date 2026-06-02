// SPDX-License-Identifier: Apache-2.0

#include "client_req_stream.hxx"

#include "log_val_type.hxx"
#include "msg_type.hxx"
#include "raft_server.hxx"
#include "rpc_cli.hxx"

namespace nuraft {

client_req_stream::client_req_stream(raft_server& srv,
                                     uint64_t stream_term,
                                     ptr<rpc_client> rpc,
                                     uint64_t send_timeout_ms)
    : srv_(&srv)
    , stream_term_(stream_term)
    , rpc_(rpc)
    , send_timeout_ms_(send_timeout_ms)
    , supports_pipelining_(rpc_ ? rpc_->supports_pipelining() : true)
    , state_(cs_new<State>())
{}

void client_req_stream::append(std::vector< ptr<buffer> > logs)
{
    if (state_->abandoned_.load()) {
        // If leader rejected a previous append, drop all subsequent ones.
        return;
    }

    ptr<req_msg> req = cs_new<req_msg>(
        /*term=*/ stream_term_, msg_type::client_request,
        /*src=*/ 0, /*dst=*/ 0,
        /*last_log_term=*/ 0, /*last_log_idx=*/ 0, /*commit_idx=*/ 0);
    // CLOSE_ON_ERROR is required to avoid skipping/reordering if the leader
    // rejects a request (e.g. because of max_uncommitted_log_entries_)
    // but accepts the next one.
    req->set_extra_flags(req->get_extra_flags() | req_msg::CLOSE_ON_ERROR);
    req->log_entries().reserve(logs.size());
    for (auto& buf : logs) {
        buf->pos(0);
        req->log_entries().push_back(
            cs_new<log_entry>(0, buf, log_val_type::app_log));
    }

    state_->in_flight_.fetch_add(1);

    auto handle_result = [state = state_](bool success)
    {
        if (!success) state->abandoned_.store(true);
        size_t prev_in_flight = state->in_flight_.fetch_sub(1);
        assert(prev_in_flight > 0);
        (void)prev_in_flight;
    };

    if (!rpc_) {
        // Leader is on current node. Pass the request to it directly without
        // going through network. This is mostly to allow running a single-node
        // cluster without loopback network available.
        ptr<resp_msg> resp;
        try {
            resp = process_req(srv_, *req);
        } catch (...) {
            resp.reset();
        }

        if (resp && resp->has_async_cb()) {
            ptr< cmd_result< ptr<buffer> > > ret = resp->call_async_cb();
            ret->when_ready(
                [handle_result = std::move(handle_result)]
                ( cmd_result<ptr<buffer>, ptr<std::exception>>& res,
                    ptr<std::exception>& exp ) {
                    bool success = !exp && res.get_accepted() &&
                        res.get_result_code() == cmd_result_code::OK;
                    handle_result(success);
                }
            );
        }
        else
        {
            if (resp && resp->has_cb())
                resp = resp->call_cb(resp);

            bool success = resp && resp->get_accepted() &&
                resp->get_result_code() == cmd_result_code::OK;
            handle_result(success);
        }
        return;
    }

    rpc_handler handler =
        [handle_result = std::move(handle_result)]
        (ptr<resp_msg>& resp, ptr<rpc_exception>& err) {
            bool success = !err && resp && resp->get_accepted() &&
                resp->get_result_code() == cmd_result_code::OK;
            handle_result(success);
        };
    rpc_->send(req, handler, send_timeout_ms_);
}

bool client_req_stream::is_abandoned() const
{
    return state_->abandoned_.load() || srv_->get_term() != stream_term_;
}

bool client_req_stream::is_ready() const
{
    if (supports_pipelining_) {
        return true;
    }
    return is_idle();
}

bool client_req_stream::is_idle() const
{
    return state_->in_flight_.load() == 0;
}

} // namespace nuraft
