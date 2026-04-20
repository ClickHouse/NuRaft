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
    , supports_pipelining_(rpc_->supports_pipelining())
    , state_(cs_new<State>())
{}

void client_req_stream::append(std::vector< ptr<buffer> > logs)
{
    ptr<req_msg> req = cs_new<req_msg>(
        /*term=*/ stream_term_, msg_type::client_request,
        /*src=*/ 0, /*dst=*/ 0,
        /*last_log_term=*/ 0, /*last_log_idx=*/ 0, /*commit_idx=*/ 0);
    req->log_entries().reserve(logs.size());
    for (auto& buf : logs) {
        buf->pos(0);
        req->log_entries().push_back(
            cs_new<log_entry>(0, buf, log_val_type::app_log));
    }

    rpc_handler handler =
        [state = state_]
        (ptr<resp_msg>& resp, ptr<rpc_exception>& err) {
            const bool non_ok = err
                || !resp
                || !resp->get_accepted()
                || resp->get_result_code() != cmd_result_code::OK;
            if (non_ok) state->abandoned_.store(true);
            size_t prev_in_flight = state->in_flight_.fetch_sub(1);
            assert(prev_in_flight > 0);
            (void)prev_in_flight;
        };
    state_->in_flight_.fetch_add(1);
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