/************************************************************************
Copyright 2024-present ClickHouse, Inc.

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

#include "client_req_stream.hxx"

#include "cluster_config.hxx"
#include "context.hxx"
#include "log_val_type.hxx"
#include "msg_type.hxx"
#include "raft_server.hxx"
#include "req_msg.hxx"
#include "resp_msg.hxx"
#include "rpc_cli.hxx"
#include "rpc_cli_factory.hxx"
#include "rpc_exception.hxx"
#include "srv_config.hxx"
#include "tracer.hxx"

namespace nuraft {

namespace {

ptr< cmd_result< ptr<buffer> > > make_ready(cmd_result_code code, bool accepted) {
    auto res = cs_new< cmd_result< ptr<buffer> > >();
    if (accepted) {
        res->accept();
    }
    ptr<buffer> nil_buf;
    ptr<std::exception> nil_err;
    res->set_result(nil_buf, nil_err, code);
    return res;
}

} // anonymous namespace

client_req_stream::client_req_stream(wptr<raft_server> srv,
                                     int32_t local_id,
                                     uint64_t fenced_term,
                                     ptr<rpc_client> rpc,
                                     uint64_t send_timeout_ms)
    : rpc_(rpc)
    , send_timeout_ms_(send_timeout_ms)
    , state_(cs_new<shared_state>())
{
    state_->srv_ = std::move(srv);
    state_->local_id_ = local_id;
    state_->fenced_term_ = fenced_term;
}

client_req_stream::~client_req_stream() = default;

bool client_req_stream::is_broken() const {
    if (state_->broken_.load(std::memory_order_acquire)) return true;
    auto srv = state_->srv_.lock();
    if (!srv) return true;
    return srv->get_term() != state_->fenced_term_;
}

ptr< cmd_result< ptr<buffer> > >
client_req_stream::append(std::vector< ptr<buffer> > logs)
{
    if (is_broken()) {
        state_->broken_.store(true, std::memory_order_release);
        return make_ready(cmd_result_code::CANCELLED, /*accepted=*/false);
    }
    // Empty batches are a no-op; avoid append_entries_ext's accepted=false
    // early-return that would wrongly mark the stream broken.
    if (logs.empty()) {
        return make_ready(cmd_result_code::OK, /*accepted=*/true);
    }
    return rpc_ ? append_remote(std::move(logs))
                : append_local(std::move(logs));
}

ptr< cmd_result< ptr<buffer> > >
client_req_stream::append_local(std::vector< ptr<buffer> > logs)
{
    auto srv = state_->srv_.lock();
    if (!srv) {
        state_->broken_.store(true, std::memory_order_release);
        return make_ready(cmd_result_code::CANCELLED, /*accepted=*/false);
    }

    raft_server::req_ext_params ext;
    ext.expected_term_ = state_->fenced_term_;

    ptr< cmd_result< ptr<buffer> > > res;
    try {
        res = srv->append_entries_ext(logs, ext);
    } catch (...) {
        // Latch broken before rethrowing — a mid-batch throw from
        // store_log_entry may leave a partial prefix in the log.
        state_->broken_.store(true, std::memory_order_release);
        throw;
    }

    auto state = state_;
    res->when_ready(
        [state]
        (cmd_result< ptr<buffer> >& r, ptr<std::exception>&) {
            if (!r.get_accepted() ||
                r.get_result_code() != cmd_result_code::OK) {
                state->broken_.store(true, std::memory_order_release);
            }
        });
    return res;
}

ptr< cmd_result< ptr<buffer> > >
client_req_stream::append_remote(std::vector< ptr<buffer> > logs)
{
    // term = fenced term (see WARNING on STREAM_FORWARDING_REQUEST in req_msg.hxx).
    ptr<req_msg> req = cs_new<req_msg>(
        state_->fenced_term_, msg_type::client_request,
        /*src*/ state_->local_id_, /*dst*/ 0,
        /*last_log_term*/ 0, /*last_log_idx*/ 0, /*commit_idx*/ 0);
    req->set_extra_flags(req_msg::STREAM_FORWARDING_REQUEST);
    req->log_entries().reserve(logs.size());
    for (auto& buf : logs) {
        buf->pos(0);
        req->log_entries().push_back(
            cs_new<log_entry>(0, buf, log_val_type::app_log));
    }

    auto res = cs_new< cmd_result< ptr<buffer> > >();
    auto state = state_;
    // One-shot latch so a partially-queued send can't let both the handler
    // and the catch block complete `res`.
    auto completed = cs_new<std::atomic<bool>>(false);
    auto try_complete =
        [res](std::atomic<bool>& once, ptr<buffer> buf,
              ptr<std::exception> err, cmd_result_code code) {
            bool expected = false;
            if (once.compare_exchange_strong(expected, true)) {
                res->set_result(buf, err, code);
            }
        };

    rpc_handler handler =
        [res, state, completed, try_complete]
        (ptr<resp_msg>& resp, ptr<rpc_exception>& err) {
            ptr<buffer> ret_buf;
            ptr<std::exception> ret_err =
                err ? std::static_pointer_cast<std::exception>(err) : nullptr;
            cmd_result_code code = cmd_result_code::OK;

            if (err || !resp) {
                state->broken_.store(true, std::memory_order_release);
                code = cmd_result_code::FAILED;
            } else if (!resp->get_accepted()) {
                state->broken_.store(true, std::memory_order_release);
                code = resp->get_result_code();
                // Invariant: accepted=false always carries non-OK
                // (reject_cli_req). OK here means protocol violation.
                if (code == cmd_result_code::OK) code = cmd_result_code::FAILED;
            } else {
                res->accept();
                ret_buf = resp->get_ctx();
            }

            try_complete(*completed, ret_buf, ret_err, code);
        };
    try {
        rpc_->send(req, handler, send_timeout_ms_);
    } catch (...) {
        // No-op if the handler already ran (latched by `completed`).
        state_->broken_.store(true, std::memory_order_release);
        try_complete(*completed, ptr<buffer>{}, ptr<std::exception>{},
                     cmd_result_code::FAILED);
    }
    return res;
}

// ---- factory -------------------------------------------------------------

ptr<client_req_stream> raft_server::open_client_req_stream(
    uint64_t send_timeout_ms,
    cmd_result_code* out_err)
{
    auto fail = [&](cmd_result_code code) -> ptr<client_req_stream> {
        if (out_err) *out_err = code;
        return nullptr;
    };

    ptr<cluster_config> cfg = get_config();
    if (!cfg || !cfg->is_async_replication()) {
        p_wn("open_client_req_stream: refused - async_replication is disabled");
        return fail(cmd_result_code::CANCELLED);
    }

    // leader/term reads aren't atomic together; a stale snapshot self-heals
    // on the first append via the term fence.
    int32 leader = get_leader();
    if (leader == -1) {
        p_wn("open_client_req_stream: refused - no current leader");
        return fail(cmd_result_code::NOT_LEADER);
    }

    ulong term = get_term();
    if (term == 0) {
        p_wn("open_client_req_stream: refused - raft state not initialized");
        return fail(cmd_result_code::NOT_LEADER);
    }

    wptr<raft_server> srv_weak = this->shared_from_this();

    if (leader == id_) {
        return cs_new<client_req_stream>(srv_weak, id_, term);
    }

    ptr<srv_config> srv_cfg = cfg->get_server(leader);
    if (!srv_cfg) {
        p_wn("open_client_req_stream: refused - leader %d not in cluster config",
             leader);
        return fail(cmd_result_code::FAILED);
    }

    ptr<rpc_client> rpc =
        ctx_->rpc_cli_factory_->create_client(srv_cfg->get_endpoint());
    if (!rpc) {
        p_wn("open_client_req_stream: refused - rpc_client creation failed "
             "for endpoint %s", srv_cfg->get_endpoint().c_str());
        return fail(cmd_result_code::FAILED);
    }
    // Without pipelining, a second in-flight append hits a busy-socket assert.
    if (!rpc->supports_pipelining()) {
        p_wn("open_client_req_stream: refused - rpc_client for endpoint %s "
             "does not support pipelining (streaming_mode_ not enabled?)",
             srv_cfg->get_endpoint().c_str());
        return fail(cmd_result_code::FAILED);
    }

    return cs_new<client_req_stream>(
        srv_weak, id_, term, rpc, send_timeout_ms);
}

} // namespace nuraft
