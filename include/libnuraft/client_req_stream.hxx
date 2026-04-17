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

#ifndef _CLIENT_REQ_STREAM_HXX_
#define _CLIENT_REQ_STREAM_HXX_

#include "async.hxx"
#include "buffer.hxx"
#include "ptr.hxx"

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

namespace nuraft {

class raft_server;
class rpc_client;

/**
 * Single-channel, ordered, fail-closed stream of client_request batches to
 * the current leader. See `docs/stream_forwarding.md` for semantics,
 * preconditions, and the no-gap invariant.
 *
 * Obtain via `raft_server::open_client_req_stream`.
 */
class client_req_stream {
public:
    // Prefer `raft_server::open_client_req_stream`. `rpc == nullptr`
    // selects the local fast path (delegates to `append_entries_ext`).
    client_req_stream(wptr<raft_server> srv,
                      int32_t local_id,
                      uint64_t fenced_term,
                      ptr<rpc_client> rpc = nullptr,
                      uint64_t send_timeout_ms = 0);

    ~client_req_stream();

    /**
     * True once the stream is broken — either latched by `append` after
     * a stream-fatal outcome, or observed live when the local
     * `raft_server`'s term has diverged from the fenced term. Pure
     * observation, safe from any thread, cheap (two atomic loads).
     */
    bool is_broken() const;

    /**
     * Append a batch. **Not thread-safe**: callers must serialize
     * `append` calls on the same stream. Ordering across serialized
     * calls is preserved.
     *
     * Result contract: `accepted=true, OK` on success; otherwise
     * `accepted=false` with a non-OK `result_code`. Callers should
     * distinguish `CANCELLED` (client-side short-circuit when already
     * broken, no I/O) from anything else (leader's verbatim code on
     * remote rejection, or `FAILED` on transport exception).
     *
     * On the local path, if `append_entries_ext` throws the stream is
     * latched broken before the exception propagates.
     */
    ptr< cmd_result< ptr<buffer> > >
        append(std::vector< ptr<buffer> > logs);

private:
    ptr< cmd_result< ptr<buffer> > >
        append_local(std::vector< ptr<buffer> > logs);
    ptr< cmd_result< ptr<buffer> > >
        append_remote(std::vector< ptr<buffer> > logs);

    // Shared with in-flight completion callbacks so they can still flip
    // `broken_` after the stream is destroyed. `srv_` is weak so a stream
    // outliving the server becomes permanently broken rather than
    // dangling.
    struct shared_state {
        std::atomic<bool> broken_{false};
        uint64_t fenced_term_;
        int32_t local_id_;          // stamped as `src` on forwarded reqs
        wptr<raft_server> srv_;
    };

    ptr<rpc_client> rpc_;   // null for local path
    uint64_t send_timeout_ms_;
    ptr<shared_state> state_;
};

} // namespace nuraft

#endif // _CLIENT_REQ_STREAM_HXX_
