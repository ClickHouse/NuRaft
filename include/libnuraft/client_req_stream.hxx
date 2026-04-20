// SPDX-License-Identifier: Apache-2.0

#ifndef _CLIENT_REQ_STREAM_HXX_
#define _CLIENT_REQ_STREAM_HXX_

#include "async.hxx"
#include "buffer.hxx"
#include "ptr.hxx"

#include <atomic>
#include <cstdint>
#include <vector>

namespace nuraft {

class raft_server;
class req_msg;
class rpc_client;

/**
 * Pipelined forwarding of client requests to the current leader.
 * Traffic is pinned to a single persistent `rpc_client`
 * (one TCP connection) and a single leader term number.
 *
 * Entries sent through one stream get committed in the original
 * order (possibly interleaved with entries from other sources),
 * with no skipped entries. I.e. some prefix of the sequence of
 * entries ends up committed. E.g. if you send 3 entries ABC, the
 * log may end up with nothing, A, AB, or ABC (most likely).
 *
 * Obtain via `raft_server::open_client_req_stream`.
 *
 * `append` is not thread-safe. Callers must serialize concurrent
 * calls externally. On a non-pipelining transport
 * (`rpc_client::supports_pipelining() == false`) the caller must
 * additionally wait for is_ready() to become true, indicating that
 * the previous RPC has returned.
 *
 * On any error or leader term change, the stream becomes
 * "abandoned", and the caller has to create a new stream.
 *
 * RPC results are not reported. Instead, the caller is expected
 * to separately monitor committed entries and match them with
 * previously submitted append requests. And to frequently check
 * is_abandoned(); if the stream is abandoned, the remaining
 * in-flight requests may or may not be eventually committed, and
 * future requests will be dropped.
 *
 * Lifetime: the stream must not outlive its `raft_server`.
 *
 * Notes:
 *  - In contrast to client_req_stream, auto-forwarding
 *    (raft_params::auto_forwarding_) dispatches through an
 *    `rpc_client` pool, so concurrent requests may reach the leader
 *    out of order.
 *  - client_req_stream sends messages through network even if the
 *    leader is on the local node, for simplicity.
 */
class client_req_stream {
public:
    // Prefer `raft_server::open_client_req_stream`.
    client_req_stream(raft_server& srv,
                      uint64_t stream_term,
                      ptr<rpc_client> rpc,
                      uint64_t send_timeout_ms);

    ~client_req_stream() = default;

    // Precondition: is_ready() == true.
    void append(std::vector< ptr<buffer> > logs);

    /**
     * True when the stream is unusable. Two signals:
     *  - latched flag set by a completion handler on any non-OK
     *    outcome (transport error, TERM_MISMATCH, etc.);
     *  - proactive term check — this node's current term differs
     *    from `stream_term_`, a cheap atomic read.
     *
     * Once true, subsequent `append` calls return a pre-resolved
     * FAILED cmd_result; caller should close this stream and open a
     * new one.
     */
    bool is_abandoned() const;

    /**
     * If transport doesn't supports pipelining
     * (asio_service_options::streaming_mode_ == true),
     * this is always true.
     * Otherwise this returns false if there's a request in progress.
     * `append` calls are not allowed when is_ready() == false.
     */
    bool is_ready() const;

    /**
     * True if all previous requests have been sent to the leader.
     * They may or may not have been committed.
     * Can be used for best-effort fire-and-forget requests:
     * create a stream, call `append`, wait for
     * `is_idle() || is_abandoned()`, destroy the stream.
     */
    bool is_idle() const;

private:
    struct State {
        alignas(64) std::atomic<bool> abandoned_ {};
        alignas(64) std::atomic<size_t> in_flight_ {};
    };

    raft_server* srv_;
    uint64_t stream_term_;
    ptr<rpc_client> rpc_;
    uint64_t send_timeout_ms_;
    bool supports_pipelining_;
    // Shared with in-flight completion handlers so they can access
    // it safely even if the stream has been destroyed.
    ptr<State> state_;
};

} // namespace nuraft

#endif // _CLIENT_REQ_STREAM_HXX_
