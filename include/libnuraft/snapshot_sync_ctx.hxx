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

#ifndef _SNAPSHOT_SYNC_CTX_HXX_
#define _SNAPSHOT_SYNC_CTX_HXX_

#include "basic_types.hxx"
#include "event_awaiter.hxx"
#include "internal_timer.hxx"
#include "pp_util.hxx"
#include "ptr.hxx"

#include <atomic>
#include <functional>
#include <list>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "thread.hxx"

class EventAwaiter;

namespace nuraft {

class peer;
class raft_server;
class resp_msg;
class rpc_exception;
class snapshot;
class state_machine;
class snapshot_sync_ctx {
public:
    snapshot_sync_ctx(const ptr<snapshot>& s,
                      int peer_id,
                      ulong timeout_ms,
                      ulong offset = 0L);

    __nocopy__(snapshot_sync_ctx);

public:
    const ptr<snapshot>& get_snapshot() const { return snapshot_; }
    ulong get_offset() const { return offset_; }
    ulong get_obj_idx() const { return obj_idx_; }
    // Logical snapshot context lifecycle must use `user_snp_ctx_io_guard`
    // and `close_user_snp_ctx`; this raw accessor is not lifetime-safe.
    void*& get_user_snp_ctx() { return user_snp_ctx_; }

    class user_snp_ctx_io_guard
    {
    public:
        user_snp_ctx_io_guard(snapshot_sync_ctx& ctx, state_machine& sm);
        ~user_snp_ctx_io_guard() noexcept;

        user_snp_ctx_io_guard(const user_snp_ctx_io_guard&) = delete;
        user_snp_ctx_io_guard& operator=(const user_snp_ctx_io_guard&) = delete;
        user_snp_ctx_io_guard(user_snp_ctx_io_guard&&) = delete;
        user_snp_ctx_io_guard& operator=(user_snp_ctx_io_guard&&) = delete;

        explicit operator bool() const
        {
            return active_;
        }
        void*& get();
        bool finish();

    private:
        snapshot_sync_ctx* ctx_;
        state_machine* sm_;
        bool active_;
    };

    void close_user_snp_ctx(state_machine& sm);

    void set_offset(ulong offset);
    void set_obj_idx(ulong obj_idx) { obj_idx_ = obj_idx; }
    // Logical snapshot context lifecycle must use `user_snp_ctx_io_guard`
    // and `close_user_snp_ctx`; this raw setter is not lifetime-safe.
    void set_user_snp_ctx(void* _user_snp_ctx) { user_snp_ctx_ = _user_snp_ctx; }

    timer_helper& get_timer() { return timer_; }

    bool begin_async_snapshot_request();
    void finish_async_snapshot_request();
    void finish_async_snapshot_transfer();
    bool is_async_snapshot_request_in_progress() const;
    bool is_async_snapshot_transfer_started() const;

private:
    void io_thread_loop();
    bool begin_user_snp_ctx_io();
    bool finish_user_snp_ctx_io(state_machine& sm);

    /**
     * Destination peer ID.
     */
    int32_t peer_id_;

    /**
     * Pointer to snapshot.
     */
    ptr<snapshot> snapshot_;

    /**
     * Current cursor of snapshot.
     * Can be used for either byte offset or object index,
     * but the legacy raw snapshot (offset_) is deprecated.
     */
    union {
        ulong offset_;
        ulong obj_idx_;
    };

    /**
     * User-defined snapshot context, given by the state machine.
     */
    void* user_snp_ctx_;

    /**
     * Leaf mutex protecting the logical user snapshot context lifecycle.
     */
    std::mutex user_snp_ctx_lock_;

    bool user_snp_ctx_io_active_ = false;
    bool user_snp_ctx_closed_ = false;

    std::atomic<bool> async_snapshot_transfer_started_{false};
    std::atomic<bool> async_snapshot_request_in_progress_{false};

    /**
     * Timer to check snapshot transfer timeout.
     */
    timer_helper timer_;
};

// Singleton class.
class snapshot_io_mgr {
public:
    static snapshot_io_mgr& instance();

    /**
     * Shutdown the global snapshot IO manager if it was initialized.
     */
    static void shutdown_instance();

    /**
     * Push a snapshot read request to the queue.
     *
     * @param r Raft server instance.
     * @param p Peer instance.
     * @param h Response handler.
     * @return `true` if a request was queued. `false` if another request is
     *         already pending for this peer, or if a safe current snapshot
     *         sync context and snapshot could not be captured.
     */
    bool push(ptr<raft_server> r,
              ptr<peer> p,
              std::function< void(ptr<resp_msg>&, ptr<rpc_exception>&) >& h);

    /**
     * Invoke IO thread.
     */
    void invoke();

    /**
     * Drop all pending requests belonging to the given Raft instance.
     *
     * @param r Raft server instance.
     */
    void drop_reqs(raft_server* r);

    /**
     * Check if there is pending request for the given peer.
     *
     * @param r Raft server instance.
     * @param srv_id Server ID to check.
     * @return `true` if pending request exists.
     */
    bool has_pending_request(raft_server* r, int srv_id);

    /**
     * Shutdown the global snapshot IO manager.
     */
    void shutdown();

private:
    friend class snapshot_io_mgr_singleton;

    struct io_queue_elem;

    snapshot_io_mgr();

    ~snapshot_io_mgr();

    void async_io_loop();

    bool push(ptr<io_queue_elem>& elem);
    void clear_dropped_request(ptr<io_queue_elem>& elem);

    /**
     * A dedicated thread for reading snapshot object.
     */
    nuraft_thread io_thread_;

    /**
     * Event awaiter for `io_thread_`.
     */
    ptr<EventAwaiter> io_thread_ea_;

    /**
     * `true` if we are closing this context.
     */
    std::atomic<bool> terminating_;

    /**
     * Request queue. Allow only one request per peer at a time.
     */
    std::list< ptr<io_queue_elem> > queue_;

    /**
     * Lock for `queue_`.
     */
    std::mutex queue_lock_;
};

}

#endif //_SNAPSHOT_SYNC_CTX_HXX_
