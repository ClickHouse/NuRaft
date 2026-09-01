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

#ifndef _PEER_HXX_
#define _PEER_HXX_

#include "context.hxx"
#include "delayed_task_scheduler.hxx"
#include "internal_timer.hxx"
#include "timer_task.hxx"
#include "rpc_cli_factory.hxx"
#include "snapshot_sync_ctx.hxx"
#include "srv_config.hxx"

#include <atomic>
#include <cassert>

namespace nuraft {

class snapshot;

/**
 * Whether a peer's gap to the leader is shrinking, measured over a window.
 *
 * The gap at the start of the window is compared with the gap at the end. If
 * it got smaller, the peer is catching up. Before a full window has passed, or
 * when there is no earlier gap to compare with, the answer is `true`: a peer
 * counts as catching up until a whole window shows that it is not. So by
 * default the leader does nothing.
 */
struct gap_trend {
    bool is_shrinking(uint64_t gap, int32_t window_ms) {
        if (window_timer_.get_ms() < (uint64_t)window_ms) {
            return true;
        }
        bool shrinking = !has_gap_at_window_start_ ||
                         gap < gap_at_window_start_;
        restart_window(gap);
        return shrinking;
    }

    void restart_window(uint64_t gap) {
        gap_at_window_start_ = gap;
        has_gap_at_window_start_ = true;
        window_timer_.reset();
    }

    // Start a window with no earlier gap to compare against, so that the first
    // window after this call always counts as catching up.
    void forget_window() {
        has_gap_at_window_start_ = false;
        window_timer_.reset();
    }

    uint64_t gap_at_window_start_ = 0;
    bool has_gap_at_window_start_ = false;
    timer_helper window_timer_;
};

class peer {
public:
    peer( ptr<srv_config>& config,
          const context& ctx,
          timer_task<int32>::executor& hb_exec,
          ptr<logger>& logger )
        : config_(config)
        , scheduler_(ctx.scheduler_)
        , rpc_( ctx.rpc_cli_factory_->create_client(config->get_endpoint()) )
        , current_hb_interval_( ctx.get_params()->heart_beat_interval_ )
        , hb_interval_( ctx.get_params()->heart_beat_interval_ )
        , rpc_backoff_( ctx.get_params()->rpc_failure_backoff_ )
        , max_hb_interval_( ctx.get_params()->max_hb_interval() )
        , next_log_idx_(0)
        , last_accepted_log_idx_(0)
        , sm_committed_idx_(0)
        , next_batch_size_hint_in_bytes_(0)
        , matched_idx_(0)
        , next_log_idx_floor_(0)
        , busy_flag_(false)
        , pending_commit_flag_(false)
        , hb_enabled_(false)
        , hb_task_( cs_new< timer_task<int32>,
                            timer_task<int32>::executor&,
                            int32 >
                          ( hb_exec, config->get_id(),
                            timer_task_type::heartbeat_timer ) )
        , snp_sync_ctx_(nullptr)
        , lock_()
        , backpressure_active_(false)
        , backpressure_given_up_(false)
        , long_pause_warnings_(0)
        , network_recoveries_(0)
        , manual_free_(false)
        , rpc_errs_(0)
        , stale_rpc_responses_(0)
        , last_sent_idx_(0)
        , cnt_not_applied_(0)
        , cnt_backward_log_probe_(0)
        , leave_requested_(false)
        , hb_cnt_since_leave_(0)
        , stepping_down_(false)
        , reconn_scheduled_(false)
        , reconn_backoff_(0)
        , suppress_following_error_(false)
        , abandoned_(false)
        , lost_by_leader_(false)
        , rsv_msg_(nullptr)
        , rsv_msg_handler_(nullptr)
        , last_streamed_log_idx_(0)
        , bytes_in_flight_(0)
        , snapshot_sync_is_needed_(false)
        , self_mark_down_(false)
        , l_(logger)
    {
        reset_ls_timer();
        reset_resp_timer();
        reset_active_timer();
    }

    __nocopy__(peer);

public:
    int32 get_id() const {
        std::lock_guard<std::mutex> lock(config_mutex_);
        return config_->get_id();
    }

    const std::string& get_endpoint() const {
        std::lock_guard<std::mutex> lock(config_mutex_);
        return config_->get_endpoint();
    }

    bool is_learner() const {
        std::lock_guard<std::mutex> lock(config_mutex_);
        return config_->is_learner();
    }

    bool is_new_joiner() const {
        return config_->is_new_joiner();
    }

    const srv_config& get_config() {
        std::lock_guard<std::mutex> lock(config_mutex_);
        return *config_;
    }

    void set_config(ptr<srv_config> new_config) {
        std::lock_guard<std::mutex> lock(config_mutex_);
        config_ = new_config;
    }

    ptr<delayed_task>& get_hb_task() {
        return hb_task_;
    }

    std::mutex& get_lock() {
        return lock_;
    }

    int32 get_current_hb_interval() const {
        return current_hb_interval_;
    }

    bool make_busy() {
        bool f = false;
        return busy_flag_.compare_exchange_strong(f, true);
    }

    bool is_busy() {
        return busy_flag_;
    }

    void set_free() {
        busy_flag_.store(false);
    }

    bool is_hb_enabled() const {
        return hb_enabled_;
    }

    void enable_hb(bool enable) {
        if (abandoned_) return;

        hb_enabled_ = enable;
        if (!enable) {
            scheduler_->cancel(hb_task_);
        }
    }

    ulong get_next_log_idx() const {
        return next_log_idx_;
    }

    void set_next_log_idx(ulong idx) {
        next_log_idx_ = idx;
    }

    ulong get_next_log_idx_floor() const {
        return next_log_idx_floor_;
    }

    void set_next_log_idx_floor(ulong idx) {
        next_log_idx_floor_ = idx;
    }

    uint64_t get_last_accepted_log_idx() const {
        return last_accepted_log_idx_;
    }

    void set_last_accepted_log_idx(uint64_t to) {
        last_accepted_log_idx_ = to;
    }

    uint64_t get_sm_committed_idx() const {
        return sm_committed_idx_;
    }

    void set_sm_committed_idx(uint64_t to) {
        sm_committed_idx_ = to;
    }

    int64 get_next_batch_size_hint_in_bytes() const {
        return next_batch_size_hint_in_bytes_;
    }

    void set_next_batch_size_hint_in_bytes(int64 batch_size) {
        next_batch_size_hint_in_bytes_ = batch_size;
    }

    ulong get_matched_idx() const {
        return matched_idx_;
    }

    void set_matched_idx(ulong idx) {
        matched_idx_ = idx;
    }

    void set_pending_commit() {
        pending_commit_flag_.store(true);
    }

    bool clear_pending_commit() {
        bool t = true;
        return pending_commit_flag_.compare_exchange_strong(t, false);
    }

    void set_snapshot_in_sync(const ptr<snapshot>& s,
                              ulong timeout_ms = 10 * 1000) {
        std::lock_guard<std::mutex> l(snp_sync_ctx_lock_);
        if (s == nilptr) {
            snp_sync_ctx_.reset();
        }
        else {
            snp_sync_ctx_ = cs_new<snapshot_sync_ctx>(s, get_id(), timeout_ms);
        }
    }

    ptr<snapshot_sync_ctx> get_snapshot_sync_ctx() const {
        std::lock_guard<std::mutex> l(snp_sync_ctx_lock_);
        return snp_sync_ctx_;
    }

    void slow_down_hb() {
        current_hb_interval_ =
            std::min(max_hb_interval_, current_hb_interval_ + rpc_backoff_);
    }

    void resume_hb_speed() {
        current_hb_interval_ = hb_interval_;
    }

    void set_hb_interval(int32 new_interval) {
        hb_interval_ = new_interval;
    }

    void send_req(ptr<peer> myself,
                  ptr<req_msg>& req,
                  rpc_handler& handler,
                  bool streaming = false);

    void shutdown();

    void reopen(context& ctx, timer_task<int32>::executor& hb_exec);

    bool is_abandoned() const { return abandoned_; }

    // Time that sent the last request.
    void reset_ls_timer()       { last_sent_timer_.reset(); }
    uint64_t get_ls_timer_us()  { return last_sent_timer_.get_us(); }

    // Time that got last successful response.
    void reset_resp_timer()         { last_resp_timer_.reset(); }
    uint64_t get_resp_timer_us()    { return last_resp_timer_.get_us(); }

    // Time of the last network activity from peer (including failure).
    void reset_active_timer()       { last_active_timer_.reset(); }
    uint64_t get_active_timer_us()  { return last_active_timer_.get_us(); }

    // Backpressure for a peer means that the leader does not let the commit
    // index move past this peer's matched index, so that this peer can catch
    // up. Only the leader does this, so the state below means nothing on a
    // follower, and `become_leader` forgets it. See
    // `raft_params::slow_member_backpressure_max_duration_`.
    //
    // Start backpressure for this peer. The caller decides when it starts and
    // when it ends, using two different gaps, so this is only a flag. Returns
    // `true` if this call changed the flag, which lets the caller log the
    // change once.
    bool start_backpressure() {
        if (backpressure_active_.exchange(true)) {
            return false;
        }
        backpressure_timer_.reset();
        return true;
    }
    // Waiting no longer helps this peer, for example because it stopped
    // answering. The give-up flag stays set on purpose. The peer has not
    // caught up, so backpressure must not start again just because the peer
    // left this state and entered it again.
    bool set_cannot_catch_up() {
        return backpressure_active_.exchange(false);
    }

    // The peer caught up. This clears the give-up flag, so backpressure can
    // start for this peer again later.
    bool set_caught_up() {
        backpressure_given_up_ = false;
        return backpressure_active_.exchange(false);
    }
    // Forget everything about this peer, and do not report a change. Used when
    // the old observations no longer mean anything: this server has just become
    // the leader and did not watch the peers while it was a follower, the
    // feature was switched on or off, or the commit quorum size is overridden.
    void forget_backpressure_state() {
        backpressure_active_ = false;
        backpressure_given_up_ = false;
        backpressure_timer_.reset();
        // Backpressure therefore needs two full windows after this call.
        gap_trend_.forget_window();
    }
    bool is_backpressure_active() const { return backpressure_active_; }
    uint64_t get_backpressure_ms() { return backpressure_timer_.get_ms(); }

    bool gap_is_shrinking(uint64_t gap, int32_t window_ms) {
        return gap_trend_.is_shrinking(gap, window_ms);
    }
    void restart_gap_window(uint64_t gap) { gap_trend_.restart_window(gap); }

    // `true` if the leader stopped holding the commit index for this peer
    // because the peer did not catch up in time. Cleared when the peer catches
    // up (`set_caught_up`), and when everything is forgotten
    // (`forget_backpressure_state`). `set_cannot_catch_up` keeps it, on
    // purpose.
    bool is_backpressure_given_up() const   { return backpressure_given_up_; }
    void set_backpressure_given_up()        { backpressure_given_up_ = true; }

    void reset_long_pause_warnings()    { long_pause_warnings_ = 0; }
    void inc_long_pause_warnings()      { long_pause_warnings_.fetch_add(1); }
    int32 get_long_puase_warnings()     { return long_pause_warnings_; }

    void reset_recovery_cnt()       { network_recoveries_ = 0; }
    void inc_recovery_cnt()         { network_recoveries_.fetch_add(1); }
    int32 get_recovery_cnt() const  { return network_recoveries_; }

    void reset_manual_free()    { manual_free_ = false; }
    void set_manual_free()      { manual_free_ = true; }
    bool is_manual_free()       { return manual_free_; }

    bool recreate_rpc(ptr<srv_config>& config,
                      context& ctx);

    void reset_rpc() {
        std::lock_guard<std::mutex> l(rpc_protector_);
        rpc_.reset();
    }

    void reset_rpc_errs()   { rpc_errs_ = 0; }
    void inc_rpc_errs()     { rpc_errs_.fetch_add(1); }
    int32 get_rpc_errs()    { return rpc_errs_; }

    void reset_stale_rpc_responses()    { stale_rpc_responses_ = 0; }
    int32_t inc_stale_rpc_responses()   { return stale_rpc_responses_.fetch_add(1); }
    int32_t get_stale_rpc_responses()   { return stale_rpc_responses_; }

    void set_last_sent_idx(ulong to)    { last_sent_idx_ = to; }
    ulong get_last_sent_idx() const     { return last_sent_idx_.load(); }

    void reset_cnt_not_applied()        { cnt_not_applied_ = 0; }
    int32 inc_cnt_not_applied()         { cnt_not_applied_++;
                                          return cnt_not_applied_; }
    int32 get_cnt_not_applied() const   { return cnt_not_applied_; }

    void reset_cnt_backward_log_probe()       { cnt_backward_log_probe_ = 0; }
    int32 inc_cnt_backward_log_probe()        { return cnt_backward_log_probe_.fetch_add(1) + 1; }
    int32 get_cnt_backward_log_probe() const  { return cnt_backward_log_probe_; }

    void step_down()                { stepping_down_ = true; }
    bool is_stepping_down() const   { return stepping_down_.load(); }

    void set_leave_flag()           { leave_requested_= true; }
    bool is_leave_flag_set() const  { return leave_requested_.load(); }

    void inc_hb_cnt_since_leave()           { hb_cnt_since_leave_.fetch_add(1); }
    int32 get_hb_cnt_since_leave() const    { return hb_cnt_since_leave_; }

    void schedule_reconnection() {
        reconn_timer_.set_duration_sec(3);
        reconn_timer_.reset();
        reconn_scheduled_ = true;
    }
    void clear_reconnection()   { reconn_scheduled_ = false; }
    bool need_to_reconnect() {
        if (abandoned_) return false;

        if (reconn_scheduled_ && reconn_timer_.timeout()) {
            return true;
        }
        {   std::lock_guard<std::mutex> l(rpc_protector_);
            if (!rpc_.get()) {
                return true;
            }
        }
        return false;
    }

    void set_suppress_following_error() { suppress_following_error_ = true; }
    bool need_to_suppress_error() {
        bool exp = true, desired = false;
        return suppress_following_error_.compare_exchange_strong(exp, desired);
    }

    void set_rsv_msg(const ptr<req_msg>& m, const rpc_handler& h) {
        rsv_msg_ = m;
        rsv_msg_handler_ = h;
    }

    ptr<req_msg> get_rsv_msg() const { return rsv_msg_; }
    rpc_handler get_rsv_msg_handler() const { return rsv_msg_handler_; }

    ulong get_last_streamed_log_idx() {
        return last_streamed_log_idx_.load();
    }

    void set_last_streamed_log_idx(ulong expected, ulong idx) {
        last_streamed_log_idx_.compare_exchange_strong(expected, idx);
    }

    void reset_stream() {
        last_streamed_log_idx_.store(0);
    }

    int64_t get_bytes_in_flight() {
        return bytes_in_flight_.load();
    }

    void bytes_in_flight_add(size_t req_size_bytes) {
        bytes_in_flight_.fetch_add(req_size_bytes);
    }

    void bytes_in_flight_sub(size_t req_size_bytes) {
        bytes_in_flight_.fetch_sub(req_size_bytes);
        assert(bytes_in_flight_ >= 0);
    }

    void reset_bytes_in_flight() {
        bytes_in_flight_.store(0);
    }

    void try_set_free(msg_type type, bool streaming);

    bool is_lost() const { return lost_by_leader_; }
    void set_lost() { lost_by_leader_ = true; }
    void set_recovered() { lost_by_leader_ = false; }

    void set_snapshot_sync_is_needed(bool to) {
        snapshot_sync_is_needed_ = to;
    }
    bool is_snapshot_sync_needed() const {
        return snapshot_sync_is_needed_;
    }

    bool is_self_mark_down() const {
        return self_mark_down_;
    }
    bool set_self_mark_down(bool to) {
        bool old = self_mark_down_;
        if (old != to) {
            self_mark_down_ = to;
        }
        return old;
    }

private:
    void handle_rpc_result(ptr<peer> myself,
                           uint64_t my_rpc_client_id,
                           ptr<req_msg>& req,
                           ptr<rpc_result>& pending_result,
                           bool streaming,
                           size_t req_size_bytes,
                           ptr<resp_msg>& resp,
                           ptr<rpc_exception>& err);

    /**
     * Information (config) of this server.
     */
    ptr<srv_config> config_;

    mutable std::mutex config_mutex_;

    /**
     * Heartbeat scheduler for this server.
     */
    ptr<delayed_task_scheduler> scheduler_;

    /**
     * RPC client to this server.
     */
    ptr<rpc_client> rpc_;

    /**
     * Guard of `rpc_`.
     */
    std::mutex rpc_protector_;

    /**
     * Current heartbeat interval after adding back-off.
     */
    std::atomic<int32> current_hb_interval_;

    /**
     * Original heartbeat interval.
     */
    int32 hb_interval_;

    /**
     * RPC backoff.
     */
    int32 rpc_backoff_;

    /**
     * Upper limit of heartbeat interval.
     */
    int32 max_hb_interval_;

    /**
     * Next log index of this server.
     */
    std::atomic<ulong> next_log_idx_;

    /**
     * The last log index accepted by this server.
     */
    std::atomic<uint64_t> last_accepted_log_idx_;

    /**
     * The committed log index of the state machine of this peer.
     */
    std::atomic<uint64_t> sm_committed_idx_;

    /**
     * Hint of the next log batch size in bytes.
     */
    std::atomic<int64> next_batch_size_hint_in_bytes_;

    /**
     * The last log index whose term matches up with the leader.
     */
    ulong matched_idx_;

    /**
     * Floor for `next_log_idx_`, set after successful snapshot sync.
     * Prevents stale RPC responses from rewinding `next_log_idx_`
     * below the snapshot-established position.
     */
    ulong next_log_idx_floor_;

    /**
     * `true` if we sent message to this server and waiting for
     * the response.
     */
    std::atomic<bool> busy_flag_;

    /**
     * `true` if we need to send follow-up request immediately
     * for commiting logs.
     */
    std::atomic<bool> pending_commit_flag_;

    /**
     * `true` if heartbeat is enabled.
     */
    bool hb_enabled_;

    /**
     * Heartbeat task.
     */
    ptr<delayed_task> hb_task_;

    /**
     * Snapshot context if snapshot transmission is in progress.
     */
    ptr<snapshot_sync_ctx> snp_sync_ctx_;

    /**
     * Lock for `snp_sync_ctx_`.
     */
    mutable std::mutex snp_sync_ctx_lock_;

    /**
     * Lock for this peer.
     */
    std::mutex lock_;

    // --- For tracking long pause ---
    /**
     * Timestamp when the last request was sent.
     */
    timer_helper last_sent_timer_;

    /**
     * Timestamp when the last (successful) response was received.
     */
    timer_helper last_resp_timer_;

    /**
     * Timestamp when the last active network activity was detected.
     */
    timer_helper last_active_timer_;

    /**
     * `true` while backpressure is active for this peer: the leader started to
     * hold the commit index back and has not released the peer yet. It stays
     * `true` after a give-up, when nothing is held any more, so that
     * backpressure and its timer cannot start again before the peer has caught
     * up.
     */
    std::atomic<bool> backpressure_active_;

    /**
     * When backpressure for this peer started, which is how long the leader
     * has been waiting for it.
     */
    timer_helper backpressure_timer_;

    /**
     * `true` if backpressure for this peer ended because the peer did not
     * catch up in time, see `is_backpressure_given_up`.
     */
    std::atomic<bool> backpressure_given_up_;

    /**
     * Whether this peer is closing the distance to the leader.
     */
    gap_trend gap_trend_;

    /**
     * Counter of long pause warnings.
     */
    std::atomic<int32> long_pause_warnings_;

    /**
     * Counter of recoveries after long pause.
     */
    std::atomic<int32> network_recoveries_;

    /**
     * `true` if user manually clear the `busy_flag_` before
     * getting response from this server.
     */
    std::atomic<bool> manual_free_;

    /**
     * For tracking RPC error.
     */
    std::atomic<int32> rpc_errs_;

    /**
     * For tracking stale RPC responses from the old client.
     */
    std::atomic<int32> stale_rpc_responses_;

    /**
     * Start log index of the last sent append entries request.
     */
    std::atomic<ulong> last_sent_idx_;

    /**
     * Number of count where start log index is the same as previous.
     */
    std::atomic<int32> cnt_not_applied_;

    /**
     * Number of consecutive rejected append responses that moved
     * `next_log_idx_` one step backward while probing for a matching log.
     */
    std::atomic<int32> cnt_backward_log_probe_;

    /**
     * `true` if leave request has been sent to this peer.
     */
    std::atomic<bool> leave_requested_;

    /**
     * Number of HB timeout after leave requested.
     */
    std::atomic<int32> hb_cnt_since_leave_;

    /**
     * `true` if this peer responded to leave request so that
     * will be removed from cluster soon.
     * To avoid HB timer trying to do something with this peer.
     */
    std::atomic<bool> stepping_down_;

    /**
     * For re-connection.
     */
    std::atomic<bool> reconn_scheduled_;

    /**
     * Back-off timer to avoid superfluous reconnection.
     */
    timer_helper reconn_timer_;

    /**
     * For exp backoff of reconnection.
     */
    timer_helper reconn_backoff_;

    /**
     * If `true`, we will lower the log level of the RPC error
     * from this server.
     */
    std::atomic<bool> suppress_following_error_;

    /**
     * if `true`, this peer is removed and shut down.
     * All operations on this peer should be rejected.
     */
    std::atomic<bool> abandoned_;

    /**
     * If `true`, this peer is considered unresponsive
     * and treated as if it has been lost.
     */
    std::atomic<bool> lost_by_leader_;

    /**
     * Reserved message that should be sent next time.
     */
    ptr<req_msg> rsv_msg_;

    /**
     * Handler for reserved message.
     */
    rpc_handler rsv_msg_handler_;

    /**
     * Last log index sent in stream mode.
     */
    std::atomic<ulong> last_streamed_log_idx_;

    /**
     * Current bytes of in-flight append entry requests.
     */
    std::atomic<int64_t> bytes_in_flight_;

    /**
     * Set to `true` if this peer was in the middle of receiving snapshot,
     * but received a normal request. In such a case, even though
     * `next_log_idx_` is within the range, we should send a snapshot.
     */
    std::atomic<bool> snapshot_sync_is_needed_;

    /**
     * If `true`, this peer marks itself down.
     */
    std::atomic<bool> self_mark_down_;


    /**
     * Logger instance.
     */
    ptr<logger> l_;
};

}

#endif //_PEER_HXX_
