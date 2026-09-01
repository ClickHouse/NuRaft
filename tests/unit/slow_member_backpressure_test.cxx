/************************************************************************
Copyright 2017-2019 eBay Inc.

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

#include "debugging_options.hxx"
#include "fake_network.hxx"
#include "raft_package_fake.hxx"

#include "event_awaiter.hxx"
#include "raft_params.hxx"
#include "test_common.h"

#include <stdio.h>
#include <string>

using namespace nuraft;
using namespace raft_functional_common;

namespace slow_member_backpressure_test {

static const size_t STALE_LOG_GAP = 5;

// Appends `num` messages on the leader, and delivers the resulting messages to
// `endpoints` only, so that any peer left out keeps its matched index while its
// connection stays healthy: alive, but failing to keep up.
static void append_and_deliver_to(RaftPkg& leader,
                                  size_t num,
                                  const std::vector<std::string>& endpoints,
                                  const std::string& prefix,
                                  const std::vector<std::string>& failing = {})
{
    for (size_t ii = 0; ii < num; ++ii) {
        std::string test_msg = prefix + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        leader.raftServer->append_entries( {msg} );

        for (const std::string& endpoint: endpoints) {
            // Request, then response.
            leader.fNet->execReqResp(endpoint);
            leader.fNet->execReqResp(endpoint);
        }
        for (const std::string& endpoint: failing) {
            // Deliver the request as a failure, so that the leader observes
            // the peer as unreachable rather than merely silent.
            leader.fNet->makeReqFailAll(endpoint);
        }
    }
}

// The last log index of the leader's own log, i.e. what a member's gap is
// measured against.
static uint64_t tail(RaftPkg& leader) {
    return leader.getTestMgr()->load_log_store()->next_slot() - 1;
}

// Whether the commit index is being held short of the log.
//
// Always the commit index and never the state machine index: the feature
// clamps the former, and the latter trails it on the apply thread, so
// asserting on it can pass while a member is wrongly held, or fail while it
// is not.
#define CHK_HELD(pkg) \
    CHK_GT( tail(pkg), (pkg).raftServer->get_target_committed_log_idx() )
#define CHK_NOT_HELD(pkg) \
    CHK_EQ( tail(pkg), (pkg).raftServer->get_target_committed_log_idx() )

// The last log index of a peer, from the leader's point of view, i.e. exactly
// what the backpressure compares against.
static uint64_t matched_idx_of(RaftPkg& leader, int32_t peer_id) {
    return leader.raftServer->get_peer_info(peer_id).last_log_idx_;
}

static int prepare(std::vector<RaftPkg*>& pkgs,
                   int32_t max_duration_ms)
{
    CHK_Z( launch_servers(pkgs) );
    CHK_Z( make_group(pkgs) );

    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        // Otherwise `append_entries` blocks until commit, which is exactly
        // what this test holds back on purpose.
        params.return_method_ = raft_params::async_handler;
        // Off by default now: an operator turns it on, so the tests do too.
        params.slow_member_backpressure_enabled_ = true;
        params.slow_member_backpressure_max_duration_ = max_duration_ms;
        // The gap trend is covered separately by
        // `not_held_while_still_catching_up_test`; every other test is about
        // the threshold and the time limit, so the trend is switched off here
        // to keep them independent of wall-clock timing.
        params.slow_member_backpressure_gap_window_ = -1;
        params.stale_log_gap_ = STALE_LOG_GAP;
        params.fresh_log_gap_ = 1;
        // These tests build a gap by appending while the commit index is held,
        // so the append brake has to stay out of the way. Left unset it is
        // derived from `STALE_LOG_GAP`, which is far too small here to let a
        // gap form at all. `refuses_new_requests_while_active_test` sets its
        // own value, because the brake is what it tests.
        params.slow_member_backpressure_max_uncommitted_ = STALE_LOG_GAP * 100;
        pkg->raftServer->update_params(params);
    }
    return 0;
}

// A member that is alive but falling behind must hold the commit index back,
// even though the other members form a majority.
int holds_commit_for_lagging_member_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    // No time limit, so that only catching up releases the member.
    CHK_Z( prepare(pkgs, -1) );

    uint64_t commit_idx = s1.raftServer->get_target_committed_log_idx();

    // S3 gets nothing, so it falls behind by more than the stale log gap,
    // while S1 and S2 alone are a majority.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "held");

    // Entries keep committing until S3 is more than `stale_log_gap_` behind,
    // and then the leader starts waiting for it: the commit index stops well
    // short of the last log index, even though S1 and S2 are a majority.
    uint64_t last_log_idx = tail(s1);
    uint64_t held_at = s1.raftServer->get_target_committed_log_idx();
    CHK_GT( last_log_idx, held_at );
    CHK_SMEQ( held_at, commit_idx + STALE_LOG_GAP );

    // Let S3 catch up. Once it is back within the gap, commits resume.
    for (size_t ii = 0; ii < STALE_LOG_GAP * 4; ++ii) {
        s1.fNet->execReqResp(s3_addr);
    }
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( last_log_idx, s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// With no hold time configured the feature does nothing, even when it has been
// switched on: both the switch and the hold time are required. The switch
// itself, including its default, is covered by `runtime_toggle_test`.
int no_hold_time_configured_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( prepare(pkgs, 0) );

    uint64_t commit_idx = s1.raftServer->get_target_committed_log_idx();

    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "ahead");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // A majority acknowledged, so everything is committed without S3.
    CHK_GT( s1.raftServer->get_target_committed_log_idx(), commit_idx );
    CHK_NOT_HELD( s1 );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// A member that never catches up must be given up on, otherwise one broken
// member would stop the cluster from committing anything at all.
int gives_up_after_max_duration_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    const int32_t MAX_HOLD_MS = 500;
    CHK_Z( prepare(pkgs, MAX_HOLD_MS) );

    uint64_t commit_idx = s1.raftServer->get_target_committed_log_idx();

    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "giveup");

    // Still within the time limit: the commit index is held short of the log.
    CHK_HELD( s1 );

    TestSuite::sleep_ms(MAX_HOLD_MS * 2, "wait for the leader to give up on S3");

    // One more round of replication to S2, which is enough to commit now:
    // the leader gave up on S3 and no longer waits for it.
    append_and_deliver_to(s1, 1, {s2_addr}, "after_giveup");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( tail(s1),
            s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// The exact threshold: a member that is behind by exactly `stale_log_gap_` is
// not yet waited for, and one more entry starts the hold. Pins the comparison
// against an off-by-one.
int hold_threshold_is_exact_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( prepare(pkgs, -1) );

    // Exactly at the threshold: everything still commits.
    append_and_deliver_to(s1, STALE_LOG_GAP, {s2_addr}, "exact");
    // S2 must have every entry for the majority index to be the tail; a busy
    // peer can leave it short, which would look like a hold.
    while (s1.fNet->execReqResp(s2_addr)) {}
    uint64_t last_log_idx = tail(s1);
    CHK_EQ( STALE_LOG_GAP, last_log_idx - matched_idx_of(s1, 3) );
    // The commit index, not the state machine index: whether anything is held
    // is decided on the commit index, and the apply thread trails it, so
    // asserting the applied index here would only measure that thread.
    CHK_EQ( last_log_idx, s1.raftServer->get_target_committed_log_idx() );

    // One entry past the threshold: the leader starts waiting.
    append_and_deliver_to(s1, 1, {s2_addr}, "past");
    // S2 must have every entry for the majority index to be the tail; a busy
    // peer can leave it short, which would look like a hold.
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_EQ( last_log_idx, s1.raftServer->get_target_committed_log_idx() );
    CHK_HELD( s1 );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}
// A learner is not a voting member, so it must never hold the commit index:
// otherwise adding an observer would make the whole cluster slower.
int learner_is_never_held_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    auto init_cb = [&](RaftPkg* pp) {
        if (pp->myId == 3) {
            pp->getTestMgr()->get_srv_config()->set_learner(true);
        }
    };
    CHK_Z( launch_servers( pkgs, nullptr, false, cb_default, init_cb ) );
    CHK_Z( make_group( pkgs ) );
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.return_method_ = raft_params::async_handler;
        params.slow_member_backpressure_enabled_ = true;
        params.slow_member_backpressure_max_duration_ = -1;
        params.slow_member_backpressure_gap_window_ = -1;
        params.slow_member_backpressure_max_uncommitted_ = STALE_LOG_GAP * 100;
        params.stale_log_gap_ = STALE_LOG_GAP;
        params.fresh_log_gap_ = 1;
        pkg->raftServer->update_params(params);
    }

    // The learner falls far behind, and the two voting members commit anyway.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "learner");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( tail(s1),
            s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// While a custom commit quorum size is in force the quorum is overridden on
// purpose, so nothing may be held back.
int not_held_with_custom_quorum_size_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( prepare(pkgs, -1) );

    raft_params params = s1.raftServer->get_current_params();
    params.custom_commit_quorum_size_ = 2;
    s1.raftServer->update_params(params);

    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "custom");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( tail(s1),
            s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// A member that is far behind but closing the distance must never be held:
// that is what a member does after a restart, after installing a snapshot, or
// right after joining, and waiting for it would stall the cluster once per
// member of a rolling restart, for a member that is recovering on its own.
int not_held_while_still_catching_up_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( prepare(pkgs, -1) );

    // Unlike the other tests, the gap trend is what is under test here.
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.slow_member_backpressure_gap_window_ = 100;
        params.max_append_size_ = 1;
        pkg->raftServer->update_params(params);
    }

    // Build a backlog far beyond the hold threshold, as a restarted member
    // would have.
    append_and_deliver_to(s1, STALE_LOG_GAP * 4, {s2_addr}, "backlog");
    uint64_t gap = tail(s1)
                   - matched_idx_of(s1, 3);
    CHK_GT( gap, STALE_LOG_GAP );

    // Now S3 works through it, one entry at a time, slowly enough that more
    // than one trend window elapses. Since the gap keeps reaching a new low it
    // must never be held, so the commit index must stay at the tail of the log
    // throughout: the leader and S2 have everything, and only a hold could
    // pull the commit index below that.
    for (size_t ii = 0; ii < STALE_LOG_GAP * 3; ++ii) {
        s1.fNet->execReqResp(s3_addr);
        s1.fNet->execReqResp(s3_addr);
        if (ii % 5 == 0) {
            TestSuite::sleep_ms(20, "let the trend window elapse");
        }
        uint64_t last_log_idx = tail(s1);
        CHK_EQ( last_log_idx, s1.raftServer->get_target_committed_log_idx() );
    }

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// An rpc error alone must release a held member, even while its matched index
// is still known (non-zero). The failure is observed here before any new
// request is sent, so the connection is not re-created and the matched index
// stays intact: only the rpc error counter can exclude the member. A test that
// let the reconnect happen would make the member ineligible for two reasons at
// once, and either check would hide a bug in the other.
int rpc_error_alone_releases_lagging_member_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( prepare(pkgs, -1) );

    // S3 falls behind with a healthy connection: the hold is active.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "pre");
    uint64_t matched_before = matched_idx_of(s1, 3);
    CHK_GT( matched_before, 0 );
    CHK_HELD( s1 );

    // One more entry. Deliver only the request to S2 and keep its response,
    // so that the commit evaluation can be triggered at a chosen moment.
    std::string test_msg = "probe";
    ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
    msg->put(test_msg);
    s1.raftServer->append_entries( {msg} );
    s1.fNet->delieverReqTo(s2_addr);

    // Now S3's outstanding request fails. The leader has not sent it anything
    // since, so its matched index is still intact: only the rpc error can
    // tell the leader that waiting is pointless.
    s1.fNet->makeReqFailAll(s3_addr);
    CHK_EQ( matched_before, matched_idx_of(s1, 3) );

    // S2's response arrives and the leader re-evaluates the commit index.
    // S3 is unreachable, so it must not be waited for anymore.
    s1.fNet->handleRespFrom(s2_addr);
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( tail(s1),
            s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// A member that has never acknowledged anything (matched index zero) must not
// be waited for, even though its connection is healthy and has never failed.
// A failed request would also raise the rpc error counter and exclude the
// member anyway, hiding a bug in the matched-index check, so this member is
// freshly added instead: healthy connection, no errors, no acknowledgements.
int never_responded_member_is_not_held_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";
    std::string s4_addr = "S4";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    RaftPkg s4(f_base, 4, s4_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( prepare(pkgs, -1) );
    std::vector<RaftPkg*> pkgs4 = {&s4};
    CHK_Z( launch_servers(pkgs4) );

    // Add S4 to the cluster, delivering only what the membership change
    // itself needs: the join request to S4 and the configuration entry to S2
    // and S3. S4 becomes a voting member the leader has never heard from.
    s1.raftServer->add_srv( *(s4.getTestMgr()->get_srv_config()) );
    // Join req/resp, log sync req/resp.
    s1.fNet->execReqResp(s4_addr);
    s1.fNet->execReqResp(s4_addr);
    // Configuration entry to the existing members.
    s1.fNet->execReqResp(s2_addr);
    s1.fNet->execReqResp(s2_addr);
    s1.fNet->execReqResp(s3_addr);
    s1.fNet->execReqResp(s3_addr);
    std::vector<RaftPkg*> pkgs_no_s4 = {&s1, &s2, &s3};
    CHK_Z( wait_for_sm_exec(pkgs_no_s4, COMMIT_TIMEOUT_SEC) );

    // S4 is a member now, and it has never responded to anything.
    CHK_EQ( 0, matched_idx_of(s1, 4) );

    // S1, S2 and S3 are a majority of four, and waiting for S4 cannot help:
    // everything must keep committing.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr, s3_addr}, "fresh");
    CHK_Z( wait_for_sm_exec(pkgs_no_s4, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( tail(s1),
            s1.raftServer->get_committed_log_idx() );
    CHK_EQ( 0, matched_idx_of(s1, 4) );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    s4.raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// The give-up flag must stay set when a member becomes unreachable and then
// reachable again without catching up. Otherwise such a member gets a new hold
// every time it comes back, and the cluster stalls for `max_duration` again and
// again, with no end. The test first shows that the give-up really happened.
// Then the connection to the member fails and comes back. If a new hold were
// granted, the test would never finish, so the mistake cannot go unnoticed.
int give_up_flag_survives_reconnect_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    const int32_t MAX_HOLD_MS = 200;
    CHK_Z( prepare(pkgs, MAX_HOLD_MS) );

    // No snapshots: a member that falls far behind would otherwise be sent a
    // snapshot, and a member receiving a snapshot is ineligible for holding,
    // which would mask what this test is about. One entry per batch, so that
    // becoming eligible again does not also mean catching up.
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.snapshot_distance_ = 0;
        params.max_append_size_ = 1;
        pkg->raftServer->update_params(params);
    }

    // S3 falls behind, the hold begins and expires: the leader gives up,
    // which is proven by the commit index reaching the end of the log.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "flap_a");
    CHK_HELD( s1 );
    TestSuite::sleep_ms(MAX_HOLD_MS * 2, "wait for the leader to give up on S3");
    append_and_deliver_to(s1, 1, {s2_addr}, "flap_b");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( tail(s1),
            s1.raftServer->get_committed_log_idx() );

    // S3's connection fails once, so waiting for it no longer helps. Then it
    // works again, while S3 is still far behind.
    s1.fNet->makeReqFailAll(s3_addr);
    append_and_deliver_to(s1, 1, {s2_addr}, "lost_c");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    // One successful exchange. The leader can wait for S3 again, and S3 is
    // still far behind, because a batch is one entry.
    s1.fNet->execReqResp(s3_addr);
    s1.fNet->execReqResp(s3_addr);
    CHK_GT( tail(s1)
                - matched_idx_of(s1, 3),
            STALE_LOG_GAP );

    // From now on, an (incorrect) fresh hold would be practically unbounded,
    // so the commit index would stall for good, not just for `max_duration`.
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.slow_member_backpressure_max_duration_ = 3600 * 1000;
        pkg->raftServer->update_params(params);
    }

    // The member was given up on and has not caught up since, so nothing may
    // be held: everything must keep committing.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "flap_d");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( tail(s1),
            s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// A member that is receiving a snapshot is waited for. The transfer itself is
// not made faster by waiting, but while it runs the commit index stops moving
// and the log stops growing, so the member does not also have to catch up on
// everything written during the transfer - which is what would otherwise leave
// it far enough behind to need another snapshot.
//
// What must not happen is the commit index being dragged back to the matched
// index of a member that has not applied the snapshot yet: that index is stale
// by construction, and entries above it are already committed and applied
// cluster-wide. The commit index may only freeze where it is.
int snapshot_receiver_is_held_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( prepare(pkgs, -1) );

    // One entry per batch, so that a single exchange with S3 cannot catch it
    // up and the exchange after it has to be a snapshot: `RaftPkg` takes a
    // snapshot every 5 commits, and S3 will be behind the latest one.
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.max_append_size_ = 1;
        pkg->raftServer->update_params(params);
    }

    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "snap");
    CHK_HELD( s1 );
    // Snapshots are created on the apply thread, so wait for it rather than
    // assuming it has caught up already.
    for (int ii = 0; ii < 200 && s1.getTestSm()->getNumSnapshotCreations() == 0;
         ++ii) {
        TestSuite::sleep_ms(10, "wait for a snapshot");
    }
    CHK_GT( s1.getTestSm()->getNumSnapshotCreations(), 0 );

    uint64_t frozen_at = s1.raftServer->get_target_committed_log_idx();
    uint64_t matched_before = matched_idx_of(s1, 3);
    // The matched index of a snapshot receiver is stale, and below the commit
    // index: that is exactly the case the floor has to survive.
    CHK_SM( matched_before, frozen_at );

    // Two exchanges: S3 acknowledges one entry, then the leader finds that its
    // next log index is at or below the latest snapshot and starts sending it.
    s1.fNet->execReqResp(s3_addr);
    s1.fNet->execReqResp(s3_addr);

    // Keep writing and committing through S2 while the transfer is in
    // progress. The commit index must stay exactly where it was: not lower,
    // which would un-commit applied entries, and not higher, which would mean
    // the leader is still outrunning the member it is waiting for.
    for (int ii = 0; ii < 20; ++ii) {
        append_and_deliver_to(s1, 1, {s2_addr}, "during" + std::to_string(ii));
        while (s1.fNet->execReqResp(s2_addr)) {}
        CHK_EQ( frozen_at, s1.raftServer->get_target_committed_log_idx() );
    }
    CHK_Z( s1.getTestSm()->getNumCommitIndexRegressions() );
    CHK_HELD( s1 );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// While the commit index is held, the index handed to
// `state_machine::adjust_commit_index` must never fall below the current
// commit index: the lagging member's matched index is usually below what has
// already been committed, and handing that value to the state machine would
// break the API promise that the expected index never regresses.
int commit_index_never_regresses_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( prepare(pkgs, -1) );

    // A few entries commit normally first, so that the current commit index
    // is well above S3's matched index once S3 stops receiving anything.
    append_and_deliver_to(s1, 2, {s2_addr, s3_addr}, "base");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // S3 falls behind and the hold begins: many evaluations happen while
    // S3's matched index is below the committed index.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "regress");
    CHK_HELD( s1 );

    CHK_Z( s1.getTestSm()->getNumCommitIndexRegressions() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// ---------------------------------------------------------------------------

// The time limit measures how long the member has been held, not how long it
// has been idle: a member that keeps inching forward but is still not back
// within the release gap when the limit expires must be given up on too,
// otherwise slow-but-nonzero progress would stall the cluster indefinitely.
int gives_up_on_a_slowly_progressing_member_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    const int32_t MAX_HOLD_MS = 300;
    CHK_Z( prepare(pkgs, MAX_HOLD_MS) );

    // One entry per batch, so that S3 can be advanced by a single entry at a
    // time and stays well above the release gap throughout.
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.max_append_size_ = 1;
        pkg->raftServer->update_params(params);
    }

    // Fall far behind, so that a handful of single entries cannot close it.
    append_and_deliver_to(s1, STALE_LOG_GAP * 4, {s2_addr}, "slow");
    CHK_HELD( s1 );

    // Trickle: progress on every step, but never enough.
    for (int ii = 0; ii < 4; ++ii) {
        TestSuite::sleep_ms(MAX_HOLD_MS / 2, "trickle");
        s1.fNet->execReqResp(s3_addr);
        s1.fNet->execReqResp(s3_addr);
    }
    uint64_t gap = tail(s1)
                   - matched_idx_of(s1, 3);
    CHK_GT( gap, STALE_LOG_GAP );

    // The limit expired while it was still behind, so it must have been given
    // up on and everything a majority has must be committed.
    append_and_deliver_to(s1, 1, {s2_addr}, "after_slow");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( tail(s1),
            s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// The whole cycle: a member is held, given up on, catches up completely, and
// can then be held again when it falls behind a second time. If catching up did
// not clear the give-up flag, the member could never be held again for the rest
// of the leader's term.
int held_again_after_catching_up_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    const int32_t MAX_HOLD_MS = 200;
    CHK_Z( prepare(pkgs, MAX_HOLD_MS) );

    // Round one: fall behind and be given up on.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "round1");
    TestSuite::sleep_ms(MAX_HOLD_MS * 2, "wait for the leader to give up");
    append_and_deliver_to(s1, 1, {s2_addr}, "round1_end");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( tail(s1),
            s1.raftServer->get_committed_log_idx() );

    // Let S3 catch up completely, which is what clears the give-up.
    for (size_t ii = 0; ii < STALE_LOG_GAP * 6; ++ii) {
        s1.fNet->execReqResp(s3_addr);
    }
    CHK_EQ( tail(s1),
            matched_idx_of(s1, 3) );

    // Round two: fall behind again. It must be held again, i.e. the commit
    // index must stop short of the log.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "round2");
    CHK_HELD( s1 );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}



// The gap trend decides whether a member that is behind is also failing to
// close the distance, which is what separates a member recovering from a
// restart - never to be waited for, however far behind - from one that cannot
// keep up. Tested directly: through a cluster it would depend on the exact
// interleaving of message delivery, which says nothing about the rule itself.
int gap_trend_test() {
    // Wide enough that two scheduler stalls in a row cannot open a window
    // between the first two calls.
    const int32_t WINDOW_MS = 200;
    gap_trend trend;

    // Nothing to conclude before a window has passed.
    CHK_TRUE( trend.is_shrinking(1000, WINDOW_MS) );
    CHK_TRUE( trend.is_shrinking(1000, WINDOW_MS) );

    // A gap that shrank over the window: the member is catching up.
    trend.restart_window(1000);
    TestSuite::sleep_ms(WINDOW_MS * 2, "let the window elapse");
    CHK_TRUE( trend.is_shrinking(900, WINDOW_MS) );

    // Still catching up, however far behind it remains.
    TestSuite::sleep_ms(WINDOW_MS * 2, "let the window elapse");
    CHK_TRUE( trend.is_shrinking(800, WINDOW_MS) );

    // A gap that did not move: not keeping up.
    TestSuite::sleep_ms(WINDOW_MS * 2, "let the window elapse");
    CHK_FALSE( trend.is_shrinking(800, WINDOW_MS) );

    // A gap that grew: not keeping up either.
    TestSuite::sleep_ms(WINDOW_MS * 2, "let the window elapse");
    CHK_FALSE( trend.is_shrinking(900, WINDOW_MS) );

    // Resuming progress from there is recognised, i.e. the comparison is
    // against the previous window and not against the smallest gap ever seen:
    // a member above the threshold could never reach that again.
    TestSuite::sleep_ms(WINDOW_MS * 2, "let the window elapse");
    CHK_TRUE( trend.is_shrinking(850, WINDOW_MS) );

    // `restart_window` starts a new comparison at the current gap.
    trend.restart_window(850);
    TestSuite::sleep_ms(WINDOW_MS * 2, "let the window elapse");
    CHK_FALSE( trend.is_shrinking(850, WINDOW_MS) );

    // `forget_window` leaves nothing to compare against, so the next full
    // window counts as catching up even though the gap did not move. That is
    // what makes backpressure need two windows after a reset.
    trend.forget_window();
    TestSuite::sleep_ms(WINDOW_MS * 2, "let the window elapse");
    CHK_TRUE( trend.is_shrinking(850, WINDOW_MS) );
    TestSuite::sleep_ms(WINDOW_MS * 2, "let the window elapse");
    CHK_FALSE( trend.is_shrinking(850, WINDOW_MS) );

    return 0;
}


// While the commit index is held, the leader must also stop accepting new
// client requests. Otherwise it keeps appending, the log tail runs away from
// the member it is waiting for, and the hold achieves nothing except a growing
// pile of unacknowledged entries.
int refuses_new_requests_while_active_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( prepare(pkgs, -1) );
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.slow_member_backpressure_max_uncommitted_ = 1;
        pkg->raftServer->update_params(params);
    }

    // Before any hold, requests are accepted even though entries are
    // uncommitted, because the tightened limit only applies while holding.
    // In async mode an accepted request has no result yet; only a rejection
    // sets a result code straight away.
    for (size_t ii = 0; ii < STALE_LOG_GAP; ++ii) {
        std::string msg_str = "before" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(msg_str.size() + 1);
        msg->put(msg_str);
        auto ret = s1.raftServer->append_entries( {msg} );
        CHK_EQ( cmd_result_code::RESULT_NOT_EXIST_YET, ret->get_result_code() );
        // Delivered to both, so that nothing falls behind and no hold starts.
        for (const std::string& endpoint: {s2_addr, s3_addr}) {
            s1.fNet->execReqResp(endpoint);
            s1.fNet->execReqResp(endpoint);
        }
    }

    // Push S3 past the threshold, which starts a hold.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "hold");
    CHK_HELD( s1 );

    // Now the leader must refuse, so that the log stops growing.
    std::string refused_str = "refused";
    ptr<buffer> refused = buffer::alloc(refused_str.size() + 1);
    refused->put(refused_str);
    uint64_t last_log_idx = tail(s1);
    auto rejected = s1.raftServer->append_entries( {refused} );
    CHK_EQ( cmd_result_code::TIMEOUT, rejected->get_result_code() );
    CHK_EQ( last_log_idx, tail(s1) );

    // Once S3 catches up the hold ends and requests are accepted again.
    for (size_t ii = 0; ii < STALE_LOG_GAP * 6; ++ii) {
        s1.fNet->execReqResp(s3_addr);
    }
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    std::string accepted_str = "accepted";
    ptr<buffer> accepted = buffer::alloc(accepted_str.size() + 1);
    accepted->put(accepted_str);
    auto ok = s1.raftServer->append_entries( {accepted} );
    CHK_EQ( cmd_result_code::RESULT_NOT_EXIST_YET, ok->get_result_code() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// The switch itself: off by default, a follower's request reaches the leader,
// the leader uses it and sends it on, and turning it off releases the member.
// Nothing else in this file covers the switch, so without this test the whole
// switch could be removed and every other test would still pass.
int runtime_toggle_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers(pkgs) );
    CHK_Z( make_group(pkgs) );
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.return_method_ = raft_params::async_handler;
        // Configured, but not enabled on purpose: that is the default state
        // an operator starts from.
        params.slow_member_backpressure_max_duration_ = -1;
        params.slow_member_backpressure_gap_window_ = -1;
        params.slow_member_backpressure_max_uncommitted_ = STALE_LOG_GAP * 100;
        params.stale_log_gap_ = STALE_LOG_GAP;
        params.fresh_log_gap_ = 1;
        // No snapshots: a member that falls behind far enough would be sent
        // one, which excludes it for a reason that has nothing to do with the
        // switch under test.
        params.snapshot_distance_ = 0;
        pkg->raftServer->update_params(params);
        CHK_FALSE( pkg->raftServer->get_current_params()
                       .slow_member_backpressure_enabled_ );
    }

    // Off: the leader just runs ahead of a member that falls behind.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "off");
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_NOT_HELD( s1 );

    // A follower asks for it, which has to reach the leader.
    CHK_TRUE( s3.raftServer->request_slow_member_backpressure(true) );
    s3.fNet->execReqResp(s1_addr);
    CHK_TRUE( s1.raftServer->get_current_params()
                  .slow_member_backpressure_enabled_ );

    // The leader sends it on, so the other members report it too.
    s1.fNet->execReqResp(s2_addr);
    s1.fNet->execReqResp(s3_addr);
    CHK_TRUE( s2.raftServer->get_current_params()
                  .slow_member_backpressure_enabled_ );

    // On: the same lag now holds the commit index back.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "on");
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_HELD( s1 );

    // Off again, from the leader this time: the hold is released without the
    // member having caught up.
    CHK_TRUE( s1.raftServer->request_slow_member_backpressure(false) );
    CHK_FALSE( s1.raftServer->get_current_params()
                   .slow_member_backpressure_enabled_ );
    append_and_deliver_to(s1, 1, {s2_addr}, "off_again");
    // Turning it off also sends the setting to every peer, over the same
    // connection as the append answers, so keep delivering S2's messages until
    // the commit index reaches the last log entry. S3 is left far behind on
    // purpose: if the hold were still in place, delivering S2's messages could
    // never move the commit index past S3's matched index.
    uint64_t last_log_idx = tail(s1);
    for (int ii = 0; ii < 40; ++ii) {
        if (s1.raftServer->get_target_committed_log_idx() == last_log_idx) break;
        // The heartbeat is what re-sends an entry to a peer that was busy with
        // the switch message when it was appended.
        s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        while (s1.fNet->execReqResp(s2_addr)) {}
    }
    CHK_EQ( last_log_idx, s1.raftServer->get_target_committed_log_idx() );
    CHK_GT( last_log_idx - matched_idx_of(s1, 3), STALE_LOG_GAP );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// The release threshold: a member is released as soon as it is back within one
// replication batch of the hold threshold, not only when it has caught up
// completely. Without this, a release either far too late or a batch too early
// would go unnoticed.
int release_happens_at_the_release_threshold_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( prepare(pkgs, -1) );
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.max_append_size_ = 1;
        pkg->raftServer->update_params(params);
    }
    // With a one entry batch the release threshold is one below the hold
    // threshold, so the two are adjacent and the boundary is exact.
    const uint64_t RELEASE_GAP = STALE_LOG_GAP - 1;

    append_and_deliver_to(s1, STALE_LOG_GAP + 2, {s2_addr}, "boundary");
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_HELD( s1 );

    // Feed the member one message at a time. Above the release threshold it
    // must stay held; the step that brings it to or below the threshold must
    // release it, with the member still behind.
    bool released = false;
    for (int ii = 0; ii < 60 && !released; ++ii) {
        uint64_t gap = tail(s1) - matched_idx_of(s1, 3);
        if (gap > RELEASE_GAP) {
            CHK_HELD( s1 );
        } else {
            released = true;
            break;
        }
        s1.fNet->execReqResp(s3_addr);
    }
    CHK_TRUE( released );
    CHK_SMEQ( tail(s1) - matched_idx_of(s1, 3), RELEASE_GAP );
    CHK_GT( tail(s1) - matched_idx_of(s1, 3), (uint64_t)0 );
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_NOT_HELD( s1 );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// With two members behind, the commit index has to be held at the position of
// the member that is furthest behind. Holding at whichever member the loop
// happens to see last would let the leader run ahead of the member that needs
// the help most.
//
// The commit index never moves backwards, so this is only observable when the
// nearer member has itself passed the point where the commit index froze: then
// holding at the nearer member would advance the commit index, and holding at
// the further one cannot.
int holds_at_the_furthest_lagging_member_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";
    std::string s4_addr = "S4";
    std::string s5_addr = "S5";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    RaftPkg s4(f_base, 4, s4_addr);
    RaftPkg s5(f_base, 5, s5_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3, &s4, &s5};

    CHK_Z( prepare(pkgs, -1) );
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.max_append_size_ = 1;
        // No snapshots: a member far enough behind would be sent one, which
        // excludes it for a reason unrelated to what this test checks.
        params.snapshot_distance_ = 0;
        pkg->raftServer->update_params(params);
    }

    // S4 and S5 stop receiving, so both fall behind and the hold engages.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr, s3_addr}, "two_a");
    while (s1.fNet->execReqResp(s2_addr)) {}
    while (s1.fNet->execReqResp(s3_addr)) {}
    CHK_HELD( s1 );
    uint64_t frozen_at = s1.raftServer->get_target_committed_log_idx();

    // The tail runs far ahead while they stay behind.
    append_and_deliver_to(s1, STALE_LOG_GAP * 8, {s2_addr, s3_addr}, "two_b");
    while (s1.fNet->execReqResp(s2_addr)) {}
    while (s1.fNet->execReqResp(s3_addr)) {}

    // Both catch up past the point where the commit index froze, but to
    // different positions and both still far from the tail. They are advanced
    // by different amounts on purpose, so that holding at either one of them
    // gives a different commit index and the choice is observable.
    for (int ii = 0; ii < 200 && matched_idx_of(s1, 5) < frozen_at + 3; ++ii) {
        s1.fNet->execReqResp(s5_addr);
    }
    for (int ii = 0; ii < 400 && matched_idx_of(s1, 4) < frozen_at + 12; ++ii) {
        s1.fNet->execReqResp(s4_addr);
    }
    uint64_t furthest = matched_idx_of(s1, 5);
    CHK_GT( furthest, frozen_at );
    CHK_SM( furthest, matched_idx_of(s1, 4) );
    CHK_GT( tail(s1) - matched_idx_of(s1, 4), STALE_LOG_GAP );

    // Held at the member that is furthest behind, so the commit index reaches
    // its position and no further - not the position of the nearer one.
    append_and_deliver_to(s1, 1, {s2_addr, s3_addr}, "two_c");
    while (s1.fNet->execReqResp(s2_addr)) {}
    while (s1.fNet->execReqResp(s3_addr)) {}
    CHK_EQ( furthest, s1.raftServer->get_target_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// A member that goes silent without its requests failing must be released too:
// it cannot catch up while it is not answering, so waiting for it only stalls
// the cluster. This is the responsiveness window on its own, with no rpc error
// and a matched index already known, which is what every other test conflates.
int silent_member_is_released_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    // Shrink the responsiveness window so silence is noticed within the test:
    // it is `full_consensus_leader_limit` heartbeats. These limits are process
    // global, so put them back however this test ends - an early return from a
    // failed check would otherwise leave the shrunken window in place for
    // every test that runs afterwards.
    struct limits_guard {
        explicit limits_guard(raft_server::limits saved): saved_(saved) {}
        ~limits_guard() { raft_server::set_raft_limits(saved_); }
        raft_server::limits saved_;
    } guard(raft_server::get_raft_limits());

    raft_server::limits new_limits = raft_server::get_raft_limits();
    new_limits.full_consensus_leader_limit_ = 2;
    raft_server::set_raft_limits(new_limits);

    CHK_Z( prepare(pkgs, -1) );
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.heart_beat_interval_ = 20;
        params.max_append_size_ = 1;
        pkg->raftServer->update_params(params);
    }

    // S3 falls behind and is held, with its matched index known and no errors.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "silent");
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_GT( matched_idx_of(s1, 3), (uint64_t)0 );
    CHK_HELD( s1 );

    // Now it simply stops answering. Once the window passes it is no longer
    // eligible, so the commit index proceeds on the majority.
    TestSuite::sleep_ms(20 * 2 * 5, "let the responsiveness window pass");
    append_and_deliver_to(s1, 1, {s2_addr}, "silent_more");
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_NOT_HELD( s1 );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

}  // namespace slow_member_backpressure_test
using namespace slow_member_backpressure_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    // Deterministic behavior.
    debugging_options::get_instance().disable_reconn_backoff_ = true;

    ts.doTest( "holds commit for lagging member test",
               holds_commit_for_lagging_member_test );

    ts.doTest( "no hold time configured test",
               no_hold_time_configured_test );

    ts.doTest( "gives up after max duration test",
               gives_up_after_max_duration_test );

    ts.doTest( "hold threshold is exact test",
               hold_threshold_is_exact_test );

    ts.doTest( "learner is never held test",
               learner_is_never_held_test );

    ts.doTest( "not held with custom quorum size test",
               not_held_with_custom_quorum_size_test );

    ts.doTest( "not held while still catching up test",
               not_held_while_still_catching_up_test );

    ts.doTest( "rpc error alone releases lagging member test",
               rpc_error_alone_releases_lagging_member_test );

    ts.doTest( "never responded member is not held test",
               never_responded_member_is_not_held_test );

    ts.doTest( "give up flag survives reconnect test",
               give_up_flag_survives_reconnect_test );

    ts.doTest( "snapshot receiver is held test",
               snapshot_receiver_is_held_test );

    ts.doTest( "commit index never regresses test",
               commit_index_never_regresses_test );

    ts.doTest( "gives up on a slowly progressing member test",
               gives_up_on_a_slowly_progressing_member_test );

    ts.doTest( "held again after catching up test",
               held_again_after_catching_up_test );

    ts.doTest( "gap trend test",
               gap_trend_test );

    ts.doTest( "release happens at the release threshold test",
               release_happens_at_the_release_threshold_test );

    ts.doTest( "holds at the furthest lagging member test",
               holds_at_the_furthest_lagging_member_test );

    ts.doTest( "silent member is released test",
               silent_member_is_released_test );

    ts.doTest( "refuses new requests while active test",
               refuses_new_requests_while_active_test );

    ts.doTest( "runtime toggle test",
               runtime_toggle_test );

    return 0;
}
