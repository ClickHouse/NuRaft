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

// The last log index of a peer, from the leader's point of view, i.e. exactly
// what the backpressure compares against.
static uint64_t matched_idx_of(RaftPkg& leader, int32_t peer_id) {
    return leader.raftServer->get_peer_info(peer_id).last_log_idx_;
}

static int prepare(std::vector<RaftPkg*>& pkgs,
                   int32_t max_hold_ms)
{
    CHK_Z( launch_servers(pkgs) );
    CHK_Z( make_group(pkgs) );

    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        // Otherwise `append_entries` blocks until commit, which is exactly
        // what this test holds back on purpose.
        params.return_method_ = raft_params::async_handler;
        params.slow_member_backpressure_max_hold_ = max_hold_ms;
        // The gap trend is covered separately by
        // `not_held_while_still_catching_up_test`; every other test is about
        // the threshold and the time limit, so the trend is switched off here
        // to keep them independent of wall-clock timing.
        params.slow_member_backpressure_gap_window_ = -1;
        params.stale_log_gap_ = STALE_LOG_GAP;
        params.fresh_log_gap_ = 1;
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

    uint64_t commit_idx = s1.raftServer->get_committed_log_idx();

    // S3 gets nothing, so it falls behind by more than the stale log gap,
    // while S1 and S2 alone are a majority.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "held");

    // Entries keep committing until S3 is more than `stale_log_gap_` behind,
    // and then the leader starts waiting for it: the commit index stops well
    // short of the last log index, even though S1 and S2 are a majority.
    uint64_t last_log_idx = s1.getTestMgr()->load_log_store()->next_slot() - 1;
    uint64_t held_at = s1.raftServer->get_committed_log_idx();
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

// With the feature disabled, the same lagging member is simply outrun.
int disabled_by_default_test() {
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

    uint64_t commit_idx = s1.raftServer->get_committed_log_idx();

    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "outrun");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // A majority acknowledged, so everything is committed without S3.
    CHK_GT( s1.raftServer->get_committed_log_idx(), commit_idx );
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// A member that never catches up must be given up on, otherwise one broken
// member would stop the cluster from committing anything at all.
int gives_up_after_max_hold_test() {
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

    uint64_t commit_idx = s1.raftServer->get_committed_log_idx();

    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "giveup");

    // Still within the time limit: the commit index is held short of the log.
    CHK_GT( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

    TestSuite::sleep_ms(MAX_HOLD_MS * 2, "wait for the leader to give up on S3");

    // One more round of replication to S2, which is enough to commit now:
    // the leader gave up on S3 and no longer waits for it.
    append_and_deliver_to(s1, 1, {s2_addr}, "after_giveup");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
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
    uint64_t last_log_idx = s1.getTestMgr()->load_log_store()->next_slot() - 1;
    CHK_EQ( STALE_LOG_GAP, last_log_idx - matched_idx_of(s1, 3) );
    CHK_EQ( last_log_idx, s1.raftServer->get_committed_log_idx() );

    // One entry past the threshold: the leader starts waiting.
    append_and_deliver_to(s1, 1, {s2_addr}, "past");
    CHK_EQ( last_log_idx, s1.raftServer->get_committed_log_idx() );
    CHK_GT( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// A member that stops responding must not be waited for, otherwise every crash
// would stall the cluster.
int not_held_when_unresponsive_test() {
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

    // Fail S3's requests, which is what an unreachable member looks like. The
    // failures have to be delivered, otherwise the leader never learns.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "unresponsive",
                          {s3_addr});
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // S1 and S2 are a majority and S3 cannot catch up by being waited for.
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// The release threshold is strictly below the hold threshold. A member that is
// between the two stays held, which is what stops it from being released and
// held again on every batch.
int release_threshold_is_below_hold_test() {
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

    // One entry per replication batch, so that the gap can be stepped by one
    // and the release threshold is `STALE_LOG_GAP - 1`, i.e. the band in which
    // a member stays held is exactly a gap of `STALE_LOG_GAP`.
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.max_append_size_ = 1;
        pkg->raftServer->update_params(params);
    }

    // Cross the hold threshold: the gap becomes `STALE_LOG_GAP + 1`.
    append_and_deliver_to(s1, STALE_LOG_GAP + 1, {s2_addr}, "band");
    CHK_EQ( STALE_LOG_GAP + 1,
            s1.getTestMgr()->load_log_store()->next_slot() - 1
                - matched_idx_of(s1, 3) );
    CHK_GT( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

    // One entry acknowledged: the gap is now exactly at the hold threshold,
    // which is still above the release threshold, so the member stays held.
    s1.fNet->execReqResp(s3_addr);
    s1.fNet->execReqResp(s3_addr);
    CHK_EQ( STALE_LOG_GAP,
            s1.getTestMgr()->load_log_store()->next_slot() - 1
                - matched_idx_of(s1, 3) );

    // Nothing new is appended on purpose: replicating further would let the
    // leader decide the member needs a snapshot, which releases it for an
    // unrelated reason.
    CHK_GT( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

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
        params.slow_member_backpressure_max_hold_ = -1;
        params.slow_member_backpressure_gap_window_ = -1;
        params.stale_log_gap_ = STALE_LOG_GAP;
        params.fresh_log_gap_ = 1;
        pkg->raftServer->update_params(params);
    }

    // The learner falls far behind, and the two voting members commit anyway.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "learner");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
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
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
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
    uint64_t gap = s1.getTestMgr()->load_log_store()->next_slot() - 1
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
        uint64_t last_log_idx = s1.getTestMgr()->load_log_store()->next_slot() - 1;
        CHK_EQ( last_log_idx, s1.raftServer->get_target_committed_log_idx() );
    }

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// An rpc error alone must release a held member, even while its matched index
// is still known (non-zero). This is deliberately different from
// `not_held_when_unresponsive_test`: there the failure also makes the leader
// re-create the connection, which resets the matched index to zero, so the
// member is ineligible for two reasons at once and either check would hide a
// bug in the other. Here the failure is observed before any new request is
// sent, so the matched index is still intact and only the rpc error counter
// can exclude the member.
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
    CHK_GT( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

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
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// A member that has never acknowledged anything (matched index zero) must not
// be waited for, even though its connection is healthy and has never failed.
// This is deliberately different from `not_held_when_unresponsive_test`: a
// failed request also raises the rpc error counter, which would exclude the
// member anyway and hide a bug in the matched-index check. A freshly added
// member has a healthy connection, no errors, and no acknowledgements.
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
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );
    CHK_EQ( 0, matched_idx_of(s1, 4) );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    s4.raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// The give-up latch must survive the member becoming ineligible and eligible
// again without catching up: otherwise a flapping member earns a fresh hold
// on every flap and stalls the cluster for `max_hold` over and over, without
// bound. The test first proves that the give-up actually happened, then makes
// the member flap through a connection failure, and then would stall forever
// (rather than for one more `max_hold`) if a fresh hold were granted, so a
// cleared latch cannot go unnoticed.
int give_up_latch_survives_connection_flap_test() {
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
    CHK_GT( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );
    TestSuite::sleep_ms(MAX_HOLD_MS * 2, "wait for the leader to give up on S3");
    append_and_deliver_to(s1, 1, {s2_addr}, "flap_b");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

    // The flap: S3's connection fails once, making it ineligible, and then
    // recovers, making it eligible again while still far behind.
    s1.fNet->makeReqFailAll(s3_addr);
    append_and_deliver_to(s1, 1, {s2_addr}, "flap_c");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    // Recovery: one successful exchange. S3 is eligible again, and still far
    // behind, because a batch is one entry.
    s1.fNet->execReqResp(s3_addr);
    s1.fNet->execReqResp(s3_addr);
    CHK_GT( s1.getTestMgr()->load_log_store()->next_slot() - 1
                - matched_idx_of(s1, 3),
            STALE_LOG_GAP );

    // From now on, an (incorrect) fresh hold would be practically unbounded,
    // so the commit index would stall for good, not just for `max_hold`.
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.slow_member_backpressure_max_hold_ = 3600 * 1000;
        pkg->raftServer->update_params(params);
    }

    // The member was given up on and has not caught up since, so nothing may
    // be held: everything must keep committing.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "flap_d");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// A member that is receiving a snapshot must not be waited for: the snapshot
// does not go any faster with the commit index held. Everything else about
// the member is healthy, so only the snapshot check can exclude it.
int snapshot_receiver_is_not_held_test() {
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
    // up, and the exchange after it has to be a snapshot: `RaftPkg` takes a
    // snapshot every 5 commits, and S3 will be behind the latest one.
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.max_append_size_ = 1;
        pkg->raftServer->update_params(params);
    }

    // S3 falls behind and the hold begins. The commit index that was reached
    // is past the leader's snapshot point (every 5 commits).
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "snap");
    CHK_GT( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );
    CHK_GT( s1.getTestSm()->getNumSnapshotCreations(), 0 );

    // One exchange: S3 acknowledges a single entry, and the leader finds out
    // that S3's next log index is at or below the latest snapshot, so it
    // starts sending the snapshot.
    s1.fNet->execReqResp(s3_addr);
    s1.fNet->execReqResp(s3_addr);

    // Still far behind, but receiving a snapshot: nothing may be held.
    CHK_GT( s1.getTestMgr()->load_log_store()->next_slot() - 1
                - matched_idx_of(s1, 3),
            STALE_LOG_GAP );
    append_and_deliver_to(s1, 1, {s2_addr}, "snap_b");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// While the commit index is held, the index handed to
// `state_machine::adjust_commit_index` must never fall below the current
// commit index: the lagging member's matched index is usually below what has
// already been committed, and handing that value to the state machine would
// break the API promise that the expected index never regresses.
int held_commit_index_never_regresses_test() {
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
    CHK_GT( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

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
    CHK_GT( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_target_committed_log_idx() );

    // Trickle: progress on every step, but never enough.
    for (int ii = 0; ii < 4; ++ii) {
        TestSuite::sleep_ms(MAX_HOLD_MS / 2, "trickle");
        s1.fNet->execReqResp(s3_addr);
        s1.fNet->execReqResp(s3_addr);
    }
    uint64_t gap = s1.getTestMgr()->load_log_store()->next_slot() - 1
                   - matched_idx_of(s1, 3);
    CHK_GT( gap, STALE_LOG_GAP );

    // The limit expired while it was still behind, so it must have been given
    // up on and everything a majority has must be committed.
    append_and_deliver_to(s1, 1, {s2_addr}, "after_slow");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// The full cycle: a member is held, given up on, catches up completely, and is
// then eligible to be held again when it falls behind a second time. Without
// the latch being cleared on catch-up, the member could never be waited for
// again for the rest of the leader's term.
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
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_committed_log_idx() );

    // Let S3 catch up completely, which is what clears the give-up.
    for (size_t ii = 0; ii < STALE_LOG_GAP * 6; ++ii) {
        s1.fNet->execReqResp(s3_addr);
    }
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            matched_idx_of(s1, 3) );

    // Round two: fall behind again. It must be held again, i.e. the commit
    // index must stop short of the log.
    append_and_deliver_to(s1, STALE_LOG_GAP * 2, {s2_addr}, "round2");
    CHK_GT( s1.getTestMgr()->load_log_store()->next_slot() - 1,
            s1.raftServer->get_target_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}



// A member that fell behind while online and is now draining its backlog must
// not be held, even though it is still far behind and even though writes keep
// arriving. This is the case the gap trend exists for, and it is checked with
// the trend enabled, which is the only configuration reachable from Keeper.
int not_held_while_draining_under_load_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    const int32_t GAP_WINDOW_MS = 60;
    CHK_Z( prepare(pkgs, -1) );
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.slow_member_backpressure_gap_window_ = GAP_WINDOW_MS;
        // Four entries per batch, so that one exchange with S3 outpaces the
        // one new write per round and its gap really shrinks.
        params.max_append_size_ = 4;
        pkg->raftServer->update_params(params);
    }

    // S3 keeps up at first, so the trend baseline is a small gap, exactly as
    // for a member that has been healthy until now. This is what made an
    // earlier version of the trend hold a draining member: the baseline was
    // the historical minimum, which a member above the threshold can never
    // reach again.
    append_and_deliver_to(s1, 2, {s2_addr, s3_addr}, "warm");
    TestSuite::sleep_ms(GAP_WINDOW_MS * 2, "settle the trend baseline");

    // A burst leaves S3 far behind.
    append_and_deliver_to(s1, STALE_LOG_GAP * 6, {s2_addr}, "burst");
    uint64_t gap = s1.getTestMgr()->load_log_store()->next_slot() - 1
                   - matched_idx_of(s1, 3);
    CHK_GT( gap, STALE_LOG_GAP );
    uint64_t initial_gap = gap;

    // S3 now drains its backlog while writes keep arriving: one new entry per
    // round against one exchange that carries up to four. Several windows
    // elapse. It must never be held, so the commit index has to follow every
    // new entry - the leader and S2 have them all, and only a hold could stop
    // the commit index from reaching the tail.
    for (int round = 0; round < 5; ++round) {
        TestSuite::sleep_ms(GAP_WINDOW_MS, "let a trend window elapse");

        // One request and its response, which leaves the peer free for the
        // next round; a partial exchange would leave it busy and stall
        // replication entirely.
        // Two full exchanges against one new write, so the gap shrinks. Each
        // exchange is a request and its response; delivering a partial one
        // would leave the peer busy and stall replication entirely.
        for (int ii = 0; ii < 2; ++ii) {
            s1.fTimer->invoke( timer_task_type::heartbeat_timer );
            s1.fNet->execReqResp(s3_addr);
            s1.fNet->execReqResp(s3_addr);
        }

        append_and_deliver_to(s1, 1, {s2_addr}, "load" + std::to_string(round));

        gap = s1.getTestMgr()->load_log_store()->next_slot() - 1
              - matched_idx_of(s1, 3);
        CHK_GT( gap, STALE_LOG_GAP );
        CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot() - 1,
                s1.raftServer->get_target_committed_log_idx() );
    }

    // It really was closing the distance, and really was still far behind.
    CHK_SM( gap, initial_gap );
    CHK_GT( gap, STALE_LOG_GAP );

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

    ts.doTest( "disabled by default test",
               disabled_by_default_test );

    ts.doTest( "gives up after max hold test",
               gives_up_after_max_hold_test );

    ts.doTest( "hold threshold is exact test",
               hold_threshold_is_exact_test );

    ts.doTest( "not held when unresponsive test",
               not_held_when_unresponsive_test );

    ts.doTest( "release threshold is below hold test",
               release_threshold_is_below_hold_test );

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

    ts.doTest( "give up latch survives connection flap test",
               give_up_latch_survives_connection_flap_test );

    ts.doTest( "snapshot receiver is not held test",
               snapshot_receiver_is_not_held_test );

    ts.doTest( "held commit index never regresses test",
               held_commit_index_never_regresses_test );

    ts.doTest( "gives up on a slowly progressing member test",
               gives_up_on_a_slowly_progressing_member_test );

    ts.doTest( "held again after catching up test",
               held_again_after_catching_up_test );

    ts.doTest( "not held while draining under load test",
               not_held_while_draining_under_load_test );

    return 0;
}
