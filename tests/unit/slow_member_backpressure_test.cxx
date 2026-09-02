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

// How far a member is left behind in these tests. The backpressure has no
// threshold of its own, so this is only an amount of entries that is easy to
// see in the assertions.
static const size_t LAG = 5;

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

// The last log index of the leader's own log, i.e. how far ahead of a member
// the leader has run.
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

// The last log index of a peer, from the leader's point of view. This is
// `last_accepted_log_idx_`, which the leader sets together with the matched
// index the backpressure reads, and which is the only one exposed publicly.
static uint64_t matched_idx_of(RaftPkg& leader, int32_t peer_id) {
    return leader.raftServer->get_peer_info(peer_id).last_log_idx_;
}

// Turns the backpressure on directly in the parameters. The switch that an
// operator uses is covered by `runtime_toggle_test`.
static int prepare(std::vector<RaftPkg*>& pkgs) {
    CHK_Z( launch_servers(pkgs) );
    CHK_Z( make_group(pkgs) );

    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        // Otherwise `append_entries` blocks until commit, which is exactly
        // what this test holds back on purpose.
        params.return_method_ = raft_params::async_handler;
        // Off by default: an operator turns it on, so the tests do too.
        params.slow_member_backpressure_enabled_ = true;
        // These tests build a backlog by appending while the commit index is
        // held, so the append brake has to stay out of the way.
        // `refuses_new_requests_while_active_test` sets its own value,
        // because the brake is what it tests.
        params.slow_member_backpressure_max_uncommitted_ = 0;
        pkg->raftServer->update_params(params);
    }
    return 0;
}

// A member that is alive but falling behind must hold the commit index back,
// even though the other members form a majority. There is no threshold: the
// commit index follows the slowest member entry by entry.
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

    CHK_Z( prepare(pkgs) );

    // A single entry that S3 does not get is already enough: S1 and S2 are a
    // majority, and the commit index still does not move past S3.
    uint64_t behind_at = matched_idx_of(s1, 3);
    append_and_deliver_to(s1, 1, {s2_addr}, "one");
    CHK_HELD( s1 );
    CHK_EQ( behind_at, s1.raftServer->get_target_committed_log_idx() );

    // The leader runs further ahead, and the commit index stays exactly at
    // S3's matched index rather than at the majority's.
    append_and_deliver_to(s1, LAG * 2, {s2_addr}, "held");
    uint64_t last_log_idx = tail(s1);
    CHK_EQ( behind_at, matched_idx_of(s1, 3) );
    CHK_EQ( behind_at, s1.raftServer->get_target_committed_log_idx() );
    CHK_GT( last_log_idx, behind_at );

    // Let S3 catch up. Once it has everything, commits resume.
    for (size_t ii = 0; ii < LAG * 6; ++ii) {
        s1.fNet->execReqResp(s3_addr);
    }
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( last_log_idx, s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// A learner is not part of the quorum, so the leader must never wait for it.
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
        pkg->raftServer->update_params(params);
    }

    // The learner falls far behind, and the two voting members commit anyway.
    append_and_deliver_to(s1, LAG * 2, {s2_addr}, "learner");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( tail(s1), s1.raftServer->get_committed_log_idx() );

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

    CHK_Z( prepare(pkgs) );

    raft_params params = s1.raftServer->get_current_params();
    params.custom_commit_quorum_size_ = 2;
    s1.raftServer->update_params(params);

    append_and_deliver_to(s1, LAG * 2, {s2_addr}, "custom");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( tail(s1), s1.raftServer->get_committed_log_idx() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// A member whose requests fail cannot catch up by being waited for, so the
// leader must stop waiting as soon as a request to it fails. The rpc error is
// the only signal available at that moment: the response timer is only reset
// by an accepted response, so a member that has just stopped still looks
// responsive until the expiry passes.
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

    CHK_Z( prepare(pkgs) );

    // S3 falls behind with a healthy connection: the leader waits for it.
    append_and_deliver_to(s1, LAG * 2, {s2_addr}, "pre");
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
    CHK_EQ( tail(s1), s1.raftServer->get_committed_log_idx() );

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

    CHK_Z( prepare(pkgs) );
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
    append_and_deliver_to(s1, LAG * 2, {s2_addr, s3_addr}, "fresh");
    CHK_Z( wait_for_sm_exec(pkgs_no_s4, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( tail(s1), s1.raftServer->get_committed_log_idx() );
    CHK_EQ( 0, matched_idx_of(s1, 4) );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    s4.raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// A member that goes silent without its requests failing must be released too:
// it makes no progress while it is not answering, so waiting for it only
// stalls the cluster. This is the no progress timeout on its own, with no rpc
// error and a matched index already known, which is what every other test
// conflates.
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

    CHK_Z( prepare(pkgs) );
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        // Short enough to pass within the test. The default is measured in
        // tens of seconds, because a member applying a big snapshot is quiet
        // for a long time and is exactly the one worth waiting for.
        params.slow_member_backpressure_no_progress_timeout_ = 100;
        pkg->raftServer->update_params(params);
    }

    // S3 falls behind and is waited for, with its matched index known and no
    // errors.
    append_and_deliver_to(s1, LAG * 2, {s2_addr}, "silent");
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_GT( matched_idx_of(s1, 3), (uint64_t)0 );
    CHK_HELD( s1 );

    // Now it simply stops making progress. Once the timeout passes it is no
    // longer waited for, so the commit index proceeds on the majority.
    TestSuite::sleep_ms(300, "let the no progress timeout pass");
    append_and_deliver_to(s1, 1, {s2_addr}, "silent_more");
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_NOT_HELD( s1 );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// With the no progress timeout switched off, a member that the leader can
// still reach is waited for however long it takes. An operator who asks for
// that has to be able to rely on it: nothing but `bpof` should end it.
int no_progress_timeout_zero_waits_indefinitely_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( prepare(pkgs) );
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.slow_member_backpressure_no_progress_timeout_ = 0;
        pkg->raftServer->update_params(params);
    }

    append_and_deliver_to(s1, LAG * 2, {s2_addr}, "forever");
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_HELD( s1 );

    // Long enough that any timeout short enough to be useful would have
    // expired. S3 answers nothing, and its requests do not fail.
    TestSuite::sleep_ms(300, "stay silent");
    append_and_deliver_to(s1, 1, {s2_addr}, "still_forever");
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_HELD( s1 );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// Counts snapshot chunks saved by any receiver, so that
// `snapshot_receiver_is_held_test` can prove a transfer really started.
static std::atomic<uint64_t> num_snapshot_chunks_saved(0);

static cb_func::ReturnCode cb_count_snapshot_chunks(cb_func::Type type,
                                                    cb_func::Param* param)
{
    if (type == cb_func::Type::SaveSnapshot) {
        num_snapshot_chunks_saved.fetch_add(1);
    }
    return cb_default(type, param);
}

// A member receiving a snapshot is waited for as well. The transfer is no
// faster for waiting, but the log stops growing, so the member does not have
// to catch up on everything written during the transfer and then need another
// snapshot.
//
// Its matched index is stale and below the commit index, which is the case the
// floor at the current commit index has to survive: without the floor the
// leader would hand the state machine a commit index below the one it has
// already applied.
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

    num_snapshot_chunks_saved = 0;
    CHK_Z( launch_servers(pkgs, nullptr, false, cb_count_snapshot_chunks) );
    CHK_Z( make_group(pkgs) );
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.return_method_ = raft_params::async_handler;
        // One entry per batch, so that a single exchange with S3 cannot catch
        // it up and the exchange after it has to be a snapshot: `RaftPkg`
        // takes a snapshot every 5 commits, and S3 will be behind the latest
        // one.
        params.max_append_size_ = 1;
        // Off to begin with, so that the commit index can run past S3 before
        // the transfer starts. That is what makes S3's matched index stale.
        params.slow_member_backpressure_enabled_ = false;
        pkg->raftServer->update_params(params);
    }

    // Entries commit everywhere first, so that there is a snapshot for S3 to
    // fall behind.
    append_and_deliver_to(s1, LAG * 2, {s2_addr, s3_addr}, "base");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    for (int ii = 0; ii < 200 && s1.getTestSm()->getNumSnapshotCreations() == 0;
         ++ii) {
        // Snapshots are created on the apply thread, so wait for it rather
        // than assuming it has caught up already.
        TestSuite::sleep_ms(10, "wait for a snapshot");
    }
    CHK_GT( s1.getTestSm()->getNumSnapshotCreations(), 0 );

    // S3 stops receiving and the leader commits without it, so the commit
    // index ends up well above S3's matched index.
    append_and_deliver_to(s1, LAG * 2, {s2_addr}, "ahead");
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // The leader finds that S3's next log index is at or below the latest
    // snapshot and starts sending it. Stop as soon as the first chunk is
    // saved: the transfer has to be still in progress below.
    for (int ii = 0; ii < 10 && !num_snapshot_chunks_saved; ++ii) {
        s1.fNet->execReqResp(s3_addr);
    }
    CHK_GT( num_snapshot_chunks_saved.load(), (uint64_t)0 );

    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.slow_member_backpressure_enabled_ = true;
        pkg->raftServer->update_params(params);
    }

    uint64_t frozen_at = s1.raftServer->get_target_committed_log_idx();
    CHK_SM( matched_idx_of(s1, 3), frozen_at );

    // Keep writing and committing through S2 while the transfer is in
    // progress. The commit index must stay exactly where it was: not lower,
    // which would un-commit applied entries, and not higher, which would mean
    // the leader is still outrunning the member it is waiting for.
    for (int ii = 0; ii < 20; ++ii) {
        append_and_deliver_to(s1, 1, {s2_addr}, "during" + std::to_string(ii));
        while (s1.fNet->execReqResp(s2_addr)) {}
        CHK_EQ( frozen_at, s1.raftServer->get_target_committed_log_idx() );
    }
    CHK_HELD( s1 );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// With more than one member behind, the commit index must follow the one that
// is furthest behind, not the nearest one and not the quorum.
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

    CHK_Z( prepare(pkgs) );
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        params.max_append_size_ = 1;
        // No snapshots: a member far enough behind would be sent one, which
        // excludes it for a reason unrelated to what this test checks.
        params.snapshot_distance_ = 0;
        pkg->raftServer->update_params(params);
    }

    // S4 and S5 stop receiving, so both fall behind.
    append_and_deliver_to(s1, LAG * 2, {s2_addr, s3_addr}, "two_a");
    while (s1.fNet->execReqResp(s2_addr)) {}
    while (s1.fNet->execReqResp(s3_addr)) {}
    CHK_HELD( s1 );
    uint64_t frozen_at = s1.raftServer->get_target_committed_log_idx();

    // The tail runs far ahead while they stay behind.
    append_and_deliver_to(s1, LAG * 8, {s2_addr, s3_addr}, "two_b");
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
    CHK_GT( tail(s1) - matched_idx_of(s1, 4), LAG );

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

// Holding the commit index does not by itself stop the leader from taking new
// writes, so the leader must also refuse them once too many entries are
// uncommitted. Otherwise the log keeps growing and the member falls even
// further behind.
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

    CHK_Z( prepare(pkgs) );
    for (RaftPkg* pkg: pkgs) {
        raft_params params = pkg->raftServer->get_current_params();
        // No general limit, so that only the backpressure one can refuse.
        params.max_uncommitted_log_entries_ = 0;
        params.slow_member_backpressure_max_uncommitted_ = LAG;
        pkg->raftServer->update_params(params);
    }

    auto append = [&](const std::string& text) {
        ptr<buffer> msg = buffer::alloc(text.size() + 1);
        msg->put(text);
        return s1.raftServer->append_entries( {msg} );
    };

    // Everything commits, so nothing is uncommitted and nothing is refused.
    // In async mode an accepted request has no result yet; only a rejection
    // sets a result code straight away.
    for (size_t ii = 0; ii < LAG * 2; ++ii) {
        CHK_EQ( cmd_result_code::RESULT_NOT_EXIST_YET,
                append("ok" + std::to_string(ii))->get_result_code() );
        for (const std::string& endpoint: {s2_addr, s3_addr}) {
            s1.fNet->execReqResp(endpoint);
            s1.fNet->execReqResp(endpoint);
        }
    }
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // S3 stops receiving. The commit index stops with it, so uncommitted
    // entries pile up until the leader refuses, within one entry of the limit.
    uint64_t refused_after = 0;
    for (size_t ii = 0; ii < LAG * 3; ++ii) {
        uint64_t last_log_idx = tail(s1);
        if (append("pile" + std::to_string(ii))->get_result_code() ==
            cmd_result_code::TIMEOUT) {
            // A refused request must not reach the log.
            CHK_EQ( last_log_idx, tail(s1) );
            refused_after = ii;
            break;
        }
        s1.fNet->execReqResp(s2_addr);
        s1.fNet->execReqResp(s2_addr);
    }
    CHK_GT( refused_after, (uint64_t)0 );
    CHK_SMEQ( refused_after, (uint64_t)(LAG + 1) );
    CHK_HELD( s1 );

    // Once S3 catches up everything commits, and requests are accepted again.
    for (size_t ii = 0; ii < LAG * 8; ++ii) {
        s1.fNet->execReqResp(s3_addr);
    }
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    CHK_EQ( cmd_result_code::RESULT_NOT_EXIST_YET,
            append("accepted")->get_result_code() );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// The switch lasts for one leadership, and a server that stops leading must
// stop reporting it. Only a leader acts on the setting, so a deposed leader
// that kept a stale `true` would tell an operator the cluster is throttled
// when it is not - and `mntr` is read exactly when that matters.
int switched_off_when_leadership_is_lost_test() {
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

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_TRUE( s1.raftServer->request_slow_member_backpressure(true) );
    CHK_TRUE( s1.raftServer->get_current_params()
                  .slow_member_backpressure_enabled_ );

    // Hand leadership to S3, the same way `leader_election_test` does.
    s2.fTimer->invoke( timer_task_type::election_timer );
    s2.fNet->execReqResp();
    s3.fTimer->invoke( timer_task_type::election_timer );
    // Pre-vote, then vote.
    s3.fNet->execReqResp();
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_TRUE( s3.raftServer->is_leader() );

    // The old leader must have let go of it, and the new one must not have
    // picked it up.
    CHK_FALSE( s1.raftServer->get_current_params()
                   .slow_member_backpressure_enabled_ );
    CHK_FALSE( s3.raftServer->get_current_params()
                   .slow_member_backpressure_enabled_ );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();
    return 0;
}

// The switch itself: off by default, a follower's request reaches the leader,
// only the leader holds it, and turning it off releases the member. Nothing
// else in this file covers the switch, so without this test the whole switch
// could be removed and every other test would still pass.
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
        // No snapshots: a member that falls behind far enough would be sent
        // one, which excludes it for a reason that has nothing to do with the
        // switch under test.
        params.snapshot_distance_ = 0;
        pkg->raftServer->update_params(params);
        // Not enabled on purpose: that is the default state an operator
        // starts from.
        CHK_FALSE( pkg->raftServer->get_current_params()
                       .slow_member_backpressure_enabled_ );
    }

    // Off: the leader just runs ahead of a member that falls behind.
    append_and_deliver_to(s1, LAG * 2, {s2_addr}, "off");
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_NOT_HELD( s1 );

    // A follower asks for it, which has to reach the leader.
    CHK_TRUE( s3.raftServer->request_slow_member_backpressure(true) );
    s3.fNet->execReqResp(s1_addr);
    CHK_TRUE( s1.raftServer->get_current_params()
                  .slow_member_backpressure_enabled_ );

    // Only the leader holds it. A follower that asked for it does not set it
    // locally, so every server reports a value it can actually act on.
    CHK_FALSE( s2.raftServer->get_current_params()
                   .slow_member_backpressure_enabled_ );
    CHK_FALSE( s3.raftServer->get_current_params()
                   .slow_member_backpressure_enabled_ );

    // On: the same lag now holds the commit index back.
    append_and_deliver_to(s1, LAG * 2, {s2_addr}, "on");
    while (s1.fNet->execReqResp(s2_addr)) {}
    CHK_HELD( s1 );

    // Off again, from the leader this time: the commit index is released
    // without the member having caught up.
    CHK_TRUE( s1.raftServer->request_slow_member_backpressure(false) );
    CHK_FALSE( s1.raftServer->get_current_params()
                   .slow_member_backpressure_enabled_ );
    append_and_deliver_to(s1, 1, {s2_addr}, "off_again");
    // Keep delivering S2's messages until the commit index reaches the last
    // log entry. S3 is left far behind on purpose: if the commit index were
    // still held, delivering S2's messages could never move it past S3's
    // matched index.
    uint64_t last_log_idx = tail(s1);
    for (int ii = 0; ii < 40; ++ii) {
        if (s1.raftServer->get_target_committed_log_idx() == last_log_idx) break;
        // The heartbeat is what re-sends an entry to a peer that was busy with
        // the switch message when it was appended.
        s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        while (s1.fNet->execReqResp(s2_addr)) {}
    }
    CHK_EQ( last_log_idx, s1.raftServer->get_target_committed_log_idx() );
    CHK_GT( last_log_idx - matched_idx_of(s1, 3), LAG );

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

    ts.doTest( "learner is never held test",
               learner_is_never_held_test );

    ts.doTest( "not held with custom quorum size test",
               not_held_with_custom_quorum_size_test );

    ts.doTest( "rpc error alone releases lagging member test",
               rpc_error_alone_releases_lagging_member_test );

    ts.doTest( "never responded member is not held test",
               never_responded_member_is_not_held_test );

    ts.doTest( "silent member is released test",
               silent_member_is_released_test );

    ts.doTest( "no progress timeout zero waits indefinitely test",
               no_progress_timeout_zero_waits_indefinitely_test );

    ts.doTest( "snapshot receiver is held test",
               snapshot_receiver_is_held_test );

    ts.doTest( "holds at the furthest lagging member test",
               holds_at_the_furthest_lagging_member_test );

    ts.doTest( "refuses new requests while active test",
               refuses_new_requests_while_active_test );

    ts.doTest( "runtime toggle test",
               runtime_toggle_test );

    ts.doTest( "switched off when leadership is lost test",
               switched_off_when_leadership_is_lost_test );

    return 0;
}
