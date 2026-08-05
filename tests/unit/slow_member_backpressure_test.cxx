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
                                  const std::string& prefix)
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
    }
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

    return 0;
}
