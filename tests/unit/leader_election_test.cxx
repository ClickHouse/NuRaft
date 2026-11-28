/************************************************************************
Copyright 2017-present eBay Inc.
Author/Developer(s): Jung-Sang Ahn

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
#include "fake_executer.hxx"
#include "handle_custom_notification.hxx"

#include "event_awaiter.hxx"
#include "raft_params.hxx"
#include "test_common.h"

#include <stdio.h>

using namespace nuraft;
using namespace raft_functional_common;

using raft_result = cmd_result< ptr<buffer> >;

namespace leader_election_test {

int leader_election_basic_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Keep the last log index.
    uint64_t last_idx = s1.raftServer->get_last_log_idx();

    // Trigger election timer of S2.
    s2.dbgLog(" --- invoke election timer of S2 ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    // Send pre-vote requests, and probably rejected by S1 and S3.
    s2.fNet->execReqResp();

    // Trigger election timer of S3.
    s3.dbgLog(" --- invoke election timer of S3 ---");
    s3.fTimer->invoke( timer_task_type::election_timer );

    // Send pre-vote requests, it will be rejected by S1, accepted by S2.
    // As a part of resp handling, it will initiate vote request.
    s3.fNet->execReqResp();
    // Send vote requests, S3 will be elected as a leader.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s3.fNet->execReqResp();
    // Follow-up: commit.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // S3's log index at becoming leader should be the next index of
    // the last one.
    TestSuite::Msg mm;
    uint64_t idx_at_leader = s3.raftServer->get_log_idx_at_becoming_leader();
    mm << "last index " << last_idx << ", log index at becoming leader "
       << idx_at_leader << std::endl;
    CHK_EQ( last_idx + 1, idx_at_leader );

    // S1 and S2's log index should be 0.
    CHK_Z( s1.raftServer->get_log_idx_at_becoming_leader() );
    CHK_Z( s2.raftServer->get_log_idx_at_becoming_leader() );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_TRUE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s3.raftServer->shutdown();
    s1.raftServer->shutdown();
    s2.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int vote_log_freshness_after_compaction_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Compact all logs of S2 into a snapshot, so that its log store
    // becomes empty and `last_entry()` returns a dummy entry with term 0.
    raft_params param = s2.raftServer->get_current_params();
    param.reserved_log_items_ = 0;
    s2.raftServer->update_params(param);

    ptr<log_store> store = s2.sMgr->load_log_store();
    const ulong last_idx = store->next_slot() - 1;
    raft_server::create_snapshot_options opt;
    opt.serialize_commit_ = true;
    CHK_EQ( last_idx, s2.raftServer->create_snapshot(opt) );
    CHK_EQ( store->start_index(), store->next_slot() );
    const ulong last_term = s2.sm->last_snapshot()->get_last_log_term();

    auto vote = [&](ulong log_term, ulong log_idx) -> bool {
        ptr<req_msg> req = cs_new<req_msg>
                           ( s2.raftServer->get_term() + 1,
                             msg_type::request_vote_request,
                             3, 2, log_term, log_idx, 0 );
        return s2.fNet->gotMsg(req)->get_accepted();
    };
    // Candidates whose log is older than S2's snapshot should be rejected.
    CHK_FALSE( vote(last_term, last_idx - 1) );
    CHK_FALSE( vote(last_term - 1, last_idx + 1) );
    // Candidate whose log is as up-to-date as S2's snapshot should be granted.
    CHK_TRUE( vote(last_term, last_idx) );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    f_base->destroy();

    return 0;
}

int leader_election_priority_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Set the priority of S2 to 100.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 100) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 85.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 85) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Trigger election timer of S2.
    s2.dbgLog(" --- invoke election timer of S2 ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    // Send pre-vote requests, and probably rejected by S1 and S3.
    s2.fNet->execReqResp();

    // Trigger election timer of S3.
    s3.dbgLog(" --- invoke election timer of S3 ---");
    // It will not initiate vote due to priority.
    s3.fTimer->invoke( timer_task_type::election_timer );

    // Trigger election timer of S3 again.
    s3.dbgLog(" --- invoke election timer of S3 ---");
    // Now it will initiate vote by help of priority decay.
    s3.fTimer->invoke( timer_task_type::election_timer );

    // Send pre-vote requests, it will be rejected by S1, accepted by S2.
    // As a part of resp handling, it will initiate vote request.
    s3.fNet->execReqResp();
    // Send vote requests, S2 will deny due to priority.
    s3.fNet->execReqResp();

    // Trigger election timer of S2.
    s2.dbgLog(" --- invoke election timer of S2 ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    // Send pre-vote requests, and accepted by S3.
    // As a result of response, it will initiate actual vote.
    s2.fNet->execReqResp();
    // Send vote requests, S3 will vote for it.
    s2.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s2.fNet->execReqResp();
    // Follow-up: commit.
    s2.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_TRUE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int leader_election_with_aggressive_node_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Set the priority of S1 to 100.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(1, 100) );
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S2 to 50.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 50) );
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 1.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 1) );
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // --- Now assume S1 is not reachable. ---

    // Trigger election timer of S2.
    s2.dbgLog(" --- invoke election timer of S2 ---");
    // It will not initiate vote due to priority.
    s2.fTimer->invoke( timer_task_type::election_timer );

    // Keep triggering election timer of S3, until it becomes leader.
    const size_t MAX_ATTEMPTS = 1000;
    size_t num_attempts = 0;
    TestSuite::UnknownProgress pp("leader election attempts: ");
    do {
        s3.dbgLog(" --- invoke election timer of S3 ---");
        s3.fTimer->invoke( timer_task_type::election_timer );
        // Drop all packets to S1.
        s3.fNet->makeReqFailAll(s1_addr);
        // Send pre-vote requests.
        s3.fNet->execReqResp();
        // Send vote request (if exists).
        s3.fNet->execReqResp();
        pp.update(++num_attempts);
    } while ( !s3.raftServer->is_leader() &&
              num_attempts < MAX_ATTEMPTS );
    pp.done();
    _msg("%zu attempts\n", num_attempts);
    CHK_SM(num_attempts, MAX_ATTEMPTS);

    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    s3.fNet->makeReqFailAll(s1_addr);
    s3.fNet->execReqResp();
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_TRUE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}
int leader_election_with_catching_up_server_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2};

    CHK_Z( launch_servers( pkgs ) );

    // Set priority of S1 to 80 and S2 to 100.
    // Each server thinks it's a leader
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(1, 80) );
    CHK_EQ( raft_server::PrioritySetResult::SET, s2.raftServer->set_priority(2, 100) );

    // Append logs to S1 to trigger log compaction.
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.log_sync_stop_gap_ = 0;
        pp->raftServer->update_params(param);
    }
    const size_t NUM = 10;
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );

        CHK_TRUE( ret->get_accepted() );

        handlers.push_back(ret);
    }
    // Pre-commit and commit.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // All handlers should be OK.
    for (auto& entry: handlers) {
        CHK_TRUE( entry->has_result() );
        CHK_EQ( cmd_result_code::OK, entry->get_result_code() );
    }

    // Add S2 to S1.
    s1.raftServer->add_srv( *s2.getTestMgr()->get_srv_config() );

    // Join req/resp.
    s1.fNet->execReqResp();
    // Add new server, notify existing peers.
    // After getting response, it will make configuration commit.
    s1.fNet->execReqResp();
    // Notify new commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Now S2 is the member of the cluster.

    // S1 resigns immediately (to schedule election timer).
    s1.raftServer->yield_leadership(true);
    CHK_FALSE( s1.raftServer->is_leader() );

    // Invoke election timer of S1.
    s1.dbgLog(" --- invoke election timer of S1 ---");
    s1.fTimer->invoke( timer_task_type::election_timer );
    // Send pre-vote request, S2 should accept it as it is in catch-up mode.
    s1.fNet->execReqResp();
    // Send vote request, S2 should accept it as it is in catch-up mode.
    s1.fNet->execReqResp();

    // Leader election should succeed.
    CHK_TRUE( s1.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int leadership_takeover_basic_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Set the priority of S2 to 10.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 10) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 5.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 5) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Yield leadership.
    s1.dbgLog(" --- yield leadership ---");
    s1.raftServer->yield_leadership();
    // Send heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    // After getting response of heartbeat, S1 will resign.
    s1.fNet->execReqResp();

    // Now S2 should have received takeover request.
    // Send vote requests.
    s2.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s2.fNet->execReqResp();
    // Follow-up: commit.
    s2.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_TRUE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    // Re-yield leadership, now S1 should be the leader again.
    s2.dbgLog(" --- yield leadership ---");
    s2.raftServer->yield_leadership();
    // Send heartbeat.
    s2.fTimer->invoke( timer_task_type::heartbeat_timer );
    s2.fNet->execReqResp();
    // After getting response of heartbeat, S2 will resign.
    s2.fNet->execReqResp();

    // Now S1 should have received takeover request.
    // Send vote requests.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s1.fNet->execReqResp();
    // Follow-up: commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int leadership_takeover_designated_successor_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Set the priority of S2 to 10.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 10) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 5.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 5) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Yield leadership to S3.
    s1.dbgLog(" --- yield leadership ---");
    s1.raftServer->yield_leadership(false, 3);
    // Send heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    // After getting response of heartbeat, S1 will resign.
    s1.fNet->execReqResp();

    // Now S3 should have received takeover request.
    // Send vote requests.
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s3.fNet->execReqResp();
    // Follow-up: commit.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_TRUE( s3.raftServer->is_leader() );

    // Re-yield leadership to S2.
    s3.dbgLog(" --- yield leadership ---");
    s3.raftServer->yield_leadership(false, 2);
    // Send heartbeat.
    s3.fTimer->invoke( timer_task_type::heartbeat_timer );
    s3.fNet->execReqResp();
    // After getting response of heartbeat, S3 will resign.
    s3.fNet->execReqResp();

    // Now S2 should have received takeover request.
    // Send vote requests.
    s2.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s2.fNet->execReqResp();
    // Follow-up: commit.
    s2.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_TRUE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    // Re-yield leadership with wrong successor,
    // S1 (highest priority server) will take over.
    s2.dbgLog(" --- yield leadership ---");
    s2.raftServer->yield_leadership(false, 12345);
    // Send heartbeat.
    s2.fTimer->invoke( timer_task_type::heartbeat_timer );
    s2.fNet->execReqResp();
    // After getting response of heartbeat, S2 will resign.
    s2.fNet->execReqResp();

    // Now S1 should have received takeover request.
    // Send vote requests.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s1.fNet->execReqResp();
    // Follow-up: commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int leadership_takeover_by_request_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Set the priority of S2 to 10.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 10) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 5.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 5) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Request leadership by the current leader, should fail.
    CHK_FALSE( s1.raftServer->request_leadership() );

    // Set callback function to refuse resignation.
    bool refuse_request = true;
    s1.ctx->set_cb_func([&](cb_func::Type t, cb_func::Param* p) -> cb_func::ReturnCode {
        if (t != cb_func::Type::ResignationFromLeader) {
            return cb_default(t, p);
        }
        if (refuse_request) {
            return cb_func::ReturnCode::ReturnNull;
        }
        return cb_func::ReturnCode::Ok;
    });

    // S3 requests the leadership from S1, and it is supposed to be declined.
    s1.dbgLog(" --- request leadership ---");
    CHK_TRUE( s3.raftServer->request_leadership() );
    // Send request.
    s3.fNet->execReqResp();

    // Send heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();

    // S1 should still be the leader.
    CHK_TRUE( s1.raftServer->is_leader() );

    // S3 requests the leadership from S1. Now it should succeed.
    refuse_request = false;
    s1.dbgLog(" --- request leadership ---");
    CHK_TRUE( s3.raftServer->request_leadership() );
    // Send request.
    s3.fNet->execReqResp();

    // Send heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    // After getting response of heartbeat, S1 will resign.
    s1.fNet->execReqResp();

    // Now S3 should have received takeover request.
    // Send vote requests.
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s3.fNet->execReqResp();
    // Follow-up: commit.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_TRUE( s3.raftServer->is_leader() );

    s1.dbgLog(" --- request leadership forwarding ---");
    CHK_TRUE( s1.raftServer->request_leadership(2) );
    s1.fNet->execReqResp();

    s2.fNet->execReqResp();

    s3.fTimer->invoke( timer_task_type::heartbeat_timer );
    s3.fNet->execReqResp();
    s3.fNet->execReqResp();

    s2.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s2.fNet->execReqResp();
    // Follow-up: commit.
    s2.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_EQ( 2, s1.raftServer->get_leader() );
    CHK_EQ( 2, s2.raftServer->get_leader() );
    CHK_EQ( 2, s3.raftServer->get_leader() );

    // Drop S3's RPC connection to leader S2 to test reconnection
    // in request_leadership.
    s3.dbgLog(" --- request leadership forwarding with dropped connection ---");
    s3.dropPeerConnection(2);
    CHK_TRUE( s3.raftServer->request_leadership(1) );
    s3.fNet->execReqResp();

    s1.fNet->execReqResp();

    s2.fTimer->invoke( timer_task_type::heartbeat_timer );
    s2.fNet->execReqResp();
    s2.fNet->execReqResp();

    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s1.fNet->execReqResp();
    // Follow-up: commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_EQ( 1, s1.raftServer->get_leader() );
    CHK_EQ( 1, s2.raftServer->get_leader() );
    CHK_EQ( 1, s3.raftServer->get_leader() );

    print_stats(pkgs);


    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

// S1 (priority 3) is the leader, S2 is a learner with `learner_priority`,
// S3 (priority 1) is a voting member.
static int launch_with_learner(const std::vector<RaftPkg*>& pkgs,
                               int32 learner_priority)
{
    auto init_cb = [&](RaftPkg* pp) {
        ptr<srv_config> s_conf = pp->getTestMgr()->get_srv_config();
        if (pp->myId == 1) {
            s_conf->set_priority(3);
        } else if (pp->myId == 2) {
            s_conf->set_learner(true);
            s_conf->set_priority(learner_priority);
        } else {
            s_conf->set_priority(1);
        }
    };
    CHK_Z( launch_servers( pkgs, nullptr, false, cb_default, init_cb ) );
    CHK_Z( make_group( pkgs ) );

    RaftPkg& s1 = *pkgs[0];
    CHK_TRUE( s1.raftServer->is_leader() );
    ptr<cluster_config> c_conf = s1.raftServer->get_config();
    CHK_FALSE( c_conf->get_server(1)->is_learner() );
    CHK_EQ( 3, c_conf->get_server(1)->get_priority() );
    CHK_TRUE( c_conf->get_server(2)->is_learner() );
    CHK_EQ( learner_priority, c_conf->get_server(2)->get_priority() );
    CHK_FALSE( c_conf->get_server(3)->is_learner() );
    CHK_EQ( 1, c_conf->get_server(3)->get_priority() );
    return 0;
}

// S1 resigned to S3: S3 becomes the leader, and the learner S2 never
// started an election.
static int check_s3_takes_over(const std::vector<RaftPkg*>& pkgs) {
    RaftPkg& s1 = *pkgs[0];
    RaftPkg& s2 = *pkgs[1];
    RaftPkg& s3 = *pkgs[2];

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_Z( s2.fNet->getNumPendingReqs(s1.myEndpoint) );
    CHK_Z( s2.fNet->getNumPendingReqs(s3.myEndpoint) );
    CHK_GT( s3.fNet->getNumPendingReqs(s1.myEndpoint), 0 );

    // Send vote requests.
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s3.fNet->execReqResp();
    // Follow-up: commit.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_TRUE( s3.raftServer->is_leader() );
    CHK_EQ( 3, s1.raftServer->get_leader() );
    CHK_EQ( 3, s2.raftServer->get_leader() );
    CHK_EQ( 3, s3.raftServer->get_leader() );
    return 0;
}

int learner_not_chosen_as_yield_successor_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_with_learner(pkgs, 2) );

    // Yield leadership without a successor.
    s1.dbgLog(" --- yield leadership ---");
    s1.raftServer->yield_leadership();
    // Send heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    // After getting response of heartbeat, S1 will resign.
    s1.fNet->execReqResp();

    CHK_Z( check_s3_takes_over(pkgs) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int learner_designated_as_yield_successor_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_with_learner(pkgs, 2) );

    // The learner requests the leadership.
    s2.dbgLog(" --- request leadership ---");
    CHK_TRUE( s2.raftServer->request_leadership() );
    // Send request.
    s2.fNet->execReqResp();

    // Send heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    // After getting response of heartbeat, S1 will resign.
    s1.fNet->execReqResp();

    CHK_Z( check_s3_takes_over(pkgs) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

// Delivers a leadership takeover request from `src` to `dst` in `term`,
// as a resigning leader sends it.
static int send_leadership_takeover(RaftPkg& src, RaftPkg& dst, ulong term) {
    ptr<req_msg> req = cs_new<req_msg>
                       ( term,
                         msg_type::custom_notification_request,
                         src.myId,
                         dst.myId,
                         src.raftServer->get_last_log_term(),
                         src.raftServer->get_last_log_idx(),
                         src.raftServer->get_committed_log_idx() );
    ptr<custom_notification_msg> custom_noti =
        cs_new<custom_notification_msg>
        ( custom_notification_msg::leadership_takeover );
    req->log_entries().push_back
        ( cs_new<log_entry>(0, custom_noti->serialize(), log_val_type::custom) );

    rpc_handler ignore_response =
        [](ptr<resp_msg>&, ptr<rpc_exception>&) { return 0; };
    src.fNet->findClient(dst.myEndpoint)->send(req, ignore_response);
    CHK_TRUE( src.fNet->delieverReqTo(dst.myEndpoint) );
    CHK_TRUE( src.fNet->handleRespFrom(dst.myEndpoint) );
    return 0;
}

int learner_ignores_leadership_takeover_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_with_learner(pkgs, 2) );

    ulong term = s1.raftServer->get_term();
    CHK_EQ( term, s2.raftServer->get_term() );
    CHK_Z( s1.fNet->getNumPendingReqs(s2.myEndpoint) );

    // The learner gets a takeover request, e.g. from a leader of an older
    // version that chose it as the successor.
    s1.dbgLog(" --- send leadership takeover request to the learner ---");
    CHK_Z( send_leadership_takeover(s1, s2, term) );

    // It does not start an election.
    CHK_EQ( term, s2.raftServer->get_term() );
    CHK_Z( s2.fNet->getNumPendingReqs(s1.myEndpoint) );
    CHK_Z( s2.fNet->getNumPendingReqs(s3.myEndpoint) );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_EQ( 1, s2.raftServer->get_leader() );
    CHK_TRUE( s1.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int learner_not_chosen_by_leadership_transfer_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    // The learner has the highest priority.
    CHK_Z( launch_with_learner(pkgs, 5) );

    // Enable leadership transfer, checked once per heartbeat interval.
    raft_params params = s1.raftServer->get_current_params();
    params.leadership_transfer_min_wait_time_ = 1;
    params.heart_beat_interval_ = 100;
    params.election_timeout_lower_bound_ = 10000;
    params.election_timeout_upper_bound_ = 20000;
    s1.raftServer->update_params(params);
    TestSuite::sleep_ms(250);

    for (size_t ii = 0; ii < 3; ++ii) {
        // Send heartbeat, S3 first so that it has responded just now when
        // the leadership transfer is checked.
        s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        s1.fNet->execReqResp(s3.myEndpoint);
        s1.fNet->execReqResp(s2.myEndpoint);
        s1.fNet->execReqResp();
    }
    s3.fNet->execReqResp();
    s2.fNet->execReqResp();

    // No server has a higher priority than S1 except the learner,
    // so S1 keeps the leadership.
    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_EQ( 1, s1.raftServer->get_leader() );
    CHK_EQ( 1, s2.raftServer->get_leader() );
    CHK_EQ( 1, s3.raftServer->get_leader() );
    CHK_Z( s2.fNet->getNumPendingReqs(s1.myEndpoint) );
    CHK_Z( s2.fNet->getNumPendingReqs(s3.myEndpoint) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int learner_flipped_during_pre_vote_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    RaftPkg s4(f_base, 4, "S4");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3, &s4};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    ulong term = s3.raftServer->get_term();

    // S3 sends pre-vote requests while it is a voting member.
    s3.dbgLog(" --- invoke election timer of S3 ---");
    s3.fTimer->invoke( timer_task_type::election_timer );
    CHK_EQ( 1, s3.fNet->getNumPendingReqs(s1.myEndpoint) );
    CHK_EQ( 1, s3.fNet->getNumPendingReqs(s2.myEndpoint) );
    CHK_EQ( 1, s3.fNet->getNumPendingReqs(s4.myEndpoint) );

    // Before the responses arrive, S3 becomes a learner.
    s1.dbgLog(" --- update config to make S3 a learner ---");
    auto enc_config = s1.raftServer->get_srv_config(s3.myId)->serialize();
    ptr<srv_config> new_config = srv_config::deserialize(*enc_config);
    new_config->set_learner(true);
    CHK_TRUE( s1.raftServer->update_srv_config(*new_config) );

    // Packet for pre-commit.
    s1.fNet->execReqResp();
    // Packet for commit.
    s1.fNet->execReqResp();
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // One more time for member that was not in quorum.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_TRUE( s3.raftServer->get_srv_config(s3.myId)->is_learner() );
    CHK_EQ( term, s3.raftServer->get_term() );

    // The leader fails, S2 and S4 stop seeing it.
    s1.fNet->goesOffline();
    s2.fTimer->invoke( timer_task_type::election_timer );
    s4.fTimer->invoke( timer_task_type::election_timer );
    CHK_FALSE( s2.raftServer->is_leader_alive() );
    CHK_FALSE( s4.raftServer->is_leader_alive() );

    // S2 and S4 grant the pre-vote of S3, which does not start a vote.
    CHK_TRUE( s3.fNet->delieverReqTo(s2.myEndpoint) );
    CHK_TRUE( s3.fNet->handleRespFrom(s2.myEndpoint) );
    CHK_TRUE( s3.fNet->delieverReqTo(s4.myEndpoint) );
    CHK_TRUE( s3.fNet->handleRespFrom(s4.myEndpoint) );

    CHK_EQ( term, s3.raftServer->get_term() );
    CHK_EQ( 1, s3.fNet->getNumPendingReqs(s1.myEndpoint) );
    CHK_Z( s3.fNet->getNumPendingReqs(s2.myEndpoint) );
    CHK_Z( s3.fNet->getNumPendingReqs(s4.myEndpoint) );
    CHK_FALSE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    s4.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int leadership_takeover_offline_candidate_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    raft_params custom_params;
    custom_params.election_timeout_lower_bound_ = 0;
    custom_params.election_timeout_upper_bound_ = 1000;
    custom_params.heart_beat_interval_ = 500;
    CHK_Z( launch_servers( pkgs, &custom_params ) );
    CHK_Z( make_group( pkgs ) );

    // Set the priority of S2 to 10.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(2, 10) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Set the priority of S3 to 5.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 5) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Wait longer than heartbeat.
    TestSuite::sleep_ms(600);

    // Now S2 is offline.
    s2.fNet->goesOffline();

    // Send heartbeat to S3 only.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();

    // Yield leadership, but now S2 is not responding sudo that
    // candidate for the next leader should be S3.
    s1.dbgLog(" --- yield leadership ---");
    s1.raftServer->yield_leadership();

    // Send heartbeat.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    // After getting response of heartbeat, S1 will resign.
    s1.fNet->execReqResp();

    // Now S3 should have received takeover request.
    // Send vote requests.
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s3.fNet->execReqResp();
    // Follow-up: commit.
    s3.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_TRUE( s3.raftServer->is_leader() );

    // Re-yield leadership, now S1 should be the leader again.
    s3.dbgLog(" --- yield leadership ---");
    s3.raftServer->yield_leadership();
    // Send heartbeat.
    s3.fTimer->invoke( timer_task_type::heartbeat_timer );
    s3.fNet->execReqResp();
    // After getting response of heartbeat, S2 will resign.
    s3.fNet->execReqResp();

    // Now S1 should have received takeover request.
    // Send vote requests.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Send new config as a new leader.
    s1.fNet->execReqResp();
    // Follow-up: commit.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int temporary_leader_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    raft_params custom_params;
    custom_params.election_timeout_lower_bound_ = 0;
    custom_params.election_timeout_upper_bound_ = 1000;
    custom_params.heart_beat_interval_ = 10;
    CHK_Z( launch_servers( pkgs, &custom_params ) );
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    // Set the priority of S3 to 0.
    CHK_EQ( raft_server::PrioritySetResult::SET, s1.raftServer->set_priority(3, 0) );
    // Send priority change reqs.
    s1.fNet->execReqResp();
    // Send reqs again for commit.
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Now S2 goes offline.
    s2.fNet->goesOffline();

    const size_t NUM = 10;

    // Append messages asynchronously.
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );

        CHK_TRUE( ret->get_accepted() );

        handlers.push_back(ret);
    }

    // Replicate.
    for (size_t ii=0; ii<3; ++ii) s1.fNet->execReqResp();
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // All handlers should be OK.
    for (auto& entry: handlers) {
        CHK_TRUE( entry->has_result() );
        CHK_EQ( cmd_result_code::OK, entry->get_result_code() );
    }

    // Now S1 goes offline, and S2 goes online.
    s1.fNet->goesOffline();
    s2.fNet->goesOnline();

    const size_t MAX_ATTEMPTS = 100;
    size_t attempts = 0;
    do {
        // Vote, S2 should be rejected all the time.
        s2.fTimer->invoke( timer_task_type::election_timer );
        // Pre-vote and vote.
        s2.fNet->execReqResp();
        s2.fNet->execReqResp();

        s3.fTimer->invoke( timer_task_type::election_timer );
        // Pre-vote and vote.
        s3.fNet->execReqResp();
        s3.fNet->execReqResp();

        // Sleep.
        attempts++;
        TestSuite::sleep_ms(custom_params.heart_beat_interval_);

        // Repeat until S3 becomes (temporary) leader.
    } while (!s3.raftServer->is_leader() && attempts < MAX_ATTEMPTS);
    CHK_SM(attempts, MAX_ATTEMPTS);

    // Commit for reconfigure.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Now S3 will yield leadership for S2.
    s3.fTimer->invoke( timer_task_type::heartbeat_timer );
    // Catch-up and commit.
    s3.fNet->execReqResp();
    s3.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Resign.
    s3.fNet->execReqResp();
    s3.fNet->execReqResp();

    // Now S2 initiates leader election.
    s2.fNet->execReqResp();
    s2.fNet->execReqResp();

    CHK_TRUE( s2.raftServer->is_leader() );
    s2.fNet->execReqResp();
    s2.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int new_leader_ignores_earlier_term_match_test() {
    // A peer's matched index from this server's earlier leadership must not
    // count in a new one: it can cover entries of the new term the peer lacks.
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

    raft_params custom_params;
    custom_params.election_timeout_lower_bound_ = 0;
    custom_params.election_timeout_upper_bound_ = 10000;
    custom_params.heart_beat_interval_ = 5000;
    custom_params.client_req_timeout_ = 1000000;
    custom_params.snapshot_distance_ = 0;
    custom_params.reserved_log_items_ = 0;
    CHK_Z( launch_servers( pkgs, &custom_params ) );
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.leadership_expiry_ = -1;
        pp->raftServer->update_params(param);
    }

    auto append_msg = [](RaftPkg& pkg, const std::string& str) {
        ptr<buffer> msg = buffer::alloc(str.size() + 1);
        msg->put(str);
        pkg.raftServer->append_entries( {msg} );
    };
    auto matched_on_s1 = [&](int peer_id) {
        return s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), peer_id);
    };
    auto fail_all_from = [&](RaftPkg& src) {
        for (RaftPkg* dst: pkgs) {
            if (dst != &src) src.fNet->makeReqFailAll(dst->myEndpoint);
        }
    };

    // Committed base, on every server.
    for (size_t ii = 0; ii < 3; ++ii) {
        append_msg(s1, "base" + std::to_string(ii));
    }
    uint64_t b = s1.raftServer->get_last_log_idx();
    for (size_t ii = 0; ii < 5; ++ii) {
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
        bool all_applied = true;
        for (RaftPkg* pp: pkgs) {
            all_applied &= pp->raftServer->get_committed_log_idx() == b;
        }
        if (all_applied) break;
    }
    uint64_t t1 = s1.raftServer->get_term();
    TestSuite::Msg mm;
    mm << "base: last idx " << b << ", term " << t1 << std::endl;
    for (RaftPkg* pp: pkgs) {
        CHK_EQ( b, pp->raftServer->get_committed_log_idx() );
        if (pp != &s1) CHK_OK( pp->getTestSm()->isSame( *s1.getTestSm() ) );
    }

    // Term T1: `b + 1 .. b + 3` reach S2 only.
    append_msg(s1, "x0");
    append_msg(s1, "x1");
    append_msg(s1, "x2");
    for (size_t ii = 0; ii < 5 && matched_on_s1(2) < b + 3; ++ii) {
        s1.fNet->execReqResp(s2_addr);
    }
    s1.fNet->makeReqFailAll(s3_addr);
    s1.fNet->makeReqFailAll(s4_addr);
    s1.fNet->makeReqFailAll(s5_addr);
    mm << "S2 last idx " << s2.raftServer->get_last_log_idx()
       << ", matched S2 " << matched_on_s1(2)
       << ", S1 commit " << s1.raftServer->get_target_committed_log_idx()
       << std::endl;
    CHK_EQ( b + 3, s2.raftServer->get_last_log_idx() );
    CHK_EQ( b + 3, matched_on_s1(2) );
    CHK_EQ( b, s1.raftServer->get_target_committed_log_idx() );

    // Term T2: S3 is elected by S4 and S5, and its first entry replaces
    // S1's tail. S2 hears nothing of it.
    s4.fTimer->invoke( timer_task_type::election_timer );
    fail_all_from(s4);
    s5.fTimer->invoke( timer_task_type::election_timer );
    fail_all_from(s5);
    s3.fTimer->invoke( timer_task_type::election_timer );
    s3.fNet->makeReqFailAll(s1_addr);
    s3.fNet->makeReqFailAll(s2_addr);
    // Pre-vote, then vote.
    s3.fNet->execReqResp(s4_addr);
    s3.fNet->execReqResp(s5_addr);
    s3.fNet->execReqResp(s4_addr);
    s3.fNet->execReqResp(s5_addr);
    CHK_TRUE( s3.raftServer->is_leader() );
    s3.fNet->makeReqFailAll(s2_addr);
    for (size_t ii = 0; ii < 10; ++ii) {
        s3.fNet->execReqResp(s1_addr);
        s3.fNet->execReqResp(s4_addr);
        s3.fNet->execReqResp(s5_addr);
        CHK_Z( wait_for_sm_exec({&s1, &s3, &s4, &s5}, COMMIT_TIMEOUT_SEC) );
        if ( s1.raftServer->get_committed_log_idx() >= b + 1 &&
             s4.raftServer->get_committed_log_idx() >= b + 1 &&
             s5.raftServer->get_committed_log_idx() >= b + 1 ) break;
    }
    s3.fNet->makeReqFailAll(s2_addr);

    uint64_t t2 = s3.raftServer->get_term();
    const std::list<uint64_t> r_idxs = s1.getTestSm()->getRollbackIdxs();
    mm << "T2 " << t2 << ", S1 term " << s1.raftServer->get_term()
       << ", S1 last idx " << s1.raftServer->get_last_log_idx()
       << " term " << s1.raftServer->get_last_log_term()
       << ", S1 commit " << s1.raftServer->get_committed_log_idx()
       << ", S1 rollbacks " << r_idxs.size()
       << ", matched S2 on S1 " << matched_on_s1(2) << std::endl;
    CHK_GT( t2, t1 );
    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_EQ( t2, s1.raftServer->get_term() );
    CHK_EQ( b + 1, s1.raftServer->get_last_log_idx() );
    CHK_EQ( t2, s1.raftServer->get_last_log_term() );
    CHK_EQ( b + 1, s3.raftServer->get_committed_log_idx() );
    CHK_EQ( b + 1, s1.raftServer->get_committed_log_idx() );
    CHK_EQ( b + 1, s4.raftServer->get_committed_log_idx() );
    CHK_EQ( b + 1, s5.raftServer->get_committed_log_idx() );
    CHK_EQ( 3, r_idxs.size() );
    uint64_t r_expected = b + 3;
    for (uint64_t idx: r_idxs) {
        CHK_EQ( r_expected--, idx );
    }
    // What S1 keeps from term T1, above its own log end now.
    CHK_EQ( b + 3, matched_on_s1(2) );
    CHK_EQ( b + 3, s2.raftServer->get_last_log_idx() );

    // Term T3: S1 is elected by S4 and S5. S2 is slow: S1's requests to it
    // stay pending.
    s4.fTimer->invoke( timer_task_type::election_timer );
    fail_all_from(s4);
    s5.fTimer->invoke( timer_task_type::election_timer );
    fail_all_from(s5);
    s1.fTimer->invoke( timer_task_type::election_timer );
    // Pre-vote, then vote.
    s1.fNet->execReqResp(s4_addr);
    s1.fNet->execReqResp(s5_addr);
    s1.fNet->execReqResp(s4_addr);
    s1.fNet->execReqResp(s5_addr);
    s1.fNet->makeReqFailAll(s3_addr);

    uint64_t t3 = s1.raftServer->get_term();
    uint64_t own_term_start = s1.raftServer->get_log_idx_at_becoming_leader();
    mm << "T3 " << t3 << ", S1 leader " << s1.raftServer->is_leader()
       << ", first log of T3 " << own_term_start
       << ", pending to S2 " << s1.fNet->getNumPendingReqs(s2_addr)
       << ", matched S2 " << matched_on_s1(2) << std::endl;
    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_GT( t3, t2 );
    CHK_EQ( b + 2, own_term_start );
    CHK_GT( s1.fNet->getNumPendingReqs(s2_addr), 0 );

    // `z` at `b + 3` reaches S4.
    append_msg(s1, "z");
    for (size_t ii = 0; ii < 5 && matched_on_s1(4) < b + 3; ++ii) {
        s1.fNet->execReqResp(s4_addr);
    }
    mm << "matched S4 " << matched_on_s1(4)
       << ", S4 last idx " << s4.raftServer->get_last_log_idx()
       << " term " << s4.raftServer->get_last_log_term()
       << ", S1 commit " << s1.raftServer->get_target_committed_log_idx()
       << std::endl;
    CHK_EQ( b + 3, matched_on_s1(4) );
    CHK_EQ( b + 3, s4.raftServer->get_last_log_idx() );
    CHK_EQ( t3, s4.raftServer->get_last_log_term() );

    // Only S1 and S4 hold `b + 2 .. b + 3`: 2 of 5, not committed.
    CHK_SM( s1.raftServer->get_target_committed_log_idx(), own_term_start );
    CHK_Z( wait_for_sm_exec({&s1}, COMMIT_TIMEOUT_SEC) );
    CHK_Z( s1.getTestSm()->isCommitted("z") );

    // S5 makes it 3 of 5.
    for (size_t ii = 0; ii < 5 && matched_on_s1(5) < b + 3; ++ii) {
        s1.fNet->execReqResp(s5_addr);
    }
    mm << "matched S5 " << matched_on_s1(5)
       << ", S1 commit " << s1.raftServer->get_target_committed_log_idx()
       << std::endl;
    CHK_EQ( b + 3, s1.raftServer->get_target_committed_log_idx() );
    CHK_Z( wait_for_sm_exec({&s1}, COMMIT_TIMEOUT_SEC) );
    CHK_GT( s1.getTestSm()->isCommitted("z"), 0 );

    print_stats(pkgs);

    for (RaftPkg* pp: pkgs) {
        fail_all_from(*pp);
    }
    for (RaftPkg* pp: pkgs) {
        pp->raftServer->shutdown();
    }

    f_base->destroy();

    return 0;
}

}  // namespace leader_election_test;
using namespace leader_election_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    // Disable reconnection timer for deterministic test.
    debugging_options::get_instance().disable_reconn_backoff_ = true;

    ts.doTest( "leader election basic test",
               leader_election_basic_test );

    ts.doTest( "vote log freshness after compaction test",
               vote_log_freshness_after_compaction_test );

    ts.doTest( "leader election priority test",
               leader_election_priority_test );

    ts.doTest( "leader election with aggressive node test",
               leader_election_with_aggressive_node_test );

    ts.doTest( "leader election with catching-up server test",
               leader_election_with_catching_up_server_test );

    ts.doTest( "leadership takeover basic test",
               leadership_takeover_basic_test );

    ts.doTest( "leadership takeover with designated successor test",
               leadership_takeover_designated_successor_test );

    ts.doTest( "leadership takeover by request test",
               leadership_takeover_by_request_test );

    ts.doTest( "learner not chosen as yield successor test",
               learner_not_chosen_as_yield_successor_test );

    ts.doTest( "learner designated as yield successor test",
               learner_designated_as_yield_successor_test );

    ts.doTest( "learner ignores leadership takeover test",
               learner_ignores_leadership_takeover_test );

    ts.doTest( "learner not chosen by leadership transfer test",
               learner_not_chosen_by_leadership_transfer_test );

    ts.doTest( "learner flipped during pre-vote test",
               learner_flipped_during_pre_vote_test );

    ts.doTest( "leadership takeover with offline candidate test",
               leadership_takeover_offline_candidate_test );

    ts.doTest( "temporary leader test",
               temporary_leader_test );

    ts.doTest( "new leader ignores earlier term match test",
               new_leader_ignores_earlier_term_match_test );

#ifdef ENABLE_RAFT_STATS
    _msg("raft stats: ENABLED\n");
#else
    _msg("raft stats: DISABLED\n");
#endif
    _msg("num allocs: %zu\n"
         "amount of allocs: %zu bytes\n"
         "num active buffers: %zu\n"
         "amount of active buffers: %zu bytes\n",
         raft_server::get_stat_counter("num_buffer_allocs"),
         raft_server::get_stat_counter("amount_buffer_allocs"),
         raft_server::get_stat_counter("num_active_buffers"),
         raft_server::get_stat_counter("amount_active_buffers"));

    return 0;
}

