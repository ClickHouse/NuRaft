/************************************************************************
Copyright 2017-2019 eBay Inc.
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

#include "event_awaiter.hxx"
#include "test_common.h"

#include <stdio.h>

using namespace nuraft;
using namespace raft_functional_common;

namespace failure_test {

int simple_conflict_test() {
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
    custom_params.snapshot_distance_ = 100;
    CHK_Z( launch_servers( pkgs, &custom_params ) );
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    const size_t NUM = 10;

    // Append messages asynchronously.
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        s1.raftServer->append_entries( {msg} );
    }

    for (size_t ii = 0; ii < NUM; ++ii) {
        s1.fNet->execReqResp();
    }
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Check if all messages are committed.
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        uint64_t idx = s1.getTestSm()->isCommitted(test_msg);
        CHK_GT(idx, 0);
    }

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    // Remember last log index before diverging.
    uint64_t idx_before_div = s1.getTestMgr()->load_log_store()->next_slot() - 1;

    // Append more messages to S1.
    const size_t MORE1 = 10;
    for (size_t ii=NUM; ii<NUM+MORE1; ++ii) {
        std::string test_msg = "more" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        s1.raftServer->append_entries( {msg} );
    }

    // Without replication of above messages,
    // initiate leader election.
    s2.dbgLog(" --- S2 will start leader election ---");
    s2.fTimer->invoke( timer_task_type::election_timer );
    s3.fTimer->invoke( timer_task_type::election_timer );
    // Send it to S3 only.
    s2.fNet->execReqResp( s3_addr );
    s2.fNet->execReqResp( s3_addr );
    s2.fNet->execReqResp( s3_addr );
    s2.fNet->execReqResp( s3_addr );
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    // Now S2 should be the new leader.
    s2.dbgLog(" --- Now S2 is leader ---");

    // Drop all messages of S2 and S3.
    s2.fNet->makeReqFailAll( s1_addr );
    s3.fNet->makeReqFailAll( s1_addr );
    s3.fNet->makeReqFailAll( s2_addr );

    // Append new (diverged) messages to S2 (new leader).
    s2.dbgLog(" --- Append diverged logs to S2 ---");
    const size_t MORE2 = 5;
    for (size_t ii=NUM; ii<NUM+MORE2; ++ii) {
        std::string test_msg = "diverged" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        s2.raftServer->append_entries( {msg} );
    }

    // S1's log index should be greater than S2's log index.
    uint64_t idx_after_div = s1.getTestMgr()->load_log_store()->next_slot() - 1;
    CHK_GT( idx_after_div,
            s2.getTestMgr()->load_log_store()->next_slot() - 1 );

    // S1 attempts to replicate messages.
    // It should be rejected.
    s1.fNet->execReqResp();

    // Now S2 replicate messages.
    // S1 has conflict, so that it should discard its local logs.
    s2.dbgLog(" --- S2 starts to replicate ---");
    for (size_t ii = 0; ii < 10; ++ii) {
        s2.fNet->execReqResp();
    }
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Check if all messages are committed.
    for (size_t ii=0; ii<NUM+MORE2; ++ii) {
        std::string test_msg;
        if (ii < NUM) {
            test_msg = "test" + std::to_string(ii);
        } else {
            test_msg = "diverged" + std::to_string(ii);
        }
        uint64_t idx = s2.getTestSm()->isCommitted(test_msg);
        CHK_GT(idx, 0);
    }

    // State machine should be identical.
    CHK_OK( s1.getTestSm()->isSame( *s2.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s2.getTestSm() ) );

    // Log store's last index should be identical.
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot(),
            s2.getTestMgr()->load_log_store()->next_slot() );
    CHK_EQ( s1.getTestMgr()->load_log_store()->next_slot(),
            s3.getTestMgr()->load_log_store()->next_slot() );

    // Rolled back indexes should be
    //   1) from `idx_before_div` (exclusive) to `idx_after_div` (inclusive),
    //   2) descending order, and
    //   3) consecutive.
    const std::list<uint64_t> r_idxs = s1.getTestSm()->getRollbackIdxs();
    CHK_EQ(idx_after_div - idx_before_div, r_idxs.size());
    for (uint64_t idx: r_idxs) {
        CHK_EQ(idx_after_div--, idx);
    }
    CHK_EQ(idx_before_div, idx_after_div);

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int rmv_not_resp_srv_wq_test(bool explicit_failure) {
    // * Remove server that is not responding.
    // * Can reach quorum.

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

    // Remove s3 from leader.
    s1.dbgLog(" --- remove ---");
    s1.raftServer->remove_srv( s3.getTestMgr()->get_srv_config()->get_id() );

    s1.fNet->execReqResp(s2_addr);
    // Fail to send it to S3.
    if (explicit_failure) {
        s1.fNet->makeReqFailAll(s3_addr);
    }

    // Heartbeat multiple times.
    for (size_t ii=0; ii<10; ++ii) {
        s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        s1.fNet->execReqResp(s2_addr);
        // Fail to send it to S3.
        if (explicit_failure) {
            s1.fNet->makeReqFailAll(s3_addr);
        }
    }

    // Wait for commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // For server 1 and 2, only 2 servers should exist.
    for (auto& entry: pkgs) {
        RaftPkg* pkg = entry;
        std::vector< ptr<srv_config> > configs;
        pkg->raftServer->get_srv_config_all(configs);

        if (pkg != &s3) {
            CHK_EQ(2, configs.size());
        }
    }

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

cb_func::ReturnCode ool_detect_cb(std::atomic<bool>* invoked,
                                  size_t purge_upto,
                                  cb_func::Type type,
                                  cb_func::Param* params)
{
    if (type == cb_func::Type::OutOfLogRangeWarning) {
        cb_func::OutOfLogRangeWarningArgs* ool_args =
            (cb_func::OutOfLogRangeWarningArgs*)params->ctx;
        auto chk_func = [purge_upto, ool_args]() -> int {
            CHK_EQ(purge_upto + 1, ool_args->startIdxOfLeader);
            return 0;
        };
        if (chk_func() == 0) {
            invoked->store(true);
        }
    } else {
        return cb_default(type, params);
    }

    return cb_func::ReturnCode::Ok;
}

int force_log_compaction_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    const size_t NUM_APPENDS = 10;
    const size_t PURGE_UPTO = 5;

    std::atomic<bool> invoked(false);
    for (size_t ii = 0; ii < pkgs.size(); ++ii) {
        RaftPkg* ff = pkgs[ii];
        raft_server::init_options opt(false, true, true);
        if (ii < 2) {
            opt.raft_callback_ = cb_default;
            ff->initServer(nullptr, opt);
        } else {
            // S3: set callback function to detect out of log range.
            opt.raft_callback_ = std::bind( ool_detect_cb,
                                            &invoked,
                                            PURGE_UPTO,
                                            std::placeholders::_1,
                                            std::placeholders::_2 );
            ff->initServer(nullptr, opt);
        }
        ff->fNet->listen(ff->raftServer);
        ff->fTimer->invoke( timer_task_type::election_timer );
    }
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        // Do not create snapshot.
        param.snapshot_distance_ = 0;
        pp->raftServer->update_params(param);
    }

    // Append messages asynchronously.
    for (size_t ii=0; ii<NUM_APPENDS; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        s1.raftServer->append_entries( {msg} );
    }

    // Send it to S2 only.
    s1.fNet->execReqResp(s2_addr);
    s1.fNet->execReqResp(s2_addr);
    s1.fNet->makeReqFailAll(s3_addr);

    // Wait for commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Force log compaction.
    s1.sMgr->load_log_store()->compact(PURGE_UPTO);

    // Trigger multiple heartbeats, it should be ok, without any crash.
    for (size_t ii = 0; ii < 5; ++ii) {
        if (ii) {
            TestSuite::sleep_ms(100);
        }
        s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        s1.fNet->execReqResp();
    }

    // Callback function should have been invoked.
    CHK_TRUE(invoked);

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int uncommitted_conf_new_leader_test() {
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
    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.max_append_size_ = 1;
        // Do not create snapshot.
        param.snapshot_distance_ = 0;
        pp->raftServer->update_params(param);
    }

    const size_t NUM_APPENDS_1 = 3;
    const size_t NUM_APPENDS_2 = 6;

    // Append 3 messages.
    for (size_t ii=0; ii<NUM_APPENDS_1; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        s1.raftServer->append_entries( {msg} );
    }
    // Replicate to all.
    for (size_t ii=0; ii<=NUM_APPENDS_1; ++ii) {
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
    }
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Append 3 more messages.
    for (size_t ii=NUM_APPENDS_1; ii<NUM_APPENDS_2; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        s1.raftServer->append_entries( {msg} );
    }
    // Replicate to S2 and S3 only.
    for (size_t ii=NUM_APPENDS_1; ii<=NUM_APPENDS_2; ++ii) {
        s1.fNet->execReqResp(s2_addr);
        s1.fNet->execReqResp(s3_addr);
    }
    // Make sure there is no message in-flight.
    for (size_t ii = 0; ii < 10; ++ii) {
        s1.fNet->execReqResp(s2_addr);
        s1.fNet->execReqResp(s3_addr);
    }

    // Now remove S2 (who was a member of the latest quorum).
    s1.raftServer->remove_srv(2);
    for (size_t ii = 0; ii < 10; ++ii) {
        s1.fNet->execReqResp(s2_addr);
    }
    // Send new update to S3 only.
    s1.dbgLog(" --- send config change (removing S2) to S3 ---");
    s1.fNet->execReqResp(s3_addr);

    // Invoke election timer of S4 and S5 (to make pre-vote of S3 succeed).
    s4.fTimer->invoke( timer_task_type::election_timer );
    s4.fNet->execReqResp();
    s5.fTimer->invoke( timer_task_type::election_timer );
    s5.fNet->execReqResp();

    // Now S3's vote should succeed.
    s3.fTimer->invoke( timer_task_type::election_timer );
    for (size_t ii = 0; ii < 10; ++ii) {
        s3.fNet->execReqResp();
    }
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    s3.fTimer->invoke( timer_task_type::heartbeat_timer );
    for (size_t ii = 0; ii < 10; ++ii) {
        s3.fNet->execReqResp();
    }
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Removing S2 should be in the latest config.
    ptr<cluster_config> c_config = s3.raftServer->get_config();
    CHK_NULL(c_config->get_server(2).get());

    print_stats( pkgs );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    s4.raftServer->shutdown();
    s5.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int removed_server_late_step_down_test() {
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

    // Remove s3 from leader.
    s1.raftServer->remove_srv( s3.getTestMgr()->get_srv_config()->get_id() );

    // Leave req/resp.
    s1.fNet->execReqResp();
    // Leave done, notify to peers, but except for S3.
    s1.fNet->execReqResp(s2_addr);
    // Notify new commit.
    s1.fNet->execReqResp(s2_addr);
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // S1 and S2: should see S1 and S2 only.
    // S3: should see everyone.
    for (auto& entry: pkgs) {
        RaftPkg* pkg = entry;
        std::vector< ptr<srv_config> > configs;
        pkg->raftServer->get_srv_config_all(configs);

        TestSuite::setInfo("id = %d", pkg->myId);
        if (pkg->myId != 3) {
            CHK_EQ(2, configs.size());
        } else {
            CHK_EQ(3, configs.size());
        }
    }

    // Removing server again should fail.
    ptr< cmd_result< ptr<buffer> > > ret =
        s1.raftServer->remove_srv( s3.getTestMgr()->get_srv_config()->get_id() );
    CHK_FALSE(ret->get_accepted());

    // More catch-up for to-be-removed server.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Now all servers should see S1 and S2 only.
    for (auto& entry: pkgs) {
        RaftPkg* pkg = entry;
        std::vector< ptr<srv_config> > configs;
        pkg->raftServer->get_srv_config_all(configs);

        TestSuite::setInfo("id = %d", pkg->myId);
        CHK_EQ(2, configs.size());
    }

    // Invoke election timer for S3, to make it step down.
    s3.fTimer->invoke( timer_task_type::election_timer );
    s3.fTimer->invoke( timer_task_type::election_timer );
    // Pending timer task should be zero in S3.
    CHK_Z( s3.fTimer->getNumPendingTasks() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int remove_server_on_pending_configs_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Make some dummy configs by setting user ctx.
    s1.raftServer->set_user_ctx("a");
    s1.raftServer->set_user_ctx("aa");

    // Without commit & replication of above configs,
    // remove S2.
    s1.raftServer->remove_srv(2);

    // Make failure.
    s1.fNet->makeReqFailAll(s2_addr);
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->makeReqFailAll(s2_addr);

    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Adding server should succeed without error about duplicate ID.
    ptr< cmd_result< ptr<buffer> > > ret =
        s1.raftServer->add_srv( *s2.getTestMgr()->get_srv_config() );
    CHK_Z( ret->get_result_code() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

// Raft Figure 8: S1 leads term `t3` over a 101-entry tail of term `t1` that S3 (term
// T2) does not hold, and S2 has stored S1's first batch, but S1 has not seen the
// acknowledgement yet. Never fail S1's requests to S2 after this: it would drop it.
static int setup_held_figure8_ack(RaftPkg& s1, RaftPkg& s2, RaftPkg& s3,
                                  uint64_t& b, uint64_t& t1, uint64_t& t3,
                                  uint64_t& own_term_start) {
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};
    const std::string& s1_addr = s1.myEndpoint;
    const std::string& s2_addr = s2.myEndpoint;
    const std::string& s3_addr = s3.myEndpoint;

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

    // Committed base, on every server.
    for (size_t ii = 0; ii < 5; ++ii) {
        append_msg(s1, "base" + std::to_string(ii));
    }
    b = s1.raftServer->get_last_log_idx();
    for (size_t ii = 0; ii < 5; ++ii) {
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
        if ( s2.raftServer->get_committed_log_idx() == b &&
             s3.raftServer->get_committed_log_idx() == b ) break;
    }
    t1 = s1.raftServer->get_term();
    TestSuite::Msg mm;
    mm << "base: last idx " << b << ", term " << t1 << std::endl;
    CHK_EQ( b, s1.raftServer->get_committed_log_idx() );
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    // S1 appends more than one batch in term T1, replicated to nobody.
    const size_t NUM_OLD = 101;
    for (size_t ii = 0; ii < NUM_OLD; ++ii) {
        append_msg(s1, "old" + std::to_string(ii));
    }
    s1.fNet->makeReqFailAll(s2_addr);
    s1.fNet->makeReqFailAll(s3_addr);
    mm << "S1 last idx " << s1.raftServer->get_last_log_idx()
       << ", S2 " << s2.raftServer->get_last_log_idx()
       << ", S3 " << s3.raftServer->get_last_log_idx()
       << ", S1 commit " << s1.raftServer->get_target_committed_log_idx()
       << std::endl;
    CHK_EQ( b + NUM_OLD, s1.raftServer->get_last_log_idx() );
    CHK_EQ( b, s2.raftServer->get_last_log_idx() );
    CHK_EQ( b, s3.raftServer->get_last_log_idx() );
    CHK_EQ( b, s1.raftServer->get_target_committed_log_idx() );

    // Term T2: S3 is elected with S2's vote, and its first entry stays on S3.
    s2.fTimer->invoke( timer_task_type::election_timer );
    s2.fNet->makeReqFailAll(s1_addr);
    s2.fNet->makeReqFailAll(s3_addr);
    s3.fTimer->invoke( timer_task_type::election_timer );
    // Pre-vote, S1 then S2.
    s3.fNet->execReqResp(s1_addr);
    s3.fNet->execReqResp(s2_addr);
    // Vote: S1 denies (its log is longer) but moves to T2, S2 grants.
    s3.fNet->execReqResp(s1_addr);
    s3.fNet->execReqResp(s2_addr);
    CHK_TRUE( s3.raftServer->is_leader() );
    s3.fNet->makeReqFailAll(s1_addr);
    s3.fNet->makeReqFailAll(s2_addr);

    uint64_t t2 = s3.raftServer->get_term();
    mm << "T2 " << t2 << ", S3 last idx " << s3.raftServer->get_last_log_idx()
       << " term " << s3.getTestMgr()->get_inmem_log_store()->term_at(b + 1)
       << ", S2 last idx " << s2.raftServer->get_last_log_idx()
       << ", S1 last idx " << s1.raftServer->get_last_log_idx() << std::endl;
    CHK_GT( t2, t1 );
    CHK_EQ( b + 1, s3.raftServer->get_last_log_idx() );
    CHK_EQ( t2, s3.getTestMgr()->get_inmem_log_store()->term_at(b + 1) );
    CHK_EQ( b, s2.raftServer->get_last_log_idx() );
    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_EQ( b + NUM_OLD, s1.raftServer->get_last_log_idx() );

    // Term T3: S1 is elected with S2's vote.
    s1.fTimer->invoke( timer_task_type::election_timer );
    // Pre-vote and vote, S2 only.
    s1.fNet->execReqResp(s2_addr);
    s1.fNet->execReqResp(s2_addr);
    s1.fNet->makeReqFailAll(s3_addr);

    t3 = s1.raftServer->get_term();
    own_term_start = s1.raftServer->get_log_idx_at_becoming_leader();
    mm << "T3 " << t3 << ", S1 leader " << s1.raftServer->is_leader()
       << ", first log of T3 " << own_term_start
       << ", matched S2 " << matched_on_s1(2) << std::endl;
    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_GT( t3, t2 );
    CHK_EQ( b + NUM_OLD + 1, own_term_start );

    // S2 rejects S1's first request, and S2 stores the next one, a full batch.
    s1.fNet->execReqResp(s2_addr);
    ptr<req_msg> batch = s1.fNet->getFirstPendingReq(s2_addr);
    CHK_NONNULL( batch.get() );
    mm << "batch to S2: prev idx " << batch->get_last_log_idx()
       << ", entries " << batch->log_entries().size() << std::endl;
    CHK_EQ( b, batch->get_last_log_idx() );
    CHK_EQ( 100, batch->log_entries().size() );
    CHK_TRUE( s1.fNet->delieverReqTo(s2_addr) );

    mm << "S2 last idx " << s2.raftServer->get_last_log_idx()
       << " term " << s2.raftServer->get_last_log_term()
       << ", pending acks " << s1.fNet->getNumPendingResps(s2_addr)
       << ", matched S2 " << matched_on_s1(2)
       << ", S1 commit " << s1.raftServer->get_target_committed_log_idx()
       << std::endl;
    CHK_EQ( b + 100, s2.raftServer->get_last_log_idx() );
    CHK_EQ( t1, s2.raftServer->get_last_log_term() );
    CHK_EQ( 1, s1.fNet->getNumPendingResps(s2_addr) );
    CHK_SM( matched_on_s1(2), b + 100 );
    CHK_EQ( b, s1.raftServer->get_target_committed_log_idx() );
    return 0;
}

static void fail_all_requests(const std::vector<RaftPkg*>& pkgs) {
    for (RaftPkg* src: pkgs) {
        for (RaftPkg* dst: pkgs) {
            if (src != dst) src->fNet->makeReqFailAll(dst->myEndpoint);
        }
    }
}

int deposed_leader_ignores_append_entries_response_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    uint64_t b = 0, t1 = 0, t3 = 0, own_term_start = 0;
    CHK_Z( setup_held_figure8_ack(s1, s2, s3, b, t1, t3, own_term_start) );

    // S1 resigns in T3, as on leadership expiry, then gets S2's acknowledgement.
    s1.raftServer->yield_leadership(true);
    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_EQ( t3, s1.raftServer->get_term() );
    CHK_TRUE( s1.fNet->handleRespFrom(s2.myEndpoint) );

    TestSuite::Msg mm;
    mm << "S1 term " << s1.raftServer->get_term()
       << ", matched S2 "
       << s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), 2)
       << ", S1 commit " << s1.raftServer->get_target_committed_log_idx()
       << std::endl;
    // `b + 1` onwards are of T1, and S3 can still overwrite them with S2's vote.
    CHK_EQ( b, s1.raftServer->get_target_committed_log_idx() );
    TestSuite::sleep_ms(500);
    CHK_Z( s1.getTestSm()->isCommitted("old0") );

    print_stats(pkgs);

    fail_all_requests(pkgs);
    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int leader_ignores_append_entries_response_of_earlier_term_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};
    const std::string& s1_addr = s1.myEndpoint;
    const std::string& s2_addr = s2.myEndpoint;
    const std::string& s3_addr = s3.myEndpoint;

    uint64_t b = 0, t1 = 0, t3 = 0, own_term_start = 0;
    CHK_Z( setup_held_figure8_ack(s1, s2, s3, b, t1, t3, own_term_start) );

    auto matched_on_s1 = [&](int peer_id) {
        return s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), peer_id);
    };
    TestSuite::Msg mm;

    // Term TA: S3 is elected with S2's vote, as S3's last log term T2 beats S2's T1.
    s2.fTimer->invoke( timer_task_type::election_timer );
    // S2's pre-vote, then its vote, to S3: S3 moves to S2's term and denies.
    for (size_t ii = 0; ii < 3; ++ii) {
        s2.fNet->execReqResp(s3_addr);
    }
    s2.fNet->makeReqFailAll(s1_addr);
    s2.fNet->makeReqFailAll(s3_addr);
    mm << "after S2's campaign: S2 term " << s2.raftServer->get_term()
       << " leader " << s2.raftServer->is_leader()
       << ", S3 term " << s3.raftServer->get_term()
       << " leader " << s3.raftServer->is_leader()
       << ", S1 term " << s1.raftServer->get_term() << std::endl;
    CHK_FALSE( s2.raftServer->is_leader() );
    CHK_FALSE( s3.raftServer->is_leader() );

    s3.fTimer->invoke( timer_task_type::election_timer );
    // Pre-vote, then vote, to S2 only.
    for (size_t ii = 0; ii < 5 && !s3.raftServer->is_leader(); ++ii) {
        s3.fNet->execReqResp(s2_addr);
    }
    s3.fNet->makeReqFailAll(s2_addr);
    uint64_t ta = s3.raftServer->get_term();
    mm << "TA " << ta << ", S3 leader " << s3.raftServer->is_leader()
       << ", S3 last idx " << s3.raftServer->get_last_log_idx() << std::endl;
    CHK_TRUE( s3.raftServer->is_leader() );
    CHK_GT( ta, t3 );
    CHK_EQ( b + 2, s3.raftServer->get_last_log_idx() );

    // S3 replicates to S1 only: S1 rolls back `b + 1 ..` and takes S3's log.
    for ( size_t ii = 0;
          ii < 10 && s1.raftServer->get_target_committed_log_idx() < b + 2;
          ++ii ) {
        if (!s3.fNet->getNumPendingReqs(s1_addr)) {
            s3.fTimer->invoke( timer_task_type::heartbeat_timer );
        }
        s3.fNet->execReqResp(s1_addr);
        s3.fNet->makeReqFailAll(s2_addr);
    }
    mm << "S1 term " << s1.raftServer->get_term()
       << " leader " << s1.raftServer->is_leader()
       << ", S1 last idx " << s1.raftServer->get_last_log_idx()
       << " term " << s1.raftServer->get_last_log_term()
       << ", S1 commit " << s1.raftServer->get_target_committed_log_idx()
       << ", S3 commit " << s3.raftServer->get_target_committed_log_idx()
       << ", pending acks from S2 " << s1.fNet->getNumPendingResps(s2_addr)
       << std::endl;
    CHK_FALSE( s1.raftServer->is_leader() );
    CHK_EQ( ta, s1.raftServer->get_term() );
    CHK_EQ( b + 2, s1.raftServer->get_last_log_idx() );
    CHK_EQ( ta, s1.raftServer->get_last_log_term() );
    CHK_EQ( b + 2, s3.raftServer->get_target_committed_log_idx() );
    CHK_EQ( 1, s1.fNet->getNumPendingResps(s2_addr) );

    // S1's takeover vote must reach S3 on a free, live client.
    s1.fNet->makeReqFailAll(s3_addr);
    CHK_TRUE( s1.fNet->forceRecreatePeerRpc(s1.raftServer.get(), 3) );

    // Term TB: S3 hands the leadership to S1, which S1 takes without a pre-vote.
    s3.raftServer->yield_leadership(false, 1);
    for (size_t ii = 0; ii < 5 && s3.raftServer->is_leader(); ++ii) {
        if (!s3.fNet->getNumPendingReqs(s1_addr)) {
            s3.fTimer->invoke( timer_task_type::heartbeat_timer );
        }
        s3.fNet->execReqResp(s1_addr);
        s3.fNet->makeReqFailAll(s2_addr);
    }
    // The takeover request.
    s3.fNet->execReqResp(s1_addr);
    // S1's vote, S3 only: S1's peer S2 is busy with the held acknowledgement.
    s1.fNet->execReqResp(s3_addr);
    uint64_t tb = s1.raftServer->get_term();
    uint64_t l2 = s1.raftServer->get_log_idx_at_becoming_leader();
    mm << "TB " << tb << ", S1 leader " << s1.raftServer->is_leader()
       << ", first log of TB " << l2
       << ", S1 commit " << s1.raftServer->get_target_committed_log_idx()
       << ", pending acks from S2 " << s1.fNet->getNumPendingResps(s2_addr)
       << std::endl;
    CHK_TRUE( s1.raftServer->is_leader() );
    CHK_GT( tb, ta );
    CHK_EQ( b + 3, l2 );
    CHK_EQ( 1, s1.fNet->getNumPendingResps(s2_addr) );

    // S2's acknowledgement of S1's T3 batch: `b + 4 ..` do not exist in S1's log now.
    mm << "matched S2 " << matched_on_s1(2) << ", next S2 "
       << s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 2)
       << ", matched S3 " << matched_on_s1(3) << std::endl;
    CHK_TRUE( s1.fNet->handleRespFrom(s2_addr) );
    mm << "after the acknowledgement: matched S2 " << matched_on_s1(2)
       << ", next S2 " << s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 2)
       << ", S1 commit " << s1.raftServer->get_target_committed_log_idx()
       << std::endl;
    // `l2` is on S1 only.
    CHK_SM( s1.raftServer->get_target_committed_log_idx(), l2 );

    // S3 and then S2 catch up with S1's term.
    for (size_t ii = 0; ii < 5 && matched_on_s1(3) < l2; ++ii) {
        if (!s1.fNet->getNumPendingReqs(s3_addr)) {
            s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        }
        s1.fNet->execReqResp(s3_addr);
    }
    mm << "matched S3 " << matched_on_s1(3)
       << ", S1 commit " << s1.raftServer->get_target_committed_log_idx()
       << std::endl;
    CHK_EQ( l2, s1.raftServer->get_target_committed_log_idx() );
    for ( size_t ii = 0;
          ii < 10 && s2.raftServer->get_target_committed_log_idx() < l2;
          ++ii ) {
        if (!s1.fNet->getNumPendingReqs(s2_addr)) {
            s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        }
        s1.fNet->execReqResp(s2_addr);
    }
    mm << "S2 last idx " << s2.raftServer->get_last_log_idx()
       << ", S2 commit " << s2.raftServer->get_target_committed_log_idx()
       << std::endl;
    CHK_EQ( l2, s2.raftServer->get_target_committed_log_idx() );
    CHK_Z( wait_for_sm_exec({&s1, &s2}, COMMIT_TIMEOUT_SEC) );
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );

    print_stats(pkgs);

    fail_all_requests(pkgs);
    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int log_append_completion_after_step_down_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};
    const std::string& s2_addr = s2.myEndpoint;

    // Holds S1's `lock_` in a heartbeat until S1's log store has reported the
    // durable entry, so that the report waits for the lock while S1 resigns.
    std::atomic<bool> armed(false);
    std::atomic<bool> hb_ran(false);
    std::atomic<bool> hb_saw_leader(false);
    std::atomic<uint64_t> hb_durable_before(0);
    std::atomic<uint64_t> hb_durable_after(0);
    auto callback = [&](cb_func::Type type, cb_func::Param* param) {
        if ( type == cb_func::HeartBeat && param && param->myId == 1 &&
             armed.exchange(false) ) {
            ptr<log_store> ls = s1.getTestMgr()->load_log_store();
            hb_ran = true;
            hb_saw_leader = s1.raftServer->is_leader();
            hb_durable_before = ls->last_durable_index();
            uint64_t target = s1.raftServer->get_last_log_idx();
            for ( size_t ii = 0;
                  ii < 500 && ls->last_durable_index() < target;
                  ++ii ) {
                TestSuite::sleep_ms(10);
            }
            hb_durable_after = ls->last_durable_index();
            TestSuite::sleep_ms(300);
        }
        return cb_default(type, param);
    };

    raft_params custom_params;
    custom_params.election_timeout_lower_bound_ = 0;
    custom_params.election_timeout_upper_bound_ = 1000;
    custom_params.heart_beat_interval_ = 500;
    custom_params.snapshot_distance_ = 100;
    CHK_Z( launch_servers( pkgs, &custom_params, false, callback ) );
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    auto append_msg = [](RaftPkg& pkg, const std::string& str) {
        ptr<buffer> msg = buffer::alloc(str.size() + 1);
        msg->put(str);
        pkg.raftServer->append_entries( {msg} );
    };
    for (size_t ii = 0; ii < 5; ++ii) {
        append_msg(s1, "base" + std::to_string(ii));
    }
    uint64_t b = s1.raftServer->get_last_log_idx();
    for (size_t ii = 0; ii < 5; ++ii) {
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
        if ( s2.raftServer->get_committed_log_idx() == b &&
             s3.raftServer->get_committed_log_idx() == b ) break;
    }
    CHK_EQ( b, s1.raftServer->get_committed_log_idx() );

    // S1's own entries become durable 2 seconds after they are appended.
    {
        raft_params param = s1.raftServer->get_current_params();
        param.parallel_log_appending_ = true;
        s1.raftServer->update_params(param);
    }
    ptr<log_store> s1_ls = s1.getTestMgr()->load_log_store();
    s1.getTestMgr()->set_disk_delay(s1.raftServer.get(), 2000);
    s1_ls->flush();
    CHK_EQ( b, s1_ls->last_durable_index() );

    // S2 holds `b + 1`, S1 does not have it on disk yet.
    append_msg(s1, "x");
    for ( size_t ii = 0;
          ii < 5 && s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), 2) < b + 1;
          ++ii ) {
        s1.fNet->execReqResp(s2_addr);
    }
    TestSuite::Msg mm;
    mm << "b " << b << ", matched S2 "
       << s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), 2)
       << ", S1 durable " << s1_ls->last_durable_index()
       << ", S1 commit " << s1.raftServer->get_target_committed_log_idx()
       << std::endl;
    CHK_EQ( b + 1, s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), 2) );
    CHK_EQ( b, s1_ls->last_durable_index() );
    CHK_EQ( b, s1.raftServer->get_target_committed_log_idx() );

    // The heartbeat resigns: S2 and S3 have not responded within the expiry.
    {
        raft_params param = s1.raftServer->get_current_params();
        param.leadership_expiry_ = 100;
        s1.raftServer->update_params(param);
    }
    armed = true;
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    mm << "heartbeat: ran " << hb_ran << ", saw leader " << hb_saw_leader
       << ", durable " << hb_durable_before << " -> " << hb_durable_after
       << ", S1 leader " << s1.raftServer->is_leader() << std::endl;
    CHK_TRUE( hb_ran );
    CHK_TRUE( hb_saw_leader );
    CHK_EQ( b, hb_durable_before.load() );
    CHK_EQ( b + 1, hb_durable_after.load() );
    CHK_FALSE( s1.raftServer->is_leader() );

    TestSuite::sleep_ms(500);
    mm << "S1 commit " << s1.raftServer->get_target_committed_log_idx() << std::endl;
    CHK_EQ( b, s1.raftServer->get_target_committed_log_idx() );

    print_stats(pkgs);

    fail_all_requests(pkgs);
    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

}  // namespace failure_test;
using namespace failure_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    // Disable reconnection timer for deterministic test.
    debugging_options::get_instance().disable_reconn_backoff_ = true;

    ts.doTest( "simple conflict test",
               simple_conflict_test );

    ts.doTest( "remove not responding server with quorum test",
               rmv_not_resp_srv_wq_test,
               TestRange<bool>({false, true}) );

    ts.doTest( "force log compaction test",
               force_log_compaction_test );

    ts.doTest( "uncommitted config for new leader test",
               uncommitted_conf_new_leader_test );

    ts.doTest( "removed server late step down test",
               removed_server_late_step_down_test );

    ts.doTest( "remove server on pending configs test",
               remove_server_on_pending_configs_test );

    ts.doTest( "deposed leader ignores append entries response test",
               deposed_leader_ignores_append_entries_response_test );

    ts.doTest( "leader ignores append entries response of earlier term test",
               leader_ignores_append_entries_response_of_earlier_term_test );

    ts.doTest( "log append completion after step down test",
               log_append_completion_after_step_down_test );

    return 0;
}

