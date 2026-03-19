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

#include "event_awaiter.hxx"
#include "raft_params.hxx"
#include "test_common.h"

#include <stdio.h>

using namespace nuraft;
using namespace raft_functional_common;

using raft_result = cmd_result< ptr<buffer> >;

namespace snapshot_test {

int snapshot_basic_test() {
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

    // Append a message using separate thread.
    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (size_t ii=0; ii<5; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        exec_args.setMsg(msg);
        exec_args.eaExecuter.invoke();

        // Wait for executer thread.
        TestSuite::sleep_ms(EXECUTOR_WAIT_MS);

        CHK_NULL( exec_args.getMsg().get() );

        // NOTE: Send it to S2 only, S3 will be lagging behind.
        s1.fNet->execReqResp("S2"); // replication.
        s1.fNet->execReqResp("S2"); // commit.
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.
    }
    // Make req to S3 failed.
    s1.fNet->makeReqFail("S3");

    // Trigger heartbeat to S3, it will initiate snapshot transmission.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->execReqResp();

    // Send the entire snapshot.
    do {
        s1.fNet->execReqResp();
    } while (s3.raftServer->is_receiving_snapshot());

    s1.fNet->execReqResp(); // commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    // There shouldn't be any open snapshot ctx.
    CHK_Z( s1.getTestSm()->getNumOpenedUserCtxs() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();

    return 0;
}

int snapshot_new_member_restart_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};
    std::vector<RaftPkg*> pkgs_orig = {&s1, &s2};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs_orig ) );

    // Append a message using separate thread.
    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (size_t ii=0; ii<5; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        exec_args.setMsg(msg);
        exec_args.eaExecuter.invoke();

        // Wait for executer thread.
        TestSuite::sleep_ms(EXECUTOR_WAIT_MS);

        CHK_NULL( exec_args.getMsg().get() );

        s1.fNet->execReqResp(); // replication.
        s1.fNet->execReqResp(); // commit.
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.
    }

    // Add S3 to S1.
    s1.raftServer->add_srv(*(s3.getTestMgr()->get_srv_config()));

    // Join req/resp.
    s1.fNet->execReqResp();
    // Add new server, notify existing peers.
    // After getting response, it will make configuration commit.
    s1.fNet->execReqResp();
    // Notify new commit, start snapshot transmission.
    s1.fNet->execReqResp();
    // Wait for bg commit for configuration change.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Shutdown S3.
    s1.dbgLog(" --- shutting down S3 ---");
    s3.raftServer->shutdown();
    s3.fNet->shutdown();
    s1.dbgLog(" --- shut down S3 ---");

    // Trigger heartbeat, to close connection.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->execReqResp();

    // Restart s3.
    s1.dbgLog(" --- restarting S3 ---");
    CHK_Z( launch_servers( {&s3},
                           /* custom_params = */ nullptr,
                           /* restart = */ true ) );
    s1.dbgLog(" --- restarted S3 ---");

    // Trigger heartbeat, to resume snapshot transmission.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();

    // Send the entire snapshot.
    do {
        s1.fNet->execReqResp();
    } while (s3.raftServer->is_receiving_snapshot());

    // commit.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    // One more heartbeat.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();

    return 0;
}

int snapshot_manual_creation_test() {
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

    // Append a message using separate thread.
    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 100;
        pp->raftServer->update_params(param);
    }

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

    // NOTE: Send it to S2 only, S3 will be lagging behind.
    s1.fNet->execReqResp("S2"); // replication.
    s1.fNet->execReqResp("S2"); // commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    // One more time to make sure.
    s1.fNet->execReqResp("S2");
    s1.fNet->execReqResp("S2");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Remember the current commit index.
    uint64_t committed_index = s1.raftServer->get_committed_log_idx();

    // Create a manual snapshot.
    ulong log_idx = s1.raftServer->create_snapshot();
    CHK_EQ( committed_index, log_idx );
    CHK_EQ( log_idx, s1.raftServer->get_last_snapshot_idx() );

    // Make req to S3 failed.
    s1.fNet->makeReqFail("S3");

    // Trigger heartbeat to S3, it will initiate snapshot transmission.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->execReqResp();

    // Send the entire snapshot.
    do {
        s1.fNet->execReqResp();
    } while (s3.raftServer->is_receiving_snapshot());

    s1.fNet->execReqResp(); // commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    CHK_EQ( committed_index, s3.getTestSm()->last_snapshot()->get_last_log_idx() );

    // There shouldn't be any open snapshot ctx.
    CHK_Z( s1.getTestSm()->getNumOpenedUserCtxs() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();

    return 0;
}

int snapshot_creation_index_inversion_test() {
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

    // Append a message using separate thread.
    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    const size_t NUM = 5;

    // Set a callback function to manually create snapshot,
    // right before the automatic snapshot creation.
    bool manual_snp_creation_succ = false;
    s1.ctx->set_cb_func([&](cb_func::Type t, cb_func::Param* p) -> cb_func::ReturnCode {
        // At the beginning of an automatic snapshot creation,
        // create a manual snapshot, to mimic index inversion.
        if (t != cb_func::Type::SnapshotCreationBegin) {
            return cb_default(t, p);
        }

        // This function should be invoked only once, to avoid
        // infinite recursive call.
        static bool invoked = false;
        if (!invoked) {
            invoked = true;
            ulong log_idx = s1.raftServer->create_snapshot();
            manual_snp_creation_succ = (log_idx > 0);
        }
        return cb_func::ReturnCode::Ok;
    });

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

    // NOTE: Send it to S2 only, S3 will be lagging behind.
    s1.fNet->execReqResp("S2"); // replication.
    s1.fNet->execReqResp("S2"); // commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    // One more time to make sure.
    s1.fNet->execReqResp("S2");
    s1.fNet->execReqResp("S2");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Snapshot creation should have happened only once, by manual creation.
    CHK_TRUE(manual_snp_creation_succ);
    CHK_EQ(1, s1.getTestSm()->getNumSnapshotCreations());

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();

    return 0;
}

int snapshot_scheduled_creation_test() {
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

    // Append a message using separate thread.
    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

    const size_t NUM = 5;

    // Append messages asynchronously.
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii = 0; ii < NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );

        CHK_TRUE( ret->get_accepted() );

        handlers.push_back(ret);
    }

    s1.fNet->execReqResp(); // replication.
    s1.fNet->execReqResp(); // commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    // One more time to make sure.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Manually create a snapshot.
    uint64_t log_idx = s1.raftServer->create_snapshot();
    CHK_GT(log_idx, 0);

    // Schedule snapshot creation and wait 500ms, there shouldn't be any progress.
    auto sched_ret = s1.raftServer->schedule_snapshot_creation();
    TestSuite::sleep_ms(500, "wait for async snapshot creation");
    CHK_FALSE(sched_ret->has_result());

    uint64_t last_idx = s1.raftServer->get_last_log_idx();

    // Append more messages asynchronously.
    for (size_t ii = NUM; ii < NUM * 2; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );

        CHK_TRUE( ret->get_accepted() );

        handlers.push_back(ret);
    }

    s1.fNet->execReqResp(); // replication.
    s1.fNet->execReqResp(); // commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    // One more time to make sure.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Now it should have the result.
    CHK_TRUE(sched_ret->has_result());
    CHK_EQ(last_idx + 1, sched_ret->get());

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();

    return 0;
}

int snapshot_randomized_creation_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    const size_t NUM = 50;

    raft_params params;
    params.with_randomized_snapshot_creation_enabled(true);
    params.with_election_timeout_lower(0);
    params.with_election_timeout_upper(10000);
    params.with_hb_interval(5000);
    params.with_client_req_timeout(1000000);
    params.with_reserved_log_items(0);
    params.with_snapshot_enabled(NUM);
    params.with_log_sync_stopping_gap(1);

    CHK_Z( launch_servers( pkgs, &params ) );
    CHK_Z( make_group( pkgs ) );

    // Append a message using separate thread.
    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        pp->raftServer->update_params(param);
    }

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

    // NOTE: Send it to S2, S3
    s1.fNet->execReqResp(); // replication.
    s1.fNet->execReqResp(); // commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    // One more time to make sure.
    s1.fNet->execReqResp(); // replication.
    s1.fNet->execReqResp(); // commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    CHK_NEQ(NUM, s1.getTestSm()->last_snapshot()->get_last_log_idx())
    CHK_NEQ(NUM, s2.getTestSm()->last_snapshot()->get_last_log_idx())
    CHK_NEQ(NUM, s3.getTestSm()->last_snapshot()->get_last_log_idx())

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();

    return 0;
}

int snapshot_close_for_removed_peer_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        // Set quorum to 1 so as to make S1 commits data locally.
        param.custom_commit_quorum_size_ = 1;
        param.custom_election_quorum_size_ = 1;
        pp->raftServer->update_params(param);
    }

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
    // Wait for bg commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Make req to S2 failed.
    s1.fNet->makeReqFailAll("S2");

    // Heartbeat, this will initiate snapshot transfer to S2.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->execReqResp("S2");

    // Now remove S2.
    s1.raftServer->remove_srv(2);

    // Heartbeat, and make request fail.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->makeReqFailAll("S2");

    // After S2 is removed, the snapshot ctx should be destroyed.
    CHK_Z( s1.getTestSm()->getNumOpenedUserCtxs() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int snapshot_leader_switch_test() {
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

    // Append a message using separate thread.
    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 5;
        if (pp->raftServer->get_id() == 2) {
            // S2: reserve more logs.
            param.reserved_log_items_ = 100;
        }
        pp->raftServer->update_params(param);
    }

    const size_t NUM = 10;

    // Append messages asynchronously.
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii = 0; ii < NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );

        CHK_TRUE( ret->get_accepted() );

        handlers.push_back(ret);
    }

    // NOTE: Send it to S2 only, S3 will be lagging behind.
    s1.fNet->execReqResp("S2"); // replication.
    s1.fNet->execReqResp("S2"); // commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    // One more time to make sure.
    s1.fNet->execReqResp("S2");
    s1.fNet->execReqResp("S2");
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // Make req to S3 failed.
    s1.fNet->makeReqFail("S3");

    // Trigger heartbeat to S3, it will initiate snapshot transmission.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    // Send a couple of messages.
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();

    // Remember the last log index of S3.
    uint64_t last_log_idx = s3.raftServer->get_last_log_idx();

    // Leader switch from S1 to S2.
    s2.raftServer->request_leadership();
    s2.fNet->execReqResp();

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

    // Send heartbeat twice.
    for (size_t ii = 0; ii < 2; ++ii) {
        s2.fTimer->invoke( timer_task_type::heartbeat_timer );
        s2.fNet->execReqResp();
        s2.fNet->execReqResp();
    }

    // S3 was in the middle of receiving snapshot, should reject the normal
    // append_entries request. That means, the last log index should remain the same.
    // Instead, S2 should re-initiate snapshot transmission.
    CHK_EQ( last_log_idx, s3.raftServer->get_last_log_idx() );

    // S3 should be in receiving snapshot state.
    CHK_TRUE( s3.raftServer->is_receiving_snapshot() );

    // Make req to S3 failed, and invoke heartbeat.
    // This will re-check the snapshot condition,
    // and should resume the previous snapshot transmission.
    s2.fNet->makeReqFail("S3");
    s2.fTimer->invoke( timer_task_type::heartbeat_timer );

    // Send the entire snapshot.
    do {
        s2.fNet->execReqResp();
    } while (s3.raftServer->is_receiving_snapshot());

    s2.fNet->execReqResp(); // Rest of logs and commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    // There shouldn't be any open snapshot ctx.
    CHK_Z( s2.getTestSm()->getNumOpenedUserCtxs() );

    // Append one more log.
    for (size_t ii = NUM; ii < NUM + 1; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s2.raftServer->append_entries( {msg} );

        CHK_TRUE( ret->get_accepted() );

        handlers.push_back(ret);
    }
    s2.fNet->execReqResp(); // replication.
    s2.fNet->execReqResp(); // commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) ); // commit execution.

    // State machine should be identical.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();

    return 0;
}

// Regression test for incident #1479: stale snapshot_sync_is_needed flag
// when peer has advanced past the leader's snapshot.
//
// Production scenario: TOCTOU race between KeeperStateMachine's
// latest_snapshot_meta and NuRaft's last_snapshot_ causes
// is_snapshot_sync_needed to remain true while the peer's log
// has advanced past the snapshot. The old code called system_exit
// (abort); the fix clears the flag and falls through to normal
// replication or OOL handling.
//
// This test uses the raft_server_handler test helper to directly
// set the flag, since the TOCTOU race cannot be reproduced
// deterministically in the fake network framework.
int snapshot_stale_sync_flag_test() {
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

    // Append a message using separate thread.
    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 5;
        pp->raftServer->update_params(param);
    }

    const size_t NUM = 10;

    // Append messages asynchronously.
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii = 0; ii < NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );

        CHK_TRUE( ret->get_accepted() );
        handlers.push_back(ret);
    }

    // Replicate to both S2 and S3.
    for (int ii = 0; ii < 4; ++ii) {
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    }

    // All servers should be in sync now with snapshot at ~index 10.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    uint64_t s3_log_idx = s3.raftServer->get_last_log_idx();

    // Leader must have a snapshot (snapshot_distance_=5, 10 entries appended).
    // This ensures we exercise the "peer caught up past snapshot" path,
    // not the "no snapshot exists" fallback.
    ptr<snapshot> leader_snp =
        s1.fNet->getLastSnapshot(s1.raftServer.get());
    CHK_NONNULL( leader_snp.get() );
    CHK_GTEQ( s3_log_idx, leader_snp->get_last_log_idx() );

    // --- Simulate the TOCTOU race condition ---
    // Force-set is_snapshot_sync_needed on the leader's peer for S3.
    // In production, this flag gets set via RECEIVING_SNAPSHOT response,
    // and becomes stale when the peer advances past the snapshot due to
    // election churn or concurrent snapshot installs.
    s1.fNet->setPeerSnapshotSyncNeeded(s1.raftServer.get(), 3, true);
    CHK_TRUE( s1.fNet->getPeerSnapshotSyncNeeded(
                  s1.raftServer.get(), 3) );

    // Also create a snapshot sync context on the peer, simulating a
    // partial snapshot transfer that was interrupted. Without this,
    // the hasPeerSnapshotSyncCtx assertion below would be vacuous
    // (checking nullptr == nullptr).
    s1.fNet->setPeerSnapshotInSync(
        s1.raftServer.get(), 3, leader_snp);
    CHK_TRUE( s1.fNet->hasPeerSnapshotSyncCtx(
                  s1.raftServer.get(), 3) );

    // Trigger heartbeat. With the old code, create_append_entries_req
    // would enter create_sync_snapshot_req and call system_exit (abort)
    // because peer's last_log_idx >= snapshot's last_log_idx.
    // With the fix, the early check at the top of create_append_entries_req
    // detects that entries_valid is true and the peer has caught up past
    // the snapshot, clears both the flag and context, and proceeds with
    // normal log replication.
    s1.fTimer->invoke( timer_task_type::heartbeat_timer );
    s1.fNet->execReqResp();
    s1.fNet->execReqResp();

    // The flag should now be cleared.
    CHK_FALSE( s1.fNet->getPeerSnapshotSyncNeeded(
                   s1.raftServer.get(), 3) );

    // Snapshot sync context must also be cleaned up alongside the flag,
    // so that late install_snapshot_response messages cannot rewind
    // next_log_idx/matched_idx to a stale snapshot point.
    // (The response handler in handle_install_snapshot_resp drops
    // responses when sync_ctx is null, so clearing it here is sufficient.)
    CHK_FALSE( s1.fNet->hasPeerSnapshotSyncCtx(
                   s1.raftServer.get(), 3) );

    // S3 must NOT have received an out_of_log_range warning — it was
    // in range the whole time; only the stale snapshot_sync flag was wrong.
    CHK_FALSE( s3.fNet->isServerOutOfLogRange(s3.raftServer.get()) );

    // S3's log index should be unchanged (no rollback).
    CHK_EQ( s3_log_idx, s3.raftServer->get_last_log_idx() );

    // Append one more log and verify normal replication works.
    for (size_t ii = NUM; ii < NUM + 1; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );

        CHK_TRUE( ret->get_accepted() );
        handlers.push_back(ret);
    }
    s1.fNet->execReqResp(); // replication.
    s1.fNet->execReqResp(); // commit.
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // State machines should be identical after normal replication.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    // There shouldn't be any open snapshot ctx.
    CHK_Z( s1.getTestSm()->getNumOpenedUserCtxs() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();

    return 0;
}

// Test for Fix 1: create_sync_snapshot_req handles null snapshot
// gracefully instead of calling system_exit/abort.
//
// Exercises the path via handle_join_leave -> sync_log_to_new_srv
// -> create_sync_snapshot_req, where get_last_snapshot() returns
// nullptr. The fix returns an empty request and sets the retry
// flag instead of aborting.
int snapshot_null_snapshot_join_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    std::string s2_addr = "S2";

    RaftPkg s1(f_base, 1, s1_addr);
    RaftPkg s2(f_base, 2, s2_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2};

    CHK_Z( launch_servers( pkgs ) );
    CHK_Z( make_group( pkgs ) );

    // Append a message using separate thread.
    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 5;
        param.reserved_log_items_ = 0;
        pp->raftServer->update_params(param);
    }

    const size_t NUM = 10;

    // Append messages.
    std::list< ptr< cmd_result< ptr<buffer> > > > handlers;
    for (size_t ii = 0; ii < NUM; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );
        CHK_TRUE( ret->get_accepted() );
        handlers.push_back(ret);
    }

    // Replicate and commit — triggers snapshot + log compaction.
    for (int ii = 0; ii < 4; ++ii) {
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    }

    // Verify snapshot was created and logs compacted.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );

    // Null out the leader's last_snapshot_ to simulate the race
    // condition where the snapshot becomes unavailable.
    s1.fNet->clearLastSnapshot(s1.raftServer.get());

    // Add a brand-new server S3. Its start_idx is 0, which is
    // < log_store_->start_index() (logs are compacted), so
    // sync_log_to_new_srv calls create_sync_snapshot_req.
    // With last_snapshot_ == nullptr, get_last_snapshot() returns null.
    //
    // Old behavior: system_exit -> abort.
    // New behavior (Fix 1): log warning, set retry flag, return.
    std::string s3_addr = "S3";
    RaftPkg s3(f_base, 3, s3_addr);
    CHK_Z( launch_servers( {&s3} ) );
    pkgs.push_back(&s3);

    s1.raftServer->add_srv( *(s3.getTestMgr()->get_srv_config()) );

    // Drive the join request/response. This calls sync_log_to_new_srv
    // which enters create_sync_snapshot_req with a null snapshot.
    // The server must NOT abort.
    s1.fNet->execReqResp();

    // Leader should still be alive.
    CHK_TRUE( s1.raftServer->is_leader() );

    // Restore the snapshot by appending more entries to trigger
    // a new snapshot creation cycle.
    for (size_t ii = NUM; ii < NUM + 5; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< cmd_result< ptr<buffer> > > ret =
            s1.raftServer->append_entries( {msg} );
        CHK_TRUE( ret->get_accepted() );
        handlers.push_back(ret);
    }

    // Replicate and commit — creates a new snapshot, restoring
    // last_snapshot_ to a valid pointer.
    for (int ii = 0; ii < 4; ++ii) {
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    }

    // Retry the join: trigger heartbeats and drive the protocol
    // until S3 finishes receiving the snapshot and catches up.
    for (int ii = 0; ii < 10; ++ii) {
        s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
        if (!s3.raftServer->is_receiving_snapshot()) break;
    }

    // A few more rounds to finalize log replication and commit.
    for (int ii = 0; ii < 4; ++ii) {
        s1.fTimer->invoke( timer_task_type::heartbeat_timer );
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    }

    // State machines should match.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    // No open snapshot contexts should remain.
    CHK_Z( s1.getTestSm()->getNumOpenedUserCtxs() );

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();

    return 0;
}

} // namespace snapshot_test
using namespace snapshot_test;

int main(int argc, char* argv[]) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    // Disable reconnection timer for deterministic test.
    debugging_options::get_instance().disable_reconn_backoff_ = true;

    ts.doTest( "snapshot basic test",
               snapshot_basic_test );

    ts.doTest( "snapshot new member restart test",
               snapshot_new_member_restart_test );

    ts.doTest( "snapshot manual creation test",
               snapshot_manual_creation_test );

    ts.doTest( "snapshot creation index inversion test",
               snapshot_creation_index_inversion_test );

    ts.doTest( "snapshot scheduled creation test",
               snapshot_scheduled_creation_test );

    ts.doTest( "snapshot randomized creation test",
               snapshot_randomized_creation_test );

    ts.doTest( "snapshot close for removed peer test",
               snapshot_close_for_removed_peer_test );

    ts.doTest( "snapshot leader switch test",
               snapshot_leader_switch_test );

    ts.doTest( "snapshot stale sync flag test",
               snapshot_stale_sync_flag_test );

    ts.doTest( "snapshot null snapshot join test",
               snapshot_null_snapshot_join_test );

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

