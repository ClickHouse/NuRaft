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
#include "snapshot_sync_ctx.hxx"
#include "test_common.h"

#include <atomic>
#include <stdio.h>
#include <thread>

using namespace nuraft;
using namespace raft_functional_common;

using raft_result = cmd_result< ptr<buffer> >;

namespace snapshot_test {

class StaleSnapshotTestServer : public raft_server {
public:
    StaleSnapshotTestServer(context* ctx, const init_options& opt)
        : raft_server(ctx, opt)
        {}

    bool handle_stale_final_snapshot_as_leader(snapshot_sync_req& req) {
        std::unique_lock<std::recursive_mutex> guard(lock_);
        role_ = srv_role::leader;
        quick_commit_index_ = req.get_snapshot().get_last_log_idx();
        bool ret = handle_snapshot_sync_req(req, guard);
        state_->set_receiving_snapshot(false);
        return ret;
    }
};

class BlockingUserCtxSm : public raft_functional_common::TestSm
{
public:
    nuraft::EventAwaiter read_started;
    nuraft::EventAwaiter release_read;
    std::atomic<size_t> free_count{0};

    int read_logical_snp_obj(snapshot& s,
                             void*& user_snp_ctx,
                             ulong obj_id,
                             ptr<buffer>& data_out,
                             bool& is_last_obj) override
    {
        int rc = raft_functional_common::TestSm::read_logical_snp_obj(
            s, user_snp_ctx, obj_id, data_out, is_last_obj);
        read_started.invoke();
        release_read.wait();
        return rc;
    }

    void free_user_snp_ctx(void*& user_snp_ctx) override
    {
        if (user_snp_ctx)
        {
            free_count.fetch_add(1);
        }
        raft_functional_common::TestSm::free_user_snp_ctx(user_snp_ctx);
        user_snp_ctx = nullptr;
    }
};

static ptr<buffer> make_test_msg(const std::string& test_msg) {
    ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
    msg->put(test_msg);
    return msg;
}

static ptr<buffer> make_resp_appendix_ctx(uint8_t order) {
    ptr<buffer> ctx = buffer::alloc(2);
    buffer_serializer bs(*ctx);
    bs.put_u8(0);
    bs.put_u8(order);
    return ctx;
}

static int append_and_replicate(RaftPkg& leader,
                                const std::vector<RaftPkg*>& pkgs,
                                size_t begin,
                                size_t count) {
    for (size_t ii = begin; ii < begin + count; ++ii) {
        ptr< cmd_result< ptr<buffer> > > ret =
            leader.raftServer->append_entries(
                { make_test_msg("test" + std::to_string(ii)) } );
        CHK_TRUE( ret->get_accepted() );

        leader.fNet->execReqResp();
        leader.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    }
    return 0;
}

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

int stale_snapshot_finalization_role_change_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s1_addr = "S1";
    ptr<FakeNetwork> f_net = cs_new<FakeNetwork>(s1_addr, f_base);
    f_base->addNetwork(f_net);

    ptr<FakeTimer> f_timer = cs_new<FakeTimer>(s1_addr, f_base->getLogger());
    ptr<TestMgr> s_mgr = cs_new<TestMgr>(1, s1_addr);
    ptr<TestSm> sm = cs_new<TestSm>(f_base->getLogger());
    ptr<logger_wrapper> log_wrapper = cs_new<logger_wrapper>("./srv1.log");
    ptr<logger> my_log = log_wrapper;

    raft_params params;
    params.with_election_timeout_lower(0);
    params.with_election_timeout_upper(10000);
    params.with_hb_interval(5000);
    params.with_client_req_timeout(1000000);
    params.with_reserved_log_items(0);
    params.with_snapshot_enabled(5);
    params.with_log_sync_stopping_gap(1);
    params.use_bg_thread_for_urgent_commit_ = false;

    context* ctx = new context( s_mgr, sm, {f_net}, my_log,
                                f_net, f_timer, params );
    ptr<StaleSnapshotTestServer> srv =
        cs_new<StaleSnapshotTestServer>
        ( ctx, raft_server::init_options(false, false, true) );

    ptr<snapshot> stale_snp =
        cs_new<snapshot>( 10, 3, s_mgr->load_config(), 0,
                          snapshot::logical_object );
    ptr<buffer> data = buffer::alloc(0);
    snapshot_sync_req req(stale_snp, 0, data, true);

    CHK_TRUE( srv->handle_stale_final_snapshot_as_leader(req) );
    CHK_NULL( sm->last_snapshot().get() );

    f_base->removeNetwork(s1_addr);
    log_wrapper->destroy();
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

int snapshot_async_request_state_lifecycle_test() {
    ptr<cluster_config> config = cs_new<cluster_config>();
    ptr<snapshot> snp =
        cs_new<snapshot>(10, 3, config, 0, snapshot::logical_object);
    snapshot_sync_ctx sync_ctx(snp, 2, 1000);

    CHK_FALSE( sync_ctx.is_async_snapshot_transfer_started() );
    CHK_FALSE( sync_ctx.is_async_snapshot_request_in_progress() );

    CHK_TRUE( sync_ctx.begin_async_snapshot_request() );
    CHK_TRUE( sync_ctx.is_async_snapshot_transfer_started() );
    CHK_TRUE( sync_ctx.is_async_snapshot_request_in_progress() );

    CHK_FALSE( sync_ctx.begin_async_snapshot_request() );
    CHK_TRUE( sync_ctx.is_async_snapshot_transfer_started() );
    CHK_TRUE( sync_ctx.is_async_snapshot_request_in_progress() );

    sync_ctx.finish_async_snapshot_request();
    CHK_TRUE( sync_ctx.is_async_snapshot_transfer_started() );
    CHK_FALSE( sync_ctx.is_async_snapshot_request_in_progress() );

    CHK_TRUE( sync_ctx.begin_async_snapshot_request() );
    sync_ctx.finish_async_snapshot_transfer();
    CHK_FALSE( sync_ctx.is_async_snapshot_transfer_started() );
    CHK_FALSE( sync_ctx.is_async_snapshot_request_in_progress() );

    return 0;
}

int async_snapshot_does_not_reselect_snapshot_at_offset_zero_test() {
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

    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (auto& entry: pkgs) {
        raft_params param = entry->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 5;
        param.use_bg_thread_for_snapshot_io_ = true;
        entry->raftServer->update_params(param);
    }

    for (size_t ii = 0; ii < 10; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< raft_result > ret = s1.raftServer->append_entries( {msg} );
        CHK_TRUE( ret->get_accepted() );
    }
    for (int ii = 0; ii < 4; ++ii) {
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    }

    ptr<snapshot> original_snp =
        s1.fNet->getLastSnapshot(s1.raftServer.get());
    CHK_NONNULL( original_snp.get() );

    ptr<snapshot> newer_snp =
        cs_new<snapshot>( original_snp->get_last_log_idx() + 100,
                          original_snp->get_last_log_term(),
                          original_snp->get_last_config(),
                          0,
                          snapshot::logical_object );
    s1.fNet->setLastSnapshot(s1.raftServer.get(), newer_snp);

    s1.fNet->setPeerSnapshotInSync(s1.raftServer.get(), 3, original_snp);
    s1.fNet->setPeerSnapshotSyncNeeded(s1.raftServer.get(), 3, true);
    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 3, 1);
    s1.fNet->beginPeerSnapshotAsyncRequest(s1.raftServer.get(), 3);

    ptr<req_msg> req =
        s1.fNet->createAppendEntriesReq(s1.raftServer.get(), 3);
    CHK_NULL( req.get() );

    CHK_EQ( original_snp->get_last_log_idx(),
            s1.fNet->getPeerSnapshotSyncCtxLastLogIdx(
                s1.raftServer.get(), 3) );
    CHK_TRUE( s1.fNet->getPeerAsyncSnapshotRequestInProgress(
                  s1.raftServer.get(), 3) );
    CHK_TRUE( s1.fNet->getPeerAsyncSnapshotTransferStarted(
                  s1.raftServer.get(), 3) );

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    fake_executer_killer(&exec_args);
    hh.join();
    CHK_Z( hh.getResult() );

    f_base->destroy();
    return 0;
}

int active_async_snapshot_context_not_cleared_by_caught_up_branch_test() {
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

    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (auto& entry: pkgs) {
        raft_params param = entry->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 5;
        param.use_bg_thread_for_snapshot_io_ = true;
        entry->raftServer->update_params(param);
    }

    for (size_t ii = 0; ii < 10; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        ptr< raft_result > ret = s1.raftServer->append_entries( {msg} );
        CHK_TRUE( ret->get_accepted() );
    }
    for (int ii = 0; ii < 4; ++ii) {
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    }

    ptr<snapshot> leader_snp =
        s1.fNet->getLastSnapshot(s1.raftServer.get());
    CHK_NONNULL( leader_snp.get() );

    s1.fNet->setPeerSnapshotInSync(s1.raftServer.get(), 3, leader_snp);
    s1.fNet->setPeerSnapshotSyncNeeded(s1.raftServer.get(), 3, true);
    s1.fNet->setPeerNextLogIdx(
        s1.raftServer.get(), 3, leader_snp->get_last_log_idx() + 1);
    s1.fNet->beginPeerSnapshotAsyncRequest(s1.raftServer.get(), 3);

    ptr<req_msg> active_req =
        s1.fNet->createAppendEntriesReq(s1.raftServer.get(), 3);
    CHK_NULL( active_req.get() );
    CHK_TRUE( s1.fNet->hasPeerSnapshotSyncCtx(
                  s1.raftServer.get(), 3) );
    CHK_TRUE( s1.fNet->getPeerAsyncSnapshotTransferStarted(
                  s1.raftServer.get(), 3) );

    s1.fNet->setPeerSnapshotInSync(s1.raftServer.get(), 3, leader_snp);
    s1.fNet->setPeerSnapshotSyncNeeded(s1.raftServer.get(), 3, true);
    s1.fNet->setPeerNextLogIdx(
        s1.raftServer.get(), 3, leader_snp->get_last_log_idx() + 1);

    ptr<req_msg> stale_req =
        s1.fNet->createAppendEntriesReq(s1.raftServer.get(), 3);
    (void)stale_req;
    CHK_FALSE( s1.fNet->hasPeerSnapshotSyncCtx(
                   s1.raftServer.get(), 3) );
    CHK_FALSE( s1.fNet->getPeerSnapshotSyncNeeded(
                   s1.raftServer.get(), 3) );

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

int snapshot_append_rejection_compacted_boundary_test() {
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

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        if (pp->raftServer->get_id() == 3) {
            param.snapshot_distance_ = 5;
            param.reserved_log_items_ = 0;
        } else {
            param.snapshot_distance_ = 100;
            param.reserved_log_items_ = 100;
        }
        pp->raftServer->update_params(param);
    }

    CHK_Z( append_and_replicate(s1, pkgs, 0, 5) );

    ptr<snapshot> s3_snp = s3.fNet->getLastSnapshot(s3.raftServer.get());
    CHK_NONNULL( s3_snp.get() );
    ulong snapshot_idx = s3_snp->get_last_log_idx();
    ulong boundary_next_idx = snapshot_idx + 1;

    raft_params s3_params = s3.raftServer->get_current_params();
    s3_params.snapshot_distance_ = 100;
    s3.raftServer->update_params(s3_params);

    CHK_Z( append_and_replicate(s1, pkgs, 5, 2) );
    CHK_GT( s3.getTestMgr()->get_inmem_log_store()->next_slot(),
            boundary_next_idx );

    ulong matched_before = 1;
    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 3, snapshot_idx);
    s1.fNet->setPeerNextLogIdxFloor(s1.raftServer.get(), 3, 0);
    s1.fNet->setPeerMatchedIdx(s1.raftServer.get(), 3, matched_before);
    s1.fNet->setPeerLastAcceptedLogIdx(
        s1.raftServer.get(), 3, matched_before);

    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    CHK_TRUE( s1.fNet->delieverReqTo("S3") );
    CHK_TRUE( s1.fNet->handleRespFrom("S3") );

    CHK_EQ( boundary_next_idx,
            s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
    CHK_EQ( boundary_next_idx,
            s1.fNet->getPeerNextLogIdxFloor(s1.raftServer.get(), 3) );
    CHK_EQ( matched_before,
            s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), 3) );
    CHK_EQ( matched_before,
            s1.fNet->getPeerLastAcceptedLogIdx(s1.raftServer.get(), 3) );

    CHK_TRUE( s1.fNet->execReqResp("S3") );
    CHK_GT( s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3),
            boundary_next_idx );
    CHK_GTEQ( s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), 3),
              snapshot_idx );

    s1.fNet->makeReqFailAll("S2");
    s1.fNet->makeReqFailAll("S3");

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int append_rejection_unmarked_ahead_next_index_test() {
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
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 100;
        pp->raftServer->update_params(param);
    }
    CHK_Z( append_and_replicate(s1, pkgs, 0, 3) );

    ulong prev_next_log = 2;
    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 3, prev_next_log);
    s1.fNet->setPeerNextLogIdxFloor(s1.raftServer.get(), 3, 0);
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );

    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    CHK_TRUE( s1.fNet->delieverReqTo("S3") );
    ptr<resp_msg> denial = cs_new<resp_msg>(
        s1.raftServer->get_term(),
        msg_type::append_entries_response,
        3,
        1,
        s1.raftServer->get_last_log_idx() + 10,
        false );
    CHK_TRUE( s1.fNet->replaceLastPendingResp("S3", denial) );
    CHK_TRUE( s1.fNet->handleRespFrom("S3") );

    CHK_EQ( prev_next_log - 1,
            s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
    CHK_EQ( 0, s1.fNet->getPeerNextLogIdxFloor(s1.raftServer.get(), 3) );
    CHK_EQ( 1, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );

    s1.fNet->makeReqFailAll("S2");
    s1.fNet->makeReqFailAll("S3");

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int append_rejection_backward_probe_throttle_test() {
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
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 100;
        param.max_append_size_ = 10;
        pp->raftServer->update_params(param);
    }
    CHK_Z( append_and_replicate(s1, pkgs, 0, 20) );
    CHK_NULL( s1.fNet->getLastSnapshot(s1.raftServer.get()).get() );

    ulong s3_next_log = 15;
    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 3, s3_next_log);
    s1.fNet->setPeerNextLogIdxFloor(s1.raftServer.get(), 3, 0);
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 2) );
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );

    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    for (int32 ii = 1; ii <= 5; ++ii) {
        ptr<req_msg> req = s1.fNet->getFirstPendingReq("S3");
        CHK_NONNULL( req.get() );
        CHK_EQ( (int)msg_type::append_entries_request, (int)req->get_type() );
        CHK_GT( req->log_entries().size(), 1 );

        CHK_TRUE( s1.fNet->delieverReqTo("S3") );
        ptr<resp_msg> denial = cs_new<resp_msg>(
            s1.raftServer->get_term(),
            msg_type::append_entries_response,
            3,
            1,
            s1.raftServer->get_last_log_idx() + 10,
            false );
        CHK_TRUE( s1.fNet->replaceLastPendingResp("S3", denial) );
        CHK_TRUE( s1.fNet->handleRespFrom("S3") );

        --s3_next_log;
        CHK_EQ( s3_next_log,
                s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
        CHK_EQ( ii, s1.fNet->getPeerBackwardLogProbeCount(
                     s1.raftServer.get(), 3) );
        CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                     s1.raftServer.get(), 2) );
    }

    ptr<req_msg> throttled_req = s1.fNet->getFirstPendingReq("S3");
    CHK_NONNULL( throttled_req.get() );
    CHK_EQ( (int)msg_type::append_entries_request,
            (int)throttled_req->get_type() );
    CHK_EQ( 1, throttled_req->log_entries().size() );
    CHK_EQ( 5, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );
    CHK_EQ( 10, s1.raftServer->get_current_params().max_append_size_ );

    for (int ii = 0; ii < 10 && s1.fNet->getNumPendingReqs("S2"); ++ii) {
        CHK_TRUE( s1.fNet->execReqResp("S2") );
    }
    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 2, 10);
    s1.fNet->setPeerNextLogIdxFloor(s1.raftServer.get(), 2, 0);
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    CHK_EQ( 1, s1.fNet->getNumPendingReqs("S3") );
    ptr<req_msg> s2_req = s1.fNet->getFirstPendingReq("S2");
    CHK_NONNULL( s2_req.get() );
    CHK_EQ( (int)msg_type::append_entries_request, (int)s2_req->get_type() );
    CHK_GT( s2_req->log_entries().size(), 1 );
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 2) );

    CHK_TRUE( s1.fNet->delieverReqTo("S3") );
    CHK_TRUE( s1.fNet->handleRespFrom("S3") );
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );

    for (int ii = 0; ii < 10 && s1.fNet->getNumPendingReqs("S3"); ++ii) {
        CHK_TRUE( s1.fNet->execReqResp("S3") );
    }
    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 3, 10);
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    ptr<req_msg> unthrottled_req = s1.fNet->getFirstPendingReq("S3");
    CHK_NONNULL( unthrottled_req.get() );
    CHK_EQ( (int)msg_type::append_entries_request,
            (int)unthrottled_req->get_type() );
    CHK_GT( unthrottled_req->log_entries().size(), 1 );

    s1.fNet->makeReqFailAll("S2");
    s1.fNet->makeReqFailAll("S3");

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int append_rejection_backward_probe_throttle_disabled_test() {
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
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 100;
        param.max_append_size_ = 10;
        param.with_append_entries_backward_probe_throttle_threshold(0);
        pp->raftServer->update_params(param);
    }
    CHK_Z( append_and_replicate(s1, pkgs, 0, 20) );

    ulong s3_next_log = 15;
    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 3, s3_next_log);
    s1.fNet->setPeerNextLogIdxFloor(s1.raftServer.get(), 3, 0);
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );
    CHK_EQ( 0, s1.raftServer->get_current_params()
                 .append_entries_backward_probe_throttle_threshold_ );

    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    for (int32 ii = 1; ii <= 5; ++ii) {
        ptr<req_msg> req = s1.fNet->getFirstPendingReq("S3");
        CHK_NONNULL( req.get() );
        CHK_EQ( (int)msg_type::append_entries_request, (int)req->get_type() );
        CHK_GT( req->log_entries().size(), 1 );

        CHK_TRUE( s1.fNet->delieverReqTo("S3") );
        ptr<resp_msg> denial = cs_new<resp_msg>(
            s1.raftServer->get_term(),
            msg_type::append_entries_response,
            3,
            1,
            s1.raftServer->get_last_log_idx() + 10,
            false );
        CHK_TRUE( s1.fNet->replaceLastPendingResp("S3", denial) );
        CHK_TRUE( s1.fNet->handleRespFrom("S3") );

        --s3_next_log;
        CHK_EQ( s3_next_log,
                s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
        CHK_EQ( ii, s1.fNet->getPeerBackwardLogProbeCount(
                     s1.raftServer.get(), 3) );
    }

    ptr<req_msg> unthrottled_req = s1.fNet->getFirstPendingReq("S3");
    CHK_NONNULL( unthrottled_req.get() );
    CHK_EQ( (int)msg_type::append_entries_request,
            (int)unthrottled_req->get_type() );
    CHK_GT( unthrottled_req->log_entries().size(), 1 );
    CHK_EQ( 5, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );
    CHK_EQ( 10, s1.raftServer->get_current_params().max_append_size_ );

    s1.fNet->makeReqFailAll("S2");
    s1.fNet->makeReqFailAll("S3");

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int append_rejection_stale_term_probe_reset_test() {
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
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 100;
        pp->raftServer->update_params(param);
    }
    CHK_Z( append_and_replicate(s1, pkgs, 0, 3) );
    CHK_GT( s1.raftServer->get_term(), 0 );

    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 3, 2);
    s1.fNet->setPeerNextLogIdxFloor(s1.raftServer.get(), 3, 0);
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    CHK_TRUE( s1.fNet->delieverReqTo("S3") );
    ptr<resp_msg> current_term_denial = cs_new<resp_msg>(
        s1.raftServer->get_term(),
        msg_type::append_entries_response,
        3,
        1,
        s1.raftServer->get_last_log_idx() + 10,
        false );
    CHK_TRUE( s1.fNet->replaceLastPendingResp("S3", current_term_denial) );
    CHK_TRUE( s1.fNet->handleRespFrom("S3") );
    CHK_EQ( 1, s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
    CHK_EQ( 1, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );

    CHK_TRUE( s1.fNet->delieverReqTo("S3") );
    ptr<resp_msg> stale_term_denial = cs_new<resp_msg>(
        s1.raftServer->get_term() - 1,
        msg_type::append_entries_response,
        3,
        1,
        s1.raftServer->get_last_log_idx() + 10,
        false );
    CHK_TRUE( s1.fNet->replaceLastPendingResp("S3", stale_term_denial) );
    CHK_TRUE( s1.fNet->handleRespFrom("S3") );
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );

    s1.fNet->makeReqFailAll("S2");
    s1.fNet->makeReqFailAll("S3");

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int append_rejection_invalid_compacted_boundary_marker_test() {
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
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 100;
        pp->raftServer->update_params(param);
    }
    CHK_Z( append_and_replicate(s1, pkgs, 0, 3) );

    ulong prev_next_log = 2;
    ulong floor = 0;
    ulong matched_before = 1;
    s1.fNet->setPeerBackwardLogProbeCount(s1.raftServer.get(), 3, 1);
    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 3, prev_next_log);
    s1.fNet->setPeerNextLogIdxFloor(s1.raftServer.get(), 3, floor);
    s1.fNet->setPeerMatchedIdx(s1.raftServer.get(), 3, matched_before);

    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    CHK_TRUE( s1.fNet->delieverReqTo("S3") );
    ptr<resp_msg> denial = cs_new<resp_msg>(
        s1.raftServer->get_term(),
        msg_type::append_entries_response,
        3,
        1,
        s1.raftServer->get_last_log_idx() + 10,
        false );
    denial->set_ctx(make_resp_appendix_ctx(3));
    CHK_TRUE( s1.fNet->replaceLastPendingResp("S3", denial) );
    CHK_TRUE( s1.fNet->handleRespFrom("S3") );

    CHK_EQ( prev_next_log,
            s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
    CHK_EQ( floor,
            s1.fNet->getPeerNextLogIdxFloor(s1.raftServer.get(), 3) );
    CHK_EQ( matched_before,
            s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), 3) );
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );

    s1.fNet->makeReqFailAll("S2");
    s1.fNet->makeReqFailAll("S3");

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int append_rejection_stale_compacted_boundary_marker_test() {
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
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 100;
        pp->raftServer->update_params(param);
    }
    CHK_Z( append_and_replicate(s1, pkgs, 0, 3) );

    ulong confirmed_idx = 3;
    ulong prev_next_log = confirmed_idx + 2;
    ulong floor = 0;
    s1.fNet->setPeerBackwardLogProbeCount(s1.raftServer.get(), 3, 1);
    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 3, prev_next_log);
    s1.fNet->setPeerNextLogIdxFloor(s1.raftServer.get(), 3, floor);
    s1.fNet->setPeerMatchedIdx(s1.raftServer.get(), 3, confirmed_idx);
    s1.fNet->setPeerLastAcceptedLogIdx(
        s1.raftServer.get(), 3, confirmed_idx);

    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    CHK_TRUE( s1.fNet->delieverReqTo("S3") );
    ptr<resp_msg> denial = cs_new<resp_msg>(
        s1.raftServer->get_term(),
        msg_type::append_entries_response,
        3,
        1,
        confirmed_idx,
        false );
    denial->set_ctx(make_resp_appendix_ctx(3));
    CHK_TRUE( s1.fNet->replaceLastPendingResp("S3", denial) );
    CHK_TRUE( s1.fNet->handleRespFrom("S3") );

    CHK_EQ( prev_next_log,
            s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
    CHK_EQ( floor,
            s1.fNet->getPeerNextLogIdxFloor(s1.raftServer.get(), 3) );
    CHK_EQ( confirmed_idx,
            s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), 3) );
    CHK_EQ( confirmed_idx,
            s1.fNet->getPeerLastAcceptedLogIdx(s1.raftServer.get(), 3) );
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );

    s1.fNet->makeReqFailAll("S2");
    s1.fNet->makeReqFailAll("S3");

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int append_rejection_do_not_rewind_ahead_next_index_test() {
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
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 100;
        pp->raftServer->update_params(param);
    }
    CHK_Z( append_and_replicate(s1, pkgs, 0, 3) );

    ulong prev_next_log = 2;
    ulong floor = 0;
    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 3, prev_next_log);
    s1.fNet->setPeerNextLogIdxFloor(s1.raftServer.get(), 3, floor);
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );

    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    CHK_TRUE( s1.fNet->delieverReqTo("S3") );
    s1.fNet->setPeerBackwardLogProbeCount(s1.raftServer.get(), 3, 5);
    CHK_EQ( 5, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );
    ptr<resp_msg> denial = cs_new<resp_msg>(
        s1.raftServer->get_term(),
        msg_type::append_entries_response,
        3,
        1,
        s1.raftServer->get_last_log_idx() + 10,
        false );
    denial->set_ctx(make_resp_appendix_ctx(1));
    CHK_TRUE( s1.fNet->replaceLastPendingResp("S3", denial) );
    CHK_TRUE( s1.fNet->handleRespFrom("S3") );

    CHK_EQ( prev_next_log,
            s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
    CHK_EQ( floor,
            s1.fNet->getPeerNextLogIdxFloor(s1.raftServer.get(), 3) );
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );

    ptr<req_msg> next_req = s1.fNet->getFirstPendingReq("S3");
    CHK_NONNULL( next_req.get() );
    CHK_EQ( (int)msg_type::append_entries_request, (int)next_req->get_type() );
    CHK_GT( next_req->log_entries().size(), 1 );

    s1.fNet->makeReqFailAll("S2");
    s1.fNet->makeReqFailAll("S3");

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

int append_rejection_unknown_extra_order_probe_reset_test() {
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
    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 100;
        param.max_append_size_ = 10;
        pp->raftServer->update_params(param);
    }
    CHK_Z( append_and_replicate(s1, pkgs, 0, 3) );

    ulong prev_next_log = 2;
    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 3, prev_next_log);
    s1.fNet->setPeerNextLogIdxFloor(s1.raftServer.get(), 3, 0);

    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    CHK_TRUE( s1.fNet->delieverReqTo("S3") );
    s1.fNet->setPeerBackwardLogProbeCount(s1.raftServer.get(), 3, 5);
    ptr<resp_msg> denial = cs_new<resp_msg>(
        s1.raftServer->get_term(),
        msg_type::append_entries_response,
        3,
        1,
        s1.raftServer->get_last_log_idx() + 10,
        false );
    denial->set_ctx(make_resp_appendix_ctx(255));
    CHK_TRUE( s1.fNet->replaceLastPendingResp("S3", denial) );
    CHK_TRUE( s1.fNet->handleRespFrom("S3") );

    CHK_EQ( prev_next_log - 1,
            s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );

    ptr<req_msg> next_req = s1.fNet->getFirstPendingReq("S3");
    CHK_NONNULL( next_req.get() );
    CHK_EQ( (int)msg_type::append_entries_request, (int)next_req->get_type() );
    CHK_GT( next_req->log_entries().size(), 1 );

    s1.fNet->makeReqFailAll("S2");
    s1.fNet->makeReqFailAll("S3");

    print_stats(pkgs);

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();

    f_base->destroy();

    return 0;
}

// Regression test for stale-RPC-after-snapshot backward probing bug.
//
// Production scenario: after snapshot sync completes, a force-reconnect
// resets next_log_idx to 0. The stale install_snapshot_response arrives
// and correctly sets next_log_idx = S+1. Then a stale append_entries
// denial arrives with NextIndex = S+1 (== next_log_idx). The strict `>`
// in the fast-move check fails, and the decrement path walks backward
// through the entire log (~15 minutes in production).
//
// This test verifies that next_log_idx_floor_, set during snapshot
// completion, prevents the backward spiral.
int snapshot_rewind_floor_test() {
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

    ExecArgs exec_args(&s1);
    TestSuite::ThreadHolder hh(&exec_args, fake_executer, fake_executer_killer);

    for (auto& entry: pkgs) {
        RaftPkg* pp = entry;
        raft_params param = pp->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 5;
        pp->raftServer->update_params(param);
    }

    // Append 5 entries, replicate to S2 only (S3 lags behind).
    for (size_t ii = 0; ii < 5; ++ii) {
        std::string test_msg = "test" + std::to_string(ii);
        ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
        msg->put(test_msg);
        exec_args.setMsg(msg);
        exec_args.eaExecuter.invoke();
        TestSuite::sleep_ms(EXECUTOR_WAIT_MS);
        CHK_NULL( exec_args.getMsg().get() );

        s1.fNet->execReqResp("S2");
        s1.fNet->execReqResp("S2");
        CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    }

    // S3 is lagging behind. Make requests to S3 fail to force snapshot path.
    s1.fNet->makeReqFail("S3");

    // Trigger heartbeat to S3 — initiates snapshot transmission.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->execReqResp();

    // Send the entire snapshot.
    do {
        s1.fNet->execReqResp();
    } while (s3.raftServer->is_receiving_snapshot());

    s1.fNet->execReqResp();
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    // All servers should be in sync.
    CHK_OK( s2.getTestSm()->isSame( *s1.getTestSm() ) );
    CHK_OK( s3.getTestSm()->isSame( *s1.getTestSm() ) );

    // --- Verify floor was set after snapshot sync ---
    ulong floor = s1.fNet->getPeerNextLogIdxFloor(s1.raftServer.get(), 3);
    ulong next_idx = s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3);
    CHK_GT( floor, 0 );
    CHK_SMEQ( floor, next_idx );

    // Record the snapshot boundary.
    ulong snapshot_floor = floor;
    ulong current_term = s1.raftServer->get_term();

    // --- Phase 1: Test decrement-path clamping (exact production scenario) ---
    // Set up the post-race state: next_log_idx == floor == snapshot boundary.
    // A denial with NextIndex == next_log_idx makes the strict `>` check fail,
    // falling to the decrement path. The floor must prevent the decrement.
    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 3, snapshot_floor);
    s1.fNet->setPeerMatchedIdx(s1.raftServer.get(), 3, 0);
    s1.fNet->setPeerNextLogIdxFloor(s1.raftServer.get(), 3, snapshot_floor);
    s1.fNet->setPeerBackwardLogProbeCount(s1.raftServer.get(), 3, 1);

    // Trigger heartbeat → creates append_entries request to S3.
    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    // Deliver request to S3, getting a real response into the pending queue.
    s1.fNet->delieverReqTo("S3");
    // Replace the real (accepted) response with a crafted denial.
    // NextIndex = snapshot_floor: same as next_log_idx, so `>` fails.
    {
        ptr<resp_msg> denial = cs_new<resp_msg>(
            current_term,
            msg_type::append_entries_response,
            3,   // src = S3
            1,   // dst = S1
            snapshot_floor,  // next_idx == next_log_idx
            false);          // accepted = false
        s1.fNet->replaceLastPendingResp("S3", denial);
    }
    // Deliver the crafted denial to S1's response handler.
    s1.fNet->handleRespFrom("S3");

    // Verify: next_log_idx must NOT have decremented below the floor.
    ulong after_phase1 =
        s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3);
    CHK_GTEQ( after_phase1, snapshot_floor );
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );

    // --- Phase 2: Test fast-move clamping ---
    // Set next_log_idx above the floor. A denial with NextIndex below the
    // floor triggers the fast-move path, but the floor must clamp it.
    s1.fNet->setPeerNextLogIdx(
        s1.raftServer.get(), 3, snapshot_floor + 5);
    s1.fNet->setPeerBackwardLogProbeCount(s1.raftServer.get(), 3, 1);

    s1.fTimer->invoke(timer_task_type::heartbeat_timer);
    s1.fNet->delieverReqTo("S3");
    {
        // NextIndex = 1: far below the floor, fast-move would jump there.
        ptr<resp_msg> denial = cs_new<resp_msg>(
            current_term,
            msg_type::append_entries_response,
            3,   // src = S3
            1,   // dst = S1
            1,   // next_idx far below floor
            false);
        s1.fNet->replaceLastPendingResp("S3", denial);
    }
    s1.fNet->handleRespFrom("S3");

    // Verify: fast-move was clamped to the floor, not to 1.
    ulong after_phase2 =
        s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3);
    CHK_GTEQ( after_phase2, snapshot_floor );
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(
                 s1.raftServer.get(), 3) );

    // --- Phase 3: Verify recovery after floor is exercised ---
    // Restore proper state and confirm normal replication still works.
    s1.fNet->setPeerNextLogIdx(s1.raftServer.get(), 3, snapshot_floor);
    s1.fNet->setPeerMatchedIdx(
        s1.raftServer.get(), 3, snapshot_floor - 1);
    for (int ii = 0; ii < 5; ++ii) {
        s1.fTimer->invoke(timer_task_type::heartbeat_timer);
        s1.fNet->execReqResp();
        s1.fNet->execReqResp();
    }

    ulong final_next_idx =
        s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3);
    CHK_GTEQ( final_next_idx, snapshot_floor );

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

int snapshot_user_ctx_deferred_free_test()
{
    BlockingUserCtxSm sm;
    ptr<cluster_config> config = cs_new<cluster_config>();
    ptr<snapshot> snp = cs_new<snapshot>(10, 3, config, 0, snapshot::logical_object);
    ptr<snapshot_sync_ctx> sync_ctx = cs_new<snapshot_sync_ctx>(snp, 2, 1000000);
    std::atomic<int> reader_result{0};

    std::thread reader([&sm, &sync_ctx, &snp, &reader_result]()
    {
        try
        {
            ptr<buffer> data;
            bool is_last_obj = false;
            snapshot_sync_ctx::user_snp_ctx_io_guard guard(*sync_ctx, sm);
            if (!guard)
            {
                reader_result = 1;
                return;
            }

            int rc = sm.read_logical_snp_obj(*snp, guard.get(), 0, data, is_last_obj);
            bool closed = guard.finish();
            if (rc != 0 || !closed)
            {
                reader_result = 2;
            }
        }
        catch (...)
        {
            reader_result = 3;
        }
    });

    sm.read_started.wait();
    sync_ctx->close_user_snp_ctx(sm);
    CHK_EQ((size_t)0, sm.free_count.load());
    sm.release_read.invoke();
    reader.join();

    CHK_Z(reader_result.load());
    CHK_EQ((size_t)1, sm.free_count.load());
    CHK_Z(sm.getNumOpenedUserCtxs());

    snapshot_sync_ctx::user_snp_ctx_io_guard closed_guard(*sync_ctx, sm);
    CHK_FALSE(closed_guard);

    ptr<snapshot_sync_ctx> closed_before_read_ctx =
        cs_new<snapshot_sync_ctx>(snp, 2, 1000000);
    closed_before_read_ctx->close_user_snp_ctx(sm);
    snapshot_sync_ctx::user_snp_ctx_io_guard closed_before_read_guard(
        *closed_before_read_ctx, sm);
    CHK_FALSE(closed_before_read_guard);
    CHK_EQ((size_t)1, sm.free_count.load());

    return 0;
}

// ---------------------------------------------------------------------------
// Late snapshot-install acknowledgements.
//
// A follower answers the last snapshot object with a terminal context. The leader used to need
// its own `snapshot_sync_ctx` to interpret that answer, because the installed snapshot's index
// lived only there. Applying a large snapshot routinely outruns `snapshot_sync_ctx_timeout` --
// which is a per-round-trip responsiveness budget, not an allowance for the apply -- so the
// context was destroyed and a *successful* install was discarded, leaving `next_log_idx_floor`
// unset and nothing to stop a subsequent backward log walk.
//
// The terminal context now carries the installed snapshot's `last_log_idx`, so these tests drive
// the leader-side handling of that payload: which acknowledgements it acts on, which it refuses,
// and what it must never do to a peer that has a *different* install in flight.
// ---------------------------------------------------------------------------

// Build a terminal install-snapshot context.
//
// `total_len` is explicit so the malformed and future-format cases can be expressed: the format
// says `len == 1` with tag 0 is the historical indexless marker, and any tag `>= 1` carries the
// index in bytes 1..8. Everything else is malformed.
static ptr<buffer> make_snp_install_done_ctx(uint8_t tag,
                                             uint64_t snp_idx,
                                             size_t total_len) {
    ptr<buffer> ctx = buffer::alloc(total_len);
    buffer_serializer bs(*ctx);
    bs.put_u8(tag);
    if (total_len >= sizeof(uint8_t) + sizeof(uint64_t)) {
        bs.put_u64(snp_idx);
    }
    while (bs.pos() < total_len) {
        bs.put_u8(0);
    }
    ctx->pos(0);
    return ctx;
}

// Get a real response from S3 into the leader's pending queue, to be replaced with a crafted one
// later.
//
// This has to happen before the state under test is arranged, and exactly once per delivery: a
// heartbeat only produces a request while the peer is free, and with a snapshot sync context in
// place it may produce no ordinary request at all.
static int arm_pending_resp(RaftPkg& leader, const std::string& peer_endpoint) {
    leader.fTimer->invoke( timer_task_type::heartbeat_timer );
    CHK_TRUE( leader.fNet->delieverReqTo(peer_endpoint) );
    return 0;
}

// Put the peer back into a state where `arm_pending_resp` works: no snapshot sync context, and a
// cursor inside the leader's *available* log.
//
// The leader compacts its log up to the last snapshot, so a cursor below that boundary makes the
// heartbeat start a snapshot transfer instead of producing the ordinary request we want to answer.
// The state actually under test is set afterwards, once the response is already armed.
static void reset_peer_for_arming(RaftPkg& leader,
                                  int32 peer_id,
                                  ulong safe_next_log_idx) {
    leader.fNet->setPeerSnapshotInSync(leader.raftServer.get(), peer_id, nullptr);
    leader.fNet->setPeerNextLogIdx(
        leader.raftServer.get(), peer_id, safe_next_log_idx);
    leader.fNet->setPeerMatchedIdx(
        leader.raftServer.get(), peer_id, safe_next_log_idx - 1);
    leader.fNet->setPeerNextLogIdxFloor(leader.raftServer.get(), peer_id, 0);
}

static void set_peer_cursor(RaftPkg& leader,
                            int32 peer_id,
                            ulong next_log_idx,
                            ulong matched_idx,
                            ulong floor_idx) {
    leader.fNet->setPeerNextLogIdx(leader.raftServer.get(), peer_id, next_log_idx);
    leader.fNet->setPeerMatchedIdx(leader.raftServer.get(), peer_id, matched_idx);
    leader.fNet->setPeerNextLogIdxFloor(leader.raftServer.get(), peer_id, floor_idx);
}

// Assert the peer's progress is exactly where it was left, i.e. the acknowledgement was refused.
static int check_peer_cursor(RaftPkg& leader,
                             int32 peer_id,
                             ulong next_log_idx,
                             ulong matched_idx,
                             ulong floor_idx) {
    CHK_EQ( next_log_idx,
            leader.fNet->getPeerNextLogIdx(leader.raftServer.get(), peer_id) );
    CHK_EQ( matched_idx,
            leader.fNet->getPeerMatchedIdx(leader.raftServer.get(), peer_id) );
    CHK_EQ( floor_idx,
            leader.fNet->getPeerNextLogIdxFloor(leader.raftServer.get(), peer_id) );
    return 0;
}

static int deliver_install_snapshot_resp(RaftPkg& leader,
                                         const std::string& peer_endpoint,
                                         int32 peer_id,
                                         ulong term,
                                         bool accepted,
                                         ulong next_idx,
                                         ptr<buffer> ctx) {
    ptr<resp_msg> resp = cs_new<resp_msg>( term,
                                           msg_type::install_snapshot_response,
                                           peer_id,
                                           1,   // dst = S1, the leader.
                                           next_idx,
                                           accepted );
    if (ctx) resp->set_ctx(ctx);
    CHK_TRUE( leader.fNet->replaceLastPendingResp(peer_endpoint, resp) );
    CHK_TRUE( leader.fNet->handleRespFrom(peer_endpoint) );
    return 0;
}

// Bring up a three-node group, replicate enough entries for the leader to hold a snapshot, and
// leave a crafted response armed for S3.
struct LateAckFixture {
    ptr<FakeNetworkBase> f_base;
    ptr<RaftPkg> s1;
    ptr<RaftPkg> s2;
    ptr<RaftPkg> s3;
    std::vector<RaftPkg*> pkgs;
    ptr<snapshot> leader_snp;
    ulong precommit_idx = 0;
    ulong term = 0;
    // A peer cursor that is inside the leader's uncompacted log, so a heartbeat produces an
    // ordinary append rather than an install, and low enough that the leader never sees a peer
    // claiming to be ahead of its own log.
    ulong safe_next_log_idx = 0;
};

static int setup_late_ack_fixture(LateAckFixture& fx,
                                 bool use_bg_thread_for_snapshot_io) {
    reset_log_files();
    fx.f_base = cs_new<FakeNetworkBase>();

    fx.s1 = cs_new<RaftPkg>(fx.f_base, 1, std::string("S1"));
    fx.s2 = cs_new<RaftPkg>(fx.f_base, 2, std::string("S2"));
    fx.s3 = cs_new<RaftPkg>(fx.f_base, 3, std::string("S3"));
    fx.pkgs = {fx.s1.get(), fx.s2.get(), fx.s3.get()};

    CHK_Z( launch_servers( fx.pkgs ) );
    CHK_Z( make_group( fx.pkgs ) );

    for (auto& entry: fx.pkgs) {
        raft_params param = entry->raftServer->get_current_params();
        param.return_method_ = raft_params::async_handler;
        param.snapshot_distance_ = 5;
        param.use_bg_thread_for_snapshot_io_ = use_bg_thread_for_snapshot_io;
        entry->raftServer->update_params(param);
    }

    CHK_Z( append_and_replicate(*fx.s1, fx.pkgs, 0, 20) );

    fx.leader_snp = fx.s1->fNet->getLastSnapshot(fx.s1->raftServer.get());
    CHK_NONNULL( fx.leader_snp.get() );
    fx.precommit_idx = fx.s1->fNet->getPrecommitIndex(fx.s1->raftServer.get());
    fx.term = fx.s1->raftServer->get_term();
    fx.safe_next_log_idx = fx.precommit_idx;
    CHK_GT( fx.safe_next_log_idx, fx.leader_snp->get_last_log_idx() );

    return 0;
}

static void shutdown_late_ack_fixture(LateAckFixture& fx) {
    fx.s1->raftServer->shutdown();
    fx.s2->raftServer->shutdown();
    fx.s3->raftServer->shutdown();
    fx.f_base->destroy();
}

// Drive the real timeout check so the peer reaches the exact post-timeout state: the context is
// destroyed, but `snapshot_sync_is_needed` -- which the timeout does not touch -- is left as it
// was. A zero timeout makes `snapshot_sync_ctx::get_timer` report expiry on the first check.
static int expire_snapshot_sync_ctx(RaftPkg& leader,
                                    int32 peer_id,
                                    ptr<snapshot> snp) {
    leader.fNet->setPeerSnapshotInSync(
        leader.raftServer.get(), peer_id, snp, /*timeout_ms=*/0);
    CHK_TRUE( leader.fNet->hasPeerSnapshotSyncCtx(
                  leader.raftServer.get(), peer_id) );
    CHK_TRUE( leader.fNet->checkPeerSnapshotTimeout(
                  leader.raftServer.get(), peer_id) );
    CHK_FALSE( leader.fNet->hasPeerSnapshotSyncCtx(
                   leader.raftServer.get(), peer_id) );
    return 0;
}

// Case 1: the acknowledgement of a successful install is acted upon even though the sync context
// it belonged to is already gone. This is the production failure, and the floor being set is the
// part that stops the backward walk.
int late_snapshot_install_ack_accepted_test() {
    LateAckFixture fx;
    CHK_Z( setup_late_ack_fixture(fx, /*use_bg_thread_for_snapshot_io=*/false) );
    RaftPkg& s1 = *fx.s1;

    ulong snp_idx = fx.leader_snp->get_last_log_idx();

    reset_peer_for_arming(s1, 3, fx.safe_next_log_idx);
    CHK_Z( arm_pending_resp(s1, "S3") );

    // The peer looks far behind, as it does while it is still applying, and a backward walk has
    // already started.
    set_peer_cursor(s1, 3, 1, 0, 0);
    s1.fNet->setPeerBackwardLogProbeCount(s1.raftServer.get(), 3, 3);

    CHK_Z( expire_snapshot_sync_ctx(s1, 3, fx.leader_snp) );

    CHK_Z( deliver_install_snapshot_resp(
               s1, "S3", 3, fx.term, true, 1,
               make_snp_install_done_ctx(1, snp_idx, 9)) );

    CHK_EQ( snp_idx + 1, s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
    CHK_EQ( snp_idx, s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), 3) );
    CHK_EQ( snp_idx + 1,
            s1.fNet->getPeerNextLogIdxFloor(s1.raftServer.get(), 3) );
    // A non-zero floor is what makes a backward walk impossible; the probe counter is also back
    // to zero, so no walk is in progress either.
    CHK_EQ( 0, s1.fNet->getPeerBackwardLogProbeCount(s1.raftServer.get(), 3) );

    print_stats(fx.pkgs);
    shutdown_late_ack_fixture(fx);
    return 0;
}

// Case 2: the updates are monotone. A late acknowledgement for an old snapshot must not pull a
// peer that has since advanced backwards -- that is the hazard which made dropping such
// acknowledgements the safe choice in the first place.
int late_snapshot_install_ack_monotonic_test() {
    LateAckFixture fx;
    CHK_Z( setup_late_ack_fixture(fx, /*use_bg_thread_for_snapshot_io=*/false) );
    RaftPkg& s1 = *fx.s1;

    reset_peer_for_arming(s1, 3, fx.safe_next_log_idx);
    CHK_Z( arm_pending_resp(s1, "S3") );

    // An index the leader really could have snapshotted, but long behind where the peer is now.
    ulong stale_idx = 3;
    CHK_GT( fx.leader_snp->get_last_log_idx(), stale_idx );

    ulong next_log_idx = fx.safe_next_log_idx;
    ulong matched_idx = next_log_idx - 1;
    ulong floor_idx = next_log_idx - 2;
    set_peer_cursor(s1, 3, next_log_idx, matched_idx, floor_idx);

    CHK_Z( expire_snapshot_sync_ctx(s1, 3, fx.leader_snp) );

    CHK_Z( deliver_install_snapshot_resp(
               s1, "S3", 3, fx.term, true, 1,
               make_snp_install_done_ctx(1, stale_idx, 9)) );

    CHK_Z( check_peer_cursor(s1, 3, next_log_idx, matched_idx, floor_idx) );

    print_stats(fx.pkgs);
    shutdown_late_ack_fixture(fx);
    return 0;
}

// Case 2b: the acknowledgement must also clear `snapshot_sync_is_needed`, otherwise the whole
// point of accepting it is lost. A follower that answered `RECEIVING_SNAPSHOT` while applying
// leaves that flag set, and the timeout does not clear it, so the next append cycle would start
// another full install as soon as the leader holds a newer snapshot.
int late_snapshot_install_ack_no_reinstall_test() {
    LateAckFixture fx;
    CHK_Z( setup_late_ack_fixture(fx, /*use_bg_thread_for_snapshot_io=*/false) );
    RaftPkg& s1 = *fx.s1;

    ulong snp_idx = fx.leader_snp->get_last_log_idx();

    // Arm before the newer snapshot and the sync-needed flag are in place, so that this heartbeat
    // is an ordinary one and does not itself start an install.
    reset_peer_for_arming(s1, 3, fx.safe_next_log_idx);
    CHK_Z( arm_pending_resp(s1, "S3") );

    set_peer_cursor(s1, 3, 1, 0, 0);
    s1.fNet->setPeerSnapshotSyncNeeded(s1.raftServer.get(), 3, true);

    // The leader moved on to a newer snapshot while the follower was applying the old one.
    ptr<snapshot> newer_snp =
        cs_new<snapshot>( snp_idx + 100,
                          fx.leader_snp->get_last_log_term(),
                          fx.leader_snp->get_last_config(),
                          0,
                          snapshot::logical_object );
    s1.fNet->setLastSnapshot(s1.raftServer.get(), newer_snp);

    CHK_Z( expire_snapshot_sync_ctx(s1, 3, fx.leader_snp) );

    CHK_Z( deliver_install_snapshot_resp(
               s1, "S3", 3, fx.term, true, 1,
               make_snp_install_done_ctx(1, snp_idx, 9)) );

    CHK_FALSE( s1.fNet->getPeerSnapshotSyncNeeded(s1.raftServer.get(), 3) );

    // With the flag cleared the peer is served ordinary log replication. Had it stayed set, this
    // would be an `install_snapshot_request` for `newer_snp` -- the retransfer this fix exists to
    // avoid.
    ptr<req_msg> req = s1.fNet->createAppendEntriesReq(s1.raftServer.get(), 3);
    CHK_NONNULL( req.get() );
    CHK_EQ( msg_type::append_entries_request, req->get_type() );

    print_stats(fx.pkgs);
    shutdown_late_ack_fixture(fx);
    return 0;
}

// Case 3: every index the leader acts on is validated first. Each of these acknowledgements is
// accepted by the follower but carries an index the leader must refuse, and none of them may
// touch the peer's progress.
int late_snapshot_install_ack_validation_test() {
    LateAckFixture fx;
    CHK_Z( setup_late_ack_fixture(fx, /*use_bg_thread_for_snapshot_io=*/false) );
    RaftPkg& s1 = *fx.s1;

    struct BadAck {
        const char* what;
        ulong term_offset;   // Subtracted from the current term.
        ulong idx;
    };
    std::vector<BadAck> bad_acks = {
        // Beyond anything this leader ever wrote, so it cannot have sent such a snapshot.
        { "idx beyond precommit index", 0, fx.precommit_idx + 1 },
        // From an earlier term: the peer state it describes may no longer be this leader's.
        { "stale term", 1, fx.leader_snp->get_last_log_idx() },
        // Never a snapshot boundary.
        { "zero idx", 0, 0 },
        // `idx + 1` would wrap.
        { "max idx", 0, std::numeric_limits<ulong>::max() },
    };

    ulong next_log_idx = fx.safe_next_log_idx;
    ulong matched_idx = next_log_idx - 1;
    ulong floor_idx = next_log_idx - 2;

    for (auto& bad_ack: bad_acks) {
        _msg("  case: %s\n", bad_ack.what);

        reset_peer_for_arming(s1, 3, fx.safe_next_log_idx);
        CHK_Z( arm_pending_resp(s1, "S3") );

        set_peer_cursor(s1, 3, next_log_idx, matched_idx, floor_idx);
        CHK_Z( expire_snapshot_sync_ctx(s1, 3, fx.leader_snp) );
        CHK_Z( deliver_install_snapshot_resp(
                   s1, "S3", 3, fx.term - bad_ack.term_offset, true, 1,
                   make_snp_install_done_ctx(1, bad_ack.idx, 9)) );

        CHK_Z( check_peer_cursor(s1, 3, next_log_idx, matched_idx, floor_idx) );
    }

    // A *declined* response is a different thing entirely: it means the follower is already past
    // what we offered, and the existing handling deliberately rewrites the peer's position and
    // zeroes the floor. Carrying the new payload must not change that.
    _msg("  case: declined response with an extended payload\n");
    reset_peer_for_arming(s1, 3, fx.safe_next_log_idx);
    CHK_Z( arm_pending_resp(s1, "S3") );
    set_peer_cursor(s1, 3, next_log_idx, matched_idx, floor_idx);
    CHK_Z( deliver_install_snapshot_resp(
               s1, "S3", 3, fx.term, false, next_log_idx + 1,
               make_snp_install_done_ctx(
                   1, fx.leader_snp->get_last_log_idx(), 9)) );

    CHK_EQ( next_log_idx + 1,
            s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
    CHK_EQ( 0, s1.fNet->getPeerNextLogIdxFloor(s1.raftServer.get(), 3) );

    print_stats(fx.pkgs);
    shutdown_late_ack_fixture(fx);
    return 0;
}

// Case 4: an acknowledgement for snapshot `I` that arrives while an install of `J` is in flight
// must not complete `J`. `matched_idx` feeds the commit-quorum calculation, so crediting the peer
// with a snapshot it never installed is a safety problem, not just bookkeeping.
//
// This test fails on the unfixed code, where the terminal context is only ever a "done" flag.
int late_snapshot_install_ack_wrong_snapshot_test() {
    LateAckFixture fx;
    CHK_Z( setup_late_ack_fixture(fx, /*use_bg_thread_for_snapshot_io=*/true) );
    RaftPkg& s1 = *fx.s1;

    ulong idx_i = 3;
    ulong idx_j = fx.leader_snp->get_last_log_idx();
    CHK_GT( idx_j, idx_i );

    reset_peer_for_arming(s1, 3, fx.safe_next_log_idx);
    CHK_Z( arm_pending_resp(s1, "S3") );

    set_peer_cursor(s1, 3, 1, 0, 0);

    // The install of `I` timed out, and the leader restarted the transfer with `J`. Marking the
    // async read as in progress keeps the resumed transfer from being driven any further by the
    // response we are about to deliver.
    ptr<snapshot> snp_i =
        cs_new<snapshot>( idx_i,
                          fx.leader_snp->get_last_log_term(),
                          fx.leader_snp->get_last_config(),
                          0,
                          snapshot::logical_object );
    CHK_Z( expire_snapshot_sync_ctx(s1, 3, snp_i) );

    s1.fNet->setPeerSnapshotInSync(s1.raftServer.get(), 3, fx.leader_snp);
    s1.fNet->beginPeerSnapshotAsyncRequest(s1.raftServer.get(), 3);
    CHK_TRUE( s1.fNet->hasPeerSnapshotSyncCtx(s1.raftServer.get(), 3) );

    CHK_Z( deliver_install_snapshot_resp(
               s1, "S3", 3, fx.term, true, 1,
               make_snp_install_done_ctx(1, idx_i, 9)) );

    // `J` was not credited, and its transfer is still live and able to report for itself.
    CHK_EQ( idx_i, s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), 3) );
    CHK_TRUE( s1.fNet->hasPeerSnapshotSyncCtx(s1.raftServer.get(), 3) );
    CHK_EQ( idx_j,
            s1.fNet->getPeerSnapshotSyncCtxLastLogIdx(s1.raftServer.get(), 3) );
    // `I` really was installed, so it is credited.
    CHK_EQ( idx_i + 1, s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
    CHK_EQ( idx_i + 1,
            s1.fNet->getPeerNextLogIdxFloor(s1.raftServer.get(), 3) );

    print_stats(fx.pkgs);
    shutdown_late_ack_fixture(fx);
    return 0;
}

// Case 5: a follower on the old format sends a one-byte marker with no index. Both outcomes are
// unchanged from before this fix -- deliberately. The second one is the residue that a carried
// index cannot fix, because there is nothing to correlate on.
int late_snapshot_install_ack_legacy_marker_test() {
    LateAckFixture fx;
    CHK_Z( setup_late_ack_fixture(fx, /*use_bg_thread_for_snapshot_io=*/false) );
    RaftPkg& s1 = *fx.s1;

    ulong next_log_idx = fx.safe_next_log_idx;
    ulong matched_idx = next_log_idx - 1;
    ulong floor_idx = next_log_idx - 2;

    // Without a context the response stays uninterpretable, so it is still dropped.
    reset_peer_for_arming(s1, 3, fx.safe_next_log_idx);
    CHK_Z( arm_pending_resp(s1, "S3") );
    set_peer_cursor(s1, 3, next_log_idx, matched_idx, floor_idx);
    CHK_Z( expire_snapshot_sync_ctx(s1, 3, fx.leader_snp) );
    CHK_Z( deliver_install_snapshot_resp(
               s1, "S3", 3, fx.term, true, 1,
               make_snp_install_done_ctx(0, 0, 1)) );

    CHK_Z( check_peer_cursor(s1, 3, next_log_idx, matched_idx, floor_idx) );

    // With a context, the marker still completes whatever that context holds -- including pulling
    // `matched_idx` back to that snapshot. An old follower gives the leader nothing to check that
    // assumption against.
    ulong snp_idx = fx.leader_snp->get_last_log_idx();
    reset_peer_for_arming(s1, 3, fx.safe_next_log_idx);
    CHK_Z( arm_pending_resp(s1, "S3") );
    set_peer_cursor(s1, 3, next_log_idx, matched_idx, floor_idx);
    s1.fNet->setPeerSnapshotInSync(s1.raftServer.get(), 3, fx.leader_snp);
    CHK_Z( deliver_install_snapshot_resp(
               s1, "S3", 3, fx.term, true, 1,
               make_snp_install_done_ctx(0, 0, 1)) );

    CHK_EQ( snp_idx, s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), 3) );
    CHK_EQ( snp_idx + 1, s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
    CHK_FALSE( s1.fNet->hasPeerSnapshotSyncCtx(s1.raftServer.get(), 3) );

    print_stats(fx.pkgs);
    shutdown_late_ack_fixture(fx);
    return 0;
}

// Case 6: a payload that is neither the historical marker nor a well-formed extended frame is
// rejected outright. Falling back to "treat it as the old done-marker" would be the *permissive*
// reading, because in this code the old marker means completion -- so a malformed acknowledgement
// would complete whichever install happened to be live.
int late_snapshot_install_ack_malformed_test() {
    LateAckFixture fx;
    CHK_Z( setup_late_ack_fixture(fx, /*use_bg_thread_for_snapshot_io=*/false) );
    RaftPkg& s1 = *fx.s1;

    ulong idx_j = fx.leader_snp->get_last_log_idx();
    ulong next_log_idx = fx.safe_next_log_idx;
    ulong matched_idx = next_log_idx - 1;
    ulong floor_idx = next_log_idx - 2;

    struct Malformed {
        const char* what;
        uint8_t tag;
        size_t len;
    };
    std::vector<Malformed> cases = {
        // Long enough to hold an index but tagged as the indexless legacy payload.
        { "legacy tag with an index-sized payload", 0, 9 },
        // Claims an index it is too short to contain.
        { "index tag with a truncated payload", 1, 5 },
    };

    for (auto& bad: cases) {
        for (int with_live_ctx = 0; with_live_ctx <= 1; ++with_live_ctx) {
            _msg("  case: %s, live context: %s\n",
                 bad.what, with_live_ctx ? "yes" : "no");

            reset_peer_for_arming(s1, 3, fx.safe_next_log_idx);
            CHK_Z( arm_pending_resp(s1, "S3") );

            set_peer_cursor(s1, 3, next_log_idx, matched_idx, floor_idx);
            CHK_Z( expire_snapshot_sync_ctx(s1, 3, fx.leader_snp) );
            if (with_live_ctx) {
                s1.fNet->setPeerSnapshotInSync(
                    s1.raftServer.get(), 3, fx.leader_snp);
            }

            CHK_Z( deliver_install_snapshot_resp(
                       s1, "S3", 3, fx.term, true, 1,
                       make_snp_install_done_ctx(bad.tag, idx_j, bad.len)) );

            CHK_Z( check_peer_cursor(
                       s1, 3, next_log_idx, matched_idx, floor_idx) );

            if (with_live_ctx) {
                // The live transfer survives an unreadable response.
                CHK_TRUE( s1.fNet->hasPeerSnapshotSyncCtx(
                              s1.raftServer.get(), 3) );
                CHK_EQ( idx_j,
                        s1.fNet->getPeerSnapshotSyncCtxLastLogIdx(
                            s1.raftServer.get(), 3) );
            }
        }
    }

    print_stats(fx.pkgs);
    shutdown_late_ack_fixture(fx);
    return 0;
}

// Case 7: the index sits at a fixed offset for every tag `>= 1`, so a leader can read it out of a
// format it does not otherwise understand. That keeps later extensions of this payload additive
// instead of being rejected by older peers.
int late_snapshot_install_ack_future_format_test() {
    LateAckFixture fx;
    CHK_Z( setup_late_ack_fixture(fx, /*use_bg_thread_for_snapshot_io=*/false) );
    RaftPkg& s1 = *fx.s1;

    ulong snp_idx = fx.leader_snp->get_last_log_idx();

    for (size_t len: {size_t(9), size_t(17)}) {
        _msg("  case: unknown tag 2, payload length %zu\n", len);

        reset_peer_for_arming(s1, 3, fx.safe_next_log_idx);
        CHK_Z( arm_pending_resp(s1, "S3") );

        set_peer_cursor(s1, 3, 1, 0, 0);
        CHK_Z( expire_snapshot_sync_ctx(s1, 3, fx.leader_snp) );
        CHK_Z( deliver_install_snapshot_resp(
                   s1, "S3", 3, fx.term, true, 1,
                   make_snp_install_done_ctx(2, snp_idx, len)) );

        CHK_EQ( snp_idx + 1,
                s1.fNet->getPeerNextLogIdx(s1.raftServer.get(), 3) );
        CHK_EQ( snp_idx, s1.fNet->getPeerMatchedIdx(s1.raftServer.get(), 3) );
        CHK_EQ( snp_idx + 1,
                s1.fNet->getPeerNextLogIdxFloor(s1.raftServer.get(), 3) );
    }

    print_stats(fx.pkgs);
    shutdown_late_ack_fixture(fx);
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

    ts.doTest( "stale snapshot finalization role change test",
               stale_snapshot_finalization_role_change_test );

    ts.doTest( "snapshot stale sync flag test",
               snapshot_stale_sync_flag_test );
    ts.doTest( "snapshot async request state lifecycle test",
               snapshot_async_request_state_lifecycle_test );
    ts.doTest( "async snapshot does not reselect snapshot at offset zero test",
               async_snapshot_does_not_reselect_snapshot_at_offset_zero_test );
    ts.doTest( "active async snapshot context not cleared by caught up branch test",
               active_async_snapshot_context_not_cleared_by_caught_up_branch_test );

    ts.doTest( "snapshot null snapshot join test",
               snapshot_null_snapshot_join_test );

    ts.doTest( "snapshot append rejection compacted boundary test",
               snapshot_append_rejection_compacted_boundary_test );

    ts.doTest( "append rejection unmarked ahead next index test",
               append_rejection_unmarked_ahead_next_index_test );

    ts.doTest( "append rejection backward probe throttle test",
               append_rejection_backward_probe_throttle_test );

    ts.doTest( "append rejection backward probe throttle disabled test",
               append_rejection_backward_probe_throttle_disabled_test );

    ts.doTest( "append rejection stale term probe reset test",
               append_rejection_stale_term_probe_reset_test );

    ts.doTest( "append rejection invalid compacted boundary marker test",
               append_rejection_invalid_compacted_boundary_marker_test );

    ts.doTest( "append rejection stale compacted boundary marker test",
               append_rejection_stale_compacted_boundary_marker_test );

    ts.doTest( "append rejection do not rewind ahead next index test",
               append_rejection_do_not_rewind_ahead_next_index_test );

    ts.doTest( "append rejection unknown extra order probe reset test",
               append_rejection_unknown_extra_order_probe_reset_test );

    ts.doTest( "snapshot rewind floor test",
               snapshot_rewind_floor_test );

    ts.doTest( "snapshot user ctx deferred free test",
               snapshot_user_ctx_deferred_free_test );

    ts.doTest( "late snapshot install ack accepted test",
               late_snapshot_install_ack_accepted_test );

    ts.doTest( "late snapshot install ack monotonic test",
               late_snapshot_install_ack_monotonic_test );

    ts.doTest( "late snapshot install ack no reinstall test",
               late_snapshot_install_ack_no_reinstall_test );

    ts.doTest( "late snapshot install ack validation test",
               late_snapshot_install_ack_validation_test );

    ts.doTest( "late snapshot install ack wrong snapshot test",
               late_snapshot_install_ack_wrong_snapshot_test );

    ts.doTest( "late snapshot install ack legacy marker test",
               late_snapshot_install_ack_legacy_marker_test );

    ts.doTest( "late snapshot install ack malformed test",
               late_snapshot_install_ack_malformed_test );

    ts.doTest( "late snapshot install ack future format test",
               late_snapshot_install_ack_future_format_test );

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
