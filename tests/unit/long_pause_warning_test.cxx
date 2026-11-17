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

#include "fake_network.hxx"
#include "raft_package_fake.hxx"

#include "raft_params.hxx"
#include "test_common.h"

#include <chrono>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

using namespace nuraft;
using namespace raft_functional_common;

namespace long_pause_warning_test {

// Number of busy checks (leader appends while the follower is stalled)
// in the first stall episode. Must be greater than `warning_limit_` (20),
// so that the warning gets suppressed within the first episode.
static const size_t STALL_APPENDS = 30;

// Counts `needle` lines below the marker line: slow test machines
// (e.g. under TSAN) may already emit a few long pause warnings during
// cluster setup, which are not the subject of the assertions.
static size_t count_log_msg_after_marker(int srv_id,
                                         const std::string& marker,
                                         const std::string& needle) {
    std::string file_name = "./srv" + std::to_string(srv_id) + ".log";
    std::ifstream file(file_name);
    size_t count = 0;
    bool after_marker = false;
    std::string line;
    while (std::getline(file, line)) {
        if (!after_marker) {
            if (line.find(marker) != std::string::npos) after_marker = true;
            continue;
        }
        if (line.find(needle) != std::string::npos) count++;
    }
    return count;
}

// The trailing separators keep `to 2` from matching peer ids like 20+.
static const std::string MARKER = "== long pause warning test: stall phase ==";

static size_t count_skipped_msgs(int leader_id) {
    return count_log_msg_after_marker(
        leader_id, MARKER, "skipped sending msg to 2 ");
}

static size_t count_suppressed_msgs(int leader_id) {
    return count_log_msg_after_marker(
        leader_id, MARKER, "long pause warning to 2 is too verbose");
}

static size_t count_recovered_msgs(int leader_id) {
    return count_log_msg_after_marker(
        leader_id, MARKER, "recovered from long pause to peer 2, ");
}

static void append_one(RaftPkg& leader, const std::string& msg_text) {
    ptr<buffer> msg = buffer::alloc(msg_text.size() + 1);
    msg->put(msg_text);
    leader.raftServer->append_entries( {msg} );
}

// Fully serves one pending leader-to-follower message (request + response).
static void serve_fast_follower(RaftPkg& leader, const std::string& endpoint) {
    leader.fNet->execReqResp(endpoint);
    leader.fNet->execReqResp(endpoint);
}

// Cluster with a 1 ms heartbeat interval, so that the long pause warning
// triggers after a couple of milliseconds of real stall.
static int launch_cluster(std::vector<RaftPkg*>& pkgs) {
    raft_params params;
    params.with_election_timeout_lower(0);
    params.with_election_timeout_upper(10000);
    params.with_hb_interval(1);
    params.with_client_req_timeout(1000000);
    params.with_reserved_log_items(0);
    params.with_snapshot_enabled(5);
    params.with_log_sync_stopping_gap(1);
    // Otherwise `append_entries` blocks waiting for the commit, which is
    // held back by the stalled follower on purpose.
    params.return_method_ = raft_params::async_handler;

    CHK_Z( launch_servers(pkgs, &params) );
    CHK_Z( make_group(pkgs) );
    return 0;
}

// One stall episode: the leader sends one request to the follower and
// withholds the response for `num_appends * wait_ms` of real time, while
// appending (and fully serving the other follower) in a loop.
static void stall_episode(RaftPkg& s1,
                          const std::string& s2_addr,
                          const std::string& s3_addr,
                          size_t num_appends,
                          size_t wait_ms,
                          const std::string& prefix) {
    append_one(s1, prefix + ":open");
    serve_fast_follower(s1, s3_addr);
    // Request only: the leader considers the follower busy from now on.
    s1.fNet->delieverReqTo(s2_addr);
    for (size_t ii = 0; ii < num_appends; ++ii) {
        append_one(s1, prefix + std::to_string(ii));
        serve_fast_follower(s1, s3_addr);
        std::this_thread::sleep_for( std::chrono::milliseconds(wait_ms) );
    }
    // The response finally arrives, later than the heartbeat interval:
    // it is a slow response and must not reset the warning counter.
    s1.fNet->handleRespFrom(s2_addr);
}

// A slow-but-alive follower must not re-arm the long pause warning burst
// on every response. All slow episodes together are allowed only one
// burst of `warning_limit_` warnings, until the peer really recovers.
int slow_peer_does_not_refill_warning_budget_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_cluster(pkgs) );

    // Warm-up: fully replicate one entry.
    append_one(s1, "warm");
    serve_fast_follower(s1, s2_addr);
    serve_fast_follower(s1, s3_addr);
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    s1.localLog(MARKER);

    // The first episode is long enough to hit the warning limit (20)
    // and get suppressed. Five more slow episodes follow: before the
    // fix each of them produced a fresh burst of warnings, since every
    // response reset the counter.
    stall_episode(s1, s2_addr, s3_addr, STALL_APPENDS, 2, "long-");
    for (size_t ep = 0; ep < 5; ++ep) {
        stall_episode(s1, s2_addr, s3_addr, 5, 2,
                      "ep" + std::to_string(ep) + "-");
    }

    // Recovery: the follower is fast again, its first fast response
    // resets the counter and reports the recovery exactly once.
    for (size_t ii = 0; ii < 6; ++ii) {
        append_one(s1, "fast" + std::to_string(ii));
        serve_fast_follower(s1, s2_addr);
        serve_fast_follower(s1, s3_addr);
        std::this_thread::sleep_for( std::chrono::milliseconds(1) );
    }
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();

    // Flush the leader's log file and count the messages.
    s1.myLogWrapper->destroy();
    size_t skipped = count_skipped_msgs(1);
    size_t suppressed = count_suppressed_msgs(1);
    size_t recovered = count_recovered_msgs(1);

    // The burst is `warning_limit_ - 1` "skipped sending" warnings
    // (the increment up to the limit prints the suppression message)
    // plus that single suppression message. Slow episodes after the
    // first one do not add anything: the counter is not reset by a
    // slow response. The upper bound tolerates a few benign warnings
    // while the recovered follower digests the accumulated backlog in
    // one heavy request (visible on slow machines, e.g. under TSAN);
    // a counter refill would add another full burst (>= 19).
    CHK_GTEQ( skipped, 19 );
    CHK_SMEQ( skipped, 24 );
    CHK_EQ( 1, suppressed );
    CHK_EQ( 1, recovered );

    return 0;
}

// A zero re-arming interval must mean "never re-arm": after one burst
// the warning stays suppressed for as long as the peer is slow.
int zero_rearm_interval_disables_rearm_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_cluster(pkgs) );

    raft_server::limits saved_limits = raft_server::get_raft_limits();
    raft_server::limits limits = saved_limits;
    limits.warning_rearm_interval_ms_ = 0;
    raft_server::set_raft_limits(limits);

    // Warm-up: fully replicate one entry.
    append_one(s1, "warm");
    serve_fast_follower(s1, s2_addr);
    serve_fast_follower(s1, s3_addr);
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    s1.localLog(MARKER);

    // A long continuous stall: far beyond the burst, still no second burst.
    stall_episode(s1, s2_addr, s3_addr, 60, 2, "zero-");
    raft_server::set_raft_limits(saved_limits);

    for (size_t ii = 0; ii < 6; ++ii) {
        append_one(s1, "fast" + std::to_string(ii));
        serve_fast_follower(s1, s2_addr);
        serve_fast_follower(s1, s3_addr);
        std::this_thread::sleep_for( std::chrono::milliseconds(1) );
    }
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();

    s1.myLogWrapper->destroy();
    size_t skipped = count_skipped_msgs(1);
    size_t suppressed = count_suppressed_msgs(1);

    // Same as in the first test: exactly one burst; the upper bound
    // tolerates benign backlog-digestion warnings on slow machines.
    CHK_GTEQ( skipped, 19 );
    CHK_SMEQ( skipped, 24 );
    CHK_EQ( 1, suppressed );

    return 0;
}

// While a follower stays slow, the suppressed warning re-arms at most
// once per `warning_rearm_interval_ms_`, so that a persistently degraded
// peer keeps producing a signal without flooding the log.
int suppressed_warning_rearms_test() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    std::string s2_addr = "S2";
    std::string s3_addr = "S3";

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, s2_addr);
    RaftPkg s3(f_base, 3, s3_addr);
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z( launch_cluster(pkgs) );

    raft_server::limits saved_limits = raft_server::get_raft_limits();
    raft_server::limits limits = saved_limits;
    limits.warning_rearm_interval_ms_ = 20;
    raft_server::set_raft_limits(limits);

    // Warm-up: fully replicate one entry.
    append_one(s1, "warm");
    serve_fast_follower(s1, s2_addr);
    serve_fast_follower(s1, s3_addr);
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );
    s1.localLog(MARKER);

    // One continuous stall: ~60 appends * 4 ms = ~240 ms, i.e. room for
    // the first burst, a re-armed burst, and a partial third one.
    stall_episode(s1, s2_addr, s3_addr, 60, 4, "rearm-");
    raft_server::set_raft_limits(saved_limits);

    // Let the follower recover and flush everything.
    for (size_t ii = 0; ii < 6; ++ii) {
        append_one(s1, "fast" + std::to_string(ii));
        serve_fast_follower(s1, s2_addr);
        serve_fast_follower(s1, s3_addr);
        std::this_thread::sleep_for( std::chrono::milliseconds(1) );
    }
    CHK_Z( wait_for_sm_exec(pkgs, COMMIT_TIMEOUT_SEC) );

    for (RaftPkg* pkg: pkgs) pkg->raftServer->shutdown();
    f_base->destroy();

    s1.myLogWrapper->destroy();
    size_t skipped = count_skipped_msgs(1);
    size_t suppressed = count_suppressed_msgs(1);

    // More than one burst (otherwise the peer would be muted forever),
    // but bounded: a commit may invoke the busy check more than once per
    // append, still far less than one warning per check.
    CHK_GTEQ( skipped, 40 );
    CHK_SMEQ( skipped, 110 );
    CHK_GTEQ( suppressed, 2 );

    return 0;
}

} // namespace long_pause_warning_test;
using namespace long_pause_warning_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = false;

    ts.doTest( "slow peer does not refill warning budget",
               slow_peer_does_not_refill_warning_budget_test );

    ts.doTest( "suppressed warning re-arms",
               suppressed_warning_rearms_test );

    ts.doTest( "zero re-arm interval disables re-arm",
               zero_rearm_interval_disables_rearm_test );

    return 0;
}
