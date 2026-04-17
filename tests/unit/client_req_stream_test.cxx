/************************************************************************
Copyright 2024-present ClickHouse, Inc.

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

// Local-path unit tests for client_req_stream. The remote (STREAM_FORWARDING
// wire) path is covered by the ClickHouse Keeper integration tests.

#include "fake_network.hxx"
#include "raft_package_fake.hxx"
#include "fake_executer.hxx"

#include "client_req_stream.hxx"
#include "cluster_config.hxx"
#include "test_common.h"

using namespace nuraft;
using namespace raft_functional_common;

namespace client_req_stream_test {

namespace {

// Start a 3-node group with a leader on S1, optionally flipping every node's
// runtime cluster_config to async_replication afterward.
int launch_group_with_async_replication(std::vector<RaftPkg*>& pkgs,
                                        bool async_replication) {
    CHK_Z(launch_servers(pkgs));
    CHK_Z(make_group(pkgs));
    if (async_replication) {
        for (auto& pkg : pkgs) {
            pkg->raftServer->get_config()->set_async_replication(true);
        }
    }
    return 0;
}

} // anonymous namespace

int open_rejects_sync_replication() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z(launch_group_with_async_replication(pkgs, /*async_replication=*/false));

    // Sync replication (default) → factory must refuse with CANCELLED.
    cmd_result_code err = cmd_result_code::OK;
    ptr<client_req_stream> stream =
        s1.raftServer->open_client_req_stream(/*send_timeout_ms=*/0, &err);
    CHK_NULL(stream.get());
    CHK_EQ(cmd_result_code::CANCELLED, err);

    // Flip the runtime config to async and confirm a stream can now be opened.
    ptr<cluster_config> rt = s1.raftServer->get_config();
    rt->set_async_replication(true);
    // get_config() returns a shared config object, so no additional plumbing is
    // needed for the leader to observe the change.
    stream = s1.raftServer->open_client_req_stream();
    CHK_NONNULL(stream.get());
    CHK_FALSE(stream->is_broken());

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    f_base->destroy();
    return 0;
}

int local_stream_append_happy_path() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z(launch_group_with_async_replication(pkgs, /*async_replication=*/true));

    ptr<client_req_stream> stream = s1.raftServer->open_client_req_stream();
    CHK_NONNULL(stream.get());
    CHK_FALSE(stream->is_broken());

    std::string test_msg = "payload";
    ptr<buffer> msg = buffer::alloc(test_msg.size() + 1);
    msg->put(test_msg);

    std::vector<ptr<buffer>> logs{msg};
    ptr<cmd_result<ptr<buffer>>> res = stream->append(std::move(logs));
    CHK_NONNULL(res.get());
    CHK_TRUE(res->get_accepted());
    CHK_FALSE(stream->is_broken());

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    f_base->destroy();
    return 0;
}

int append_empty_batch_is_no_op() {
    reset_log_files();
    ptr<FakeNetworkBase> f_base = cs_new<FakeNetworkBase>();

    RaftPkg s1(f_base, 1, "S1");
    RaftPkg s2(f_base, 2, "S2");
    RaftPkg s3(f_base, 3, "S3");
    std::vector<RaftPkg*> pkgs = {&s1, &s2, &s3};

    CHK_Z(launch_group_with_async_replication(pkgs, /*async_replication=*/true));

    ptr<client_req_stream> stream = s1.raftServer->open_client_req_stream();
    CHK_NONNULL(stream.get());

    // Empty batches short-circuit to accepted=true, OK without touching
    // append_entries_ext (which would report accepted=false, OK for empty).
    std::vector<ptr<buffer>> empty;
    ptr<cmd_result<ptr<buffer>>> res = stream->append(std::move(empty));
    CHK_NONNULL(res.get());
    CHK_TRUE(res->has_result());
    CHK_TRUE(res->get_accepted());
    CHK_EQ(cmd_result_code::OK, res->get_result_code());
    CHK_FALSE(stream->is_broken());

    s1.raftServer->shutdown();
    s2.raftServer->shutdown();
    s3.raftServer->shutdown();
    f_base->destroy();
    return 0;
}

} // namespace client_req_stream_test

using namespace client_req_stream_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);
    ts.options.printTestMessage = true;

    ts.doTest("open rejects sync replication",
              open_rejects_sync_replication);
    ts.doTest("local stream append happy path",
              local_stream_append_happy_path);
    ts.doTest("append empty batch is no-op",
              append_empty_batch_is_no_op);

    return 0;
}
