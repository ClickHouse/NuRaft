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

#pragma once

#include "nuraft.hxx"

#include "peer.hxx"
#include "raft_server_handler.hxx"

#include <map>
#include <unordered_map>

class SimpleLogger;

namespace nuraft {

class FakeClient;
class FakeNetworkBase;
class FakeNetwork
    : public raft_server_handler
    , public rpc_client_factory
    , public rpc_listener
    , public std::enable_shared_from_this<FakeNetwork>
{
public:
    FakeNetwork(const std::string& _endpoint,
                ptr<FakeNetworkBase>& _base);

    struct ReqPkg {
        ReqPkg(ptr<req_msg>& _req, rpc_handler& _when_done)
            : req(_req), whenDone(_when_done)
            {}
        ptr<req_msg> req;
        rpc_handler whenDone;
    };

    struct RespPkg {
        RespPkg(ptr<resp_msg>& _resp, rpc_handler& _when_done)
            : resp(_resp), whenDone(_when_done)
            {}
        ptr<resp_msg> resp;
        rpc_handler whenDone;
    };

    FakeNetworkBase* getBase() const { return base.get(); }

    std::string getEndpoint() const { return myEndpoint; }

    ptr<rpc_client> create_client(const std::string& endpoint);

    void listen(ptr<msg_handler>& handler);

    ptr<resp_msg> gotMsg(ptr<req_msg>& msg);

    bool execReqResp(const std::string& endpoint = std::string());

    ptr<FakeClient> findClient(const std::string& endpoint);

    bool delieverReqTo(const std::string& endpoint,
                       bool random_order = false);

    // Similar to `deliverReqTo`, but delivers request from the stale connections.
    // The response as a result of the request will be ignored.
    bool delieverStaleReqTo(const std::string& endpoint);

    void delieverAllTo(const std::string& endpoint);

    bool makeReqFail(const std::string& endpoint,
                     bool random_order = false);

    void makeReqFailAll(const std::string& endpoint);

    bool handleRespFrom(const std::string& endpoint,
                        bool random_order = false);

    void handleAllFrom(const std::string& endpoint);

    size_t getNumPendingReqs(const std::string& endpoint);

    size_t getNumPendingResps(const std::string& endpoint);

    ptr<req_msg> getFirstPendingReq(const std::string& endpoint);

    void goesOffline() { online = false; }

    void goesOnline() { online =  true; }

    void dropPeerConnection(raft_server* srv, int peer_id) {
        auto& peers = get_peers(srv);
        auto entry = peers.find(peer_id);
        if (entry != peers.end()) {
            entry->second->reset_rpc();
        }
    }

    bool isOnline() const { return online; }

    void stop();

    void shutdown();

    void setPeerSnapshotSyncNeeded(raft_server* srv,
                                   int32 peer_id,
                                   bool val) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            it->second->set_snapshot_sync_is_needed(val);
        }
    }

    bool getPeerSnapshotSyncNeeded(raft_server* srv,
                                   int32 peer_id) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            return it->second->is_snapshot_sync_needed();
        }
        return false;
    }

    void clearLastSnapshot(raft_server* srv) {
        clear_last_snapshot(srv);
    }

    bool isServerOutOfLogRange(raft_server* srv) {
        return is_out_of_log_range(srv);
    }

    bool hasPeerSnapshotSyncCtx(raft_server* srv, int32 peer_id) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            return it->second->get_snapshot_sync_ctx() != nullptr;
        }
        return false;
    }

    ulong getPeerNextLogIdxFloor(raft_server* srv, int32 peer_id) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            return it->second->get_next_log_idx_floor();
        }
        return 0;
    }

    void setPeerNextLogIdx(raft_server* srv, int32 peer_id, ulong idx) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            it->second->set_next_log_idx(idx);
        }
    }

    void setPeerMatchedIdx(raft_server* srv, int32 peer_id, ulong idx) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            it->second->set_matched_idx(idx);
        }
    }

    ulong getPeerMatchedIdx(raft_server* srv, int32 peer_id) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            return it->second->get_matched_idx();
        }
        return 0;
    }

    void setPeerLastAcceptedLogIdx(raft_server* srv, int32 peer_id, ulong idx) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            it->second->set_last_accepted_log_idx(idx);
        }
    }

    ulong getPeerLastAcceptedLogIdx(raft_server* srv, int32 peer_id) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            return it->second->get_last_accepted_log_idx();
        }
        return 0;
    }

    void setPeerNextLogIdxFloor(raft_server* srv, int32 peer_id, ulong idx) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            it->second->set_next_log_idx_floor(idx);
        }
    }

    ulong getPeerNextLogIdx(raft_server* srv, int32 peer_id) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            return it->second->get_next_log_idx();
        }
        return 0;
    }

    int32 getPeerBackwardLogProbeCount(raft_server* srv, int32 peer_id) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            return it->second->get_cnt_backward_log_probe();
        }
        return 0;
    }

    void setPeerBackwardLogProbeCount(raft_server* srv,
                                      int32 peer_id,
                                      int32 value) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            it->second->reset_cnt_backward_log_probe();
            for (int32 ii = 0; ii < value; ++ii) {
                it->second->inc_cnt_backward_log_probe();
            }
        }
    }

    bool replaceLastPendingResp(const std::string& endpoint,
                                ptr<resp_msg> new_resp);

    void setPeerSnapshotInSync(raft_server* srv,
                               int32 peer_id,
                               ptr<snapshot> snp) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            it->second->set_snapshot_in_sync(snp);
        }
    }

    ulong getPeerSnapshotSyncCtxLastLogIdx(raft_server* srv, int32 peer_id) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            ptr<snapshot_sync_ctx> sync_ctx = it->second->get_snapshot_sync_ctx();
            if (sync_ctx && sync_ctx->get_snapshot()) {
                return sync_ctx->get_snapshot()->get_last_log_idx();
            }
        }
        return 0;
    }

    void beginPeerSnapshotAsyncRequest(raft_server* srv, int32 peer_id) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            ptr<snapshot_sync_ctx> sync_ctx = it->second->get_snapshot_sync_ctx();
            if (sync_ctx) {
                sync_ctx->begin_async_snapshot_request();
            }
        }
    }

    bool getPeerAsyncSnapshotRequestInProgress(raft_server* srv, int32 peer_id) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            ptr<snapshot_sync_ctx> sync_ctx = it->second->get_snapshot_sync_ctx();
            return sync_ctx && sync_ctx->is_async_snapshot_request_in_progress();
        }
        return false;
    }

    bool getPeerAsyncSnapshotTransferStarted(raft_server* srv, int32 peer_id) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            ptr<snapshot_sync_ctx> sync_ctx = it->second->get_snapshot_sync_ctx();
            return sync_ctx && sync_ctx->is_async_snapshot_transfer_started();
        }
        return false;
    }

    ptr<snapshot> getLastSnapshot(raft_server* srv) {
        return get_last_snapshot(srv);
    }

    void setLastSnapshot(raft_server* srv, ptr<snapshot> snp) {
        set_last_snapshot(srv, snp);
    }

    ptr<req_msg> createAppendEntriesReq(raft_server* srv,
                                        int32 peer_id,
                                        ulong custom_last_log_idx = 0) {
        auto& peers = get_peers(srv);
        auto it = peers.find(peer_id);
        if (it != peers.end()) {
            return create_append_entries_req(srv, it->second, custom_last_log_idx);
        }
        return nullptr;
    }

private:
    std::string myEndpoint;
    ptr<FakeNetworkBase> base;
    ptr<msg_handler> handler;
    // NOTE: We don't use `unordered_map` as the order of traversal
    //       will be different according to platforms. We should make
    //       the test deterministic.
    std::map< std::string, ptr<FakeClient> > clients;
    std::mutex clientsLock;
    std::list< ptr<FakeClient> > staleClients;
    bool online;
};

class FakeNetworkBase {
public:
    FakeNetworkBase();

    ~FakeNetworkBase() { destroy(); }

    void destroy();

    void addNetwork(ptr<FakeNetwork>& net);

    void removeNetwork(const std::string& endpoint);

    FakeNetwork* findNetwork(const std::string& endpoint);

    SimpleLogger* getLogger() const { return myLog; }

private:
    // <endpoint, network instance>
    std::map<std::string, ptr<FakeNetwork>> nets;

    SimpleLogger* myLog;
};

class FakeClient : public rpc_client {
    friend class FakeNetwork;
public:
    FakeClient(FakeNetwork* mother,
               FakeNetwork* dst);

    ~FakeClient();

    void send(ptr<req_msg>& req, rpc_handler& when_done, uint64_t send_timeout_ms = 0);

    void dropPackets();

    bool isDstOnline();

    uint64_t get_id() const;

    bool is_abandoned() const;

private:
    uint64_t myId;
    FakeNetwork* motherNet;
    FakeNetwork* dstNet;
    std::list<FakeNetwork::ReqPkg> pendingReqs;
    std::list<FakeNetwork::RespPkg> pendingResps;
};

class FakeTimer : public delayed_task_scheduler {
public:
    FakeTimer(const std::string& endpoint,
              SimpleLogger* logger = nullptr);

    void schedule(ptr<delayed_task>& task, int32 milliseconds);

    void cancel(ptr<delayed_task>& task);

    void invoke(int type);

    size_t getNumPendingTasks(int type = -1);

private:
    void cancel_impl(ptr<delayed_task>& task);

    std::string myEndpoint;

    std::mutex tasksLock;

    std::list< ptr<delayed_task> > tasks;

    SimpleLogger* myLog;
};

}  // namespace nuraft;
