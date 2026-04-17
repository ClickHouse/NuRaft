Pipelined Client Request Forwarding
====================================

`client_req_stream` is a single-channel, ordered, fail-closed forwarding
path from any raft node to the current leader. It exists to send many
dependent client writes without waiting for commit between them, at the
cost of tearing the stream down on any ambiguity.

Obtain via `raft_server::open_client_req_stream`; see
`include/libnuraft/client_req_stream.hxx` for the API.

Semantics
---------

* **Ordering.** Across `append` calls on one stream, order is preserved
  end-to-end. If a batch commits, every previously-sent batch on the same
  stream committed too with a lower `log_idx`. Guarantee: some prefix of
  the sent sequence is applied, no gaps.

* **Fail-closed.** Any stream-fatal outcome (leader rejection, transport
  error, local leader-term divergence) breaks the stream permanently; the
  caller discards it and opens a new one. The failing `append` surfaces
  the leader's `result_code`; only subsequent sends observe the transport
  break.

* **Term fencing.** The stream snapshots leader id + term at open. Each
  forwarded request carries that term so the leader rejects on mismatch.
  `is_broken()` also returns true as soon as the local `raft_server`'s
  term diverges from the fenced term.

* **Cheap local path.** When the leader is this node, the stream is a
  thin wrapper over `append_entries_ext` with `expected_term_` set.

Preconditions
-------------

* **`async_replication` + transport pipelining** must both be on. The
  cluster config must have `async_replication` enabled (otherwise the
  leader serializes responses behind `client_req_timeout_` and
  pipelining buys nothing), and the local `asio_service` must have
  `streaming_mode_` enabled (otherwise overlapping sends hit a
  busy-socket assert). `open_client_req_stream` returns nullptr if
  either is missing.

* **All cluster peers must understand `STREAM_FORWARDING`** (remote path
  only). Not runtime-checked — mixed-version rollout is a consciously
  accepted unsupported mode for this feature. A per-feature ACK bit was
  considered and declined: it would reinvent capability negotiation
  per-feature, which is the wrong abstraction for NuRaft. The correct
  long-term answer is a general peer-capability handshake, which is
  outside the scope of this change. Until that exists, this feature
  follows the same deployment discipline as `async_replication` did:
  precondition + operator runbook, not runtime enforcement. See the
  Operator Runbook below for the rollout procedure.

* **In-flight queue is unbounded.** `rpc_client` does not limit queued
  sends; a faster-than-leader producer will grow the queue without
  back-pressure. Bound it in the caller.

* **Don't combine with `cb_func` hooks that use `ReturnNull` to cancel
  client writes.** If `PreAppendLogLeader`, `AppendLogs`, or
  `AppendLogFailed` returns `ReturnNull` mid-batch on a stream-forwarded
  request, the leader closes the rpc session without a response after a
  prefix has already been stored. On retry the client may re-submit the
  same batch and duplicate the committed prefix — the no-gap guarantee
  does not hold against this path. If you need these hooks, dedup at
  the state-machine layer or don't use stream forwarding. (Conservative:
  some technically-safe cases exist — e.g. `PreAppendLogLeader`
  returning `ReturnNull` at `i == 0` before any entry is stored — but
  treat "any hook + `ReturnNull` + stream forwarding" as unsupported
  unless you've read `src/handle_client_request.cxx`.)

Operator Runbook — rolling upgrades
-----------------------------------

Because the peer-version precondition is not runtime-checked, a rollout
that introduces the `STREAM_FORWARDING` wire flag needs a two-phase
ordering: upgrade every node in the cluster first, *then* allow any
caller to open streams. If a stream opens against a pre-flag peer, the
old leader silently drops the flag and term fencing doesn't apply; a
subsequent stream break can leave a committed prefix that a retry then
duplicates.

The failure mode is **duplicated commits in the log, not data loss**.
If the state machine dedups client operations (session + request
counter or similar), the window is harmless. Otherwise you'll see a
request apply twice in or just after the upgrade window; reconcile from
the raft log, or accept the duplicates and wait out the upgrade.

Wire flag (internal)
--------------------

`req_msg::STREAM_FORWARDING_REQUEST` is serialized as `STREAM_FORWARDING`
(0x80) in the asio header. On the leader:

1. `req_msg::term` is treated as `expected_term_` (see the WARNING in
   `req_msg.hxx`); without the flag it is ignored.
2. On rejection, `rpc_session` writes the response back then tears the
   session down, so the failing `append` sees the real `result_code`
   and subsequent sends see transport break.

No-gap invariant
----------------

The close-on-reject check looks only at `resp->get_accepted()` (not the
cb/async_cb chain). Safe because `handle_cli_req` routes every
`accepted=false` return through `reject_cli_req` above the store loop —
so after the first `store_log_entry` the response is always accepted,
and any later failure (CANCELLED/TIMEOUT) can only come from a term
change, which rolls back all uncommitted entries of the old term
together.

`cb_func` hooks that return `ReturnNull` mid-batch are a separate
hazard — not protected by this structural invariant. See the
precondition in the Preconditions section above.
