# Repository-wide write barrier decision

## Decision

Do not add a repository-wide write barrier to the 0.1 runtime.

The current checkpoint is a per-generation consistency inventory. Each JSONL
generation is fingerprinted under its stable writer lease, but discovery and
fingerprinting occur over time. Concurrent writes therefore appear during later
verification as `CHANGED`, `MISSING`, or `ADDED`. Independent-JVM stress covers
this contract.

## Why a barrier is deferred

A true point-in-time repository snapshot requires every mutation path to join a
single cross-process protocol:

- create and delete
- append and transaction
- fork and retained copy
- compaction and navigation settlement
- run/tool/queue settlement
- cleanup and administrative publication

A shared/exclusive global file lock is simple but makes checkpoint hashing hold
an exclusive repository epoch for potentially unbounded time. It blocks every
session even though generations otherwise commit independently. Lock ordering
would have to be global barrier, session-ID lease, operation lease, then
generation writer lease everywhere to avoid deadlock.

An epoch or copy-on-write protocol reduces pause time but changes the durable
layout and requires crash recovery for epoch publication. Filesystem snapshots
can provide stronger semantics operationally but are platform-specific and
outside the JSONL repository contract.

## Trigger for reconsideration

Reconsider only when a concrete requirement needs all generations at one
logical instant, such as regulated audit export or transactionally consistent
backup without filesystem snapshot support. The proposal must specify:

1. The exact snapshot isolation contract.
2. Maximum acceptable writer pause and repository size.
3. Cross-JVM lock ordering and crash recovery.
4. Interaction with create/delete and generation tombstones.
5. Backward compatibility for processes that do not understand the barrier.
6. Independent-JVM kill tests for every publication phase.

Until then, checkpoint verification plus drift reporting is the supported
administrative workflow and avoids imposing repository-wide contention on
normal agent execution.
