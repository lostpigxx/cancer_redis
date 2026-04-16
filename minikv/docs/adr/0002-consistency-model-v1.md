# ADR 0002: Consistency Model V1

## Status

Accepted on 2026-04-16.

## Context

This ADR documents the consistency model that the current `minikv`
implementation actually provides. It is scoped to the current command set:
`PING`, `HSET`, `HGETALL`, and `HDEL`.

## Decision

The current consistency model is intentionally narrow and should be read as a
baseline contract, not as a general Redis-compatible transaction model.

### Same-Key Consistency Depends On `KeyLockTable`

For commands with a route key, correctness for same-key concurrency depends on
`KeyLockTable` in the worker layer:

- `HSET`, `HGETALL`, and `HDEL` all route on the user key
- worker threads acquire a striped mutex derived from that key before executing
  the command
- same-key requests therefore execute serially even when different workers pick
  them up

This means current same-key consistency is provided by keyed execution
serialization, not by RocksDB transactions or multi-statement snapshots.

### Reads And Writes Are Not Snapshot-Isolated

Current reads do not use RocksDB snapshots:

- `HGETALL` reads metadata and then scans the `hash` column family with normal
  `ReadOptions`
- no snapshot handle is acquired for the metadata read plus iterator scan
- no request-level snapshot is shared across separate operations

Current writes are also not snapshot-based:

- `HSET` and `HDEL` perform read-modify-write flows in process memory
- they rely on same-key worker locking to avoid conflicting same-key updates

As a result, the current model is not snapshot isolation, not MVCC, and not a
general-purpose read-consistent view across keys.

### Multi-Key Operations Have No Atomicity Guarantee

The current command set does not define any multi-key atomic operation.

Operationally:

- each command routes to at most one key
- different keys may run in parallel on different workers
- there is no mechanism for atomically reading or updating multiple keys
- any future multi-key command would need an explicit new execution contract

### `version` And `expire_at_ms` Are Reserved Fields Only

The current metadata schema contains `version` and `expire_at_ms`, but they do
not currently provide active semantics:

- new hashes are created with `version = 1`
- current write paths do not roll the version forward
- current read and write paths do not enforce TTL or expiration behavior
- current scans use whatever version is already stored in metadata

These fields should therefore be treated as reserved metadata fields in the
baseline, not as implemented versioning or expiration features.

## Consequences

The current system is safe for its small single-key hash command set, but its
consistency boundary is narrow:

- same-key behavior relies on `KeyLockTable`
- reads and writes are not snapshot-isolated
- multi-key atomicity does not exist
- reserved metadata fields must not be documented as active semantics

Any future expansion beyond the current single-key hash path should update this
ADR before claiming stronger guarantees.
