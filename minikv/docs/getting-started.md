# MiniKV Getting Started

This document is the best entrypoint if you have never read `minikv/` before.
It is intentionally code-oriented and follows the current implementation rather
than a future target design.

## Summary

`minikv` is currently a small single-process key-value prototype built on top of
RocksDB.

The main request path is:

`main -> Server -> RESP parser -> CmdFactory -> WorkerRuntime -> KeyLockTable -> Cmd -> DBEngine -> RocksDB`

Current scope is intentionally narrow:

- supported commands: `PING`, `HSET`, `HGETALL`, `HDEL`
- supported data type: hash only
- deployment shape: one POSIX process exposing a TCP server

The most important implementation fact is that current correctness depends more
on the shared key-lock runtime than on RocksDB transactions. Requests for the
same logical key cannot execute concurrently, even if different workers pick
them up.

## Architecture And Class Map

### `src/main.cc`

Process entrypoint.

- parses command-line flags into `Config`
- opens `MiniKV`
- creates `Server`
- runs the server until shutdown

Read this first to understand the top-level object graph.

### `include/minikv/minikv.h` and `src/minikv.cc`

Public facade.

`MiniKV` owns an internal `Impl`, and `Impl` owns:

- `DBEngine`
- `KeyLockTable`
- compatibility `WorkerRuntime`

Important behavior:

- `MiniKV::Open()` opens the RocksDB-backed engine before publishing the facade
- `Execute()` is synchronous and runs the `Cmd` directly under the shared key
  lock
- typed helpers `HSet()`, `HGetAll()`, and `HDel()` are thin wrappers around
  the unified command path

### `src/server/server.h` and `src/server/server.cc`

Network and connection-management layer.

`Server` is responsible for:

- creating the listening socket
- accepting connections
- assigning each connection to one I/O thread
- reading and buffering bytes
- parsing RESP requests
- writing encoded RESP responses
- idle timeout and orderly shutdown

Key internal types:

- `Connection`: per-client socket state, including read/write buffers and
  request / response sequence tracking
- `CompletedResponse`: worker result routed back to one connection with the
  original request sequence
- `IOThreadState`: per-I/O-thread ownership bundle for connections, completion
  queue, wakeup pipe, and thread object

### `src/server/resp_parser.h` and `src/server/resp_parser.cc`

RESP parsing and response encoding.

Current parser support is intentionally small:

- request root must be a RESP array
- array elements must be bulk strings

Current encoder support:

- simple strings
- errors
- integers
- arrays of bulk strings

This is enough for the current command set, but it is not a general RESP
implementation.

### `src/command/cmd.*`, `cmd_create.*`, `cmd_factory.*`, `t_*.*`

Turns parsed RESP parts or compatibility `CommandRequest` values into concrete
`Cmd` objects.

This layer:

- registers each supported command once
- separates registration lookup from command creation
- maps command names and `CommandType` values onto concrete `Cmd` classes
- performs command-specific `DoInitial()` validation and parameter extraction
- carries static command flags such as read/write and fast/slow

Current file split:

- `cmd_factory.*`: registration and lookup only
- `cmd_create.*`: build a `Cmd` from RESP parts or `CommandRequest`
- `t_kv.*`: key-value style commands such as `PING`
- `t_hash.*`: hash commands such as `HSET`, `HGETALL`, `HDEL`

### `src/worker/worker.*` and `key_lock_table.*`

Concurrency core of the current design.

`WorkerRuntime` owns:

- one worker thread per configured worker
- one bounded MPSC queue per worker
- a shared `DBEngine*` used to execute queued `Cmd` objects
- a shared `KeyLockTable*` used to serialize same-key execution

Admission rule:

- pick a starting worker with round-robin
- probe each worker queue once
- reject only if every queue is full

Correctness rule:

- acquire the striped key lock for `cmd->RouteKey()`
- execute `Cmd::Execute()`
- release the key lock after completion

Each concrete `Cmd` implements command semantics directly:

- `PING` -> returns `PONG`
- `HSET` -> calls `DBEngine::HSet()`
- `HGETALL` -> calls `DBEngine::HGetAll()`
- `HDEL` -> calls `DBEngine::HDel()`

The `Cmd` base class provides the shared initialization and response-building
helpers used by those command implementations.

### `src/engine/db_engine.*`

RocksDB integration and typed hash model.

`DBEngine` is responsible for:

- opening RocksDB
- ensuring required column families exist
- implementing hash operations
- grouping storage updates with `WriteBatch`

Current column families:

- `default`
- `meta`
- `hash`

Effective data model:

- `meta`: one metadata record per logical key
- `hash`: field/value data for one hash object

### `src/engine/key_codec.*`

Storage-key encoding layer.

It defines:

- `ValueType`
- `ValueEncoding`
- `KeyMetadata`
- binary encoding helpers for metadata and hash keys

Current logical encodings:

- meta key: `m| + key_length + user_key`
- hash prefix: `h| + key_length + user_key + version`
- hash data key: `hash_prefix + field`

This encoding is what makes `HGETALL` possible through a prefix scan.

### `src/common/thread_name.*`

Tiny helper used by server and worker code to set thread names on supported
platforms. It is useful for debugging but not part of the data path.

## Thread Model

The runtime currently has three kinds of threads:

- one accept thread
- `io_threads` I/O threads
- `worker_threads` worker threads

### When threads are created

- `MiniKV::Open()` constructs the shared key-lock table and the compatibility
  async runtime used by `MiniKV::Submit()`
- `Server::Start()` creates all I/O threads and then the accept thread
- `Server::Start()` also creates the worker runtime used by the TCP path

### Accept thread

The accept thread runs `Server::AcceptLoop()`.

Its job is only:

- accept a client socket
- make it nonblocking
- assign it to one I/O thread in round-robin order

It does not parse requests and does not touch RocksDB.

### I/O threads

Each I/O thread runs `Server::RunIOThread()`.

Each I/O thread exclusively owns:

- its `connections` vector
- each connection's socket fd
- per-connection read buffer
- per-connection write buffer
- per-connection pending request count
- per-connection last-activity timestamp

I/O threads use:

- `poll()` for socket readiness
- a wakeup `pipe()` for cross-thread notification

The wakeup pipe is used when:

- a new accepted connection is assigned to that I/O thread
- a worker completion is pushed back to that I/O thread
- shutdown is requested

### Worker threads

Each worker thread runs `Worker::Run()`.

Its loop is:

1. wait on `condition_variable`
2. pop one task from its local queue
3. acquire the striped key lock for `RouteKey()`
4. execute the command with `Cmd::Execute()`
4. invoke the completion callback

The callback runs on the worker thread and pushes the completed response into
the owning I/O thread's `completed` queue. The worker then wakes that I/O
thread through the pipe.

### What this means in practice

- socket reads and writes happen only on I/O threads
- command execution happens only on worker threads
- there is no separate dispatcher thread
- same-key safety comes from the shared key-lock table, not from per-key logic
  inside the engine

### Important current note

One connection can pipeline requests for different keys. Different keys may run
on different workers, but the server now assigns request sequence numbers and
reorders completions before writing back to the socket. Cross-key pipelining on
one connection therefore preserves submission order.

## How To Read The Code Step By Step

If you are reading the project for the first time, use this order instead of
following file names alphabetically.

### Step 1: Start from behavior, not implementation

Read [`minikv/tests/server_test/server_test.cc`](../tests/server_test/server_test.cc).

This gives you the external service contract first:

- happy-path `PING` and hash lifecycle
- fragmented request handling
- malformed RESP recovery
- concurrent clients across I/O threads
- oversized request rejection

This is the fastest way to build a mental model before reading internals.

### Step 2: Read the top-level assembly

Read:

- [`minikv/src/main.cc`](../src/main.cc)
- [`minikv/src/minikv.cc`](../src/minikv.cc)

At this stage, answer only these questions:

- what objects are created
- in what order they are created
- which object owns which subsystem

Do not dive into `server.cc` yet.

### Step 3: Follow one request through the server

Read [`minikv/src/server/server.cc`](../src/server/server.cc) in this order:

1. `Start()`
2. `AcceptLoop()`
3. `RunIOThread()`
4. `HandleReadable()`
5. completion callback inside `HandleReadable()`
6. `DrainIOState()`
7. `HandleWritable()`

Goal:

- understand how a connection is assigned
- understand how bytes become requests
- understand how worker results get back to the same connection

Also read [`minikv/src/server/resp_parser.cc`](../src/server/resp_parser.cc)
next to it so the read path makes sense.

### Step 4: Read command shaping and worker execution

Read these files together:

- [`minikv/src/command/cmd_create.cc`](../src/command/cmd_create.cc)
- [`minikv/src/command/cmd_factory.cc`](../src/command/cmd_factory.cc)
- [`minikv/src/command/t_kv.cc`](../src/command/t_kv.cc)
- [`minikv/src/command/t_hash.cc`](../src/command/t_hash.cc)
- [`minikv/src/worker/worker.cc`](../src/worker/worker.cc)
- [`minikv/src/worker/key_lock_table.cc`](../src/worker/key_lock_table.cc)

At this stage, answer these questions:

- where does RESP input become a concrete `Cmd`
- where does execution become asynchronous
- why do operations on the same key serialize even across different workers

### Step 5: Read the storage model

Read:

- [`minikv/src/engine/db_engine.cc`](../src/engine/db_engine.cc)
- [`minikv/src/engine/key_codec.cc`](../src/engine/key_codec.cc)

Focus on:

- why `meta` and `hash` are separate column families
- how metadata is encoded
- how `HSET`, `HGETALL`, and `HDEL` map to RocksDB primitives

The key point is to understand one logical hash object as:

- one metadata record in `meta`
- many field/value records in `hash`

### Step 6: Use unit tests to confirm the concurrency model

Read [`minikv/tests/command_test/hash_command_test.cc`](../tests/command_test/hash_command_test.cc).

These tests tell you what the current code considers guaranteed:

- same-key concurrent updates remain consistent
- same-field concurrent overwrites do not duplicate fields
- different keys can proceed independently
- worker runtime preserves same-key serialization and allows different-key
  parallelism
- backpressure is per worker queue, not global

## Checkpoints While Reading

Use these tests as checkpoints for your understanding.

### `PingAndBasicHashLifecycle`

Map one command from socket read all the way to RocksDB write and back to socket
response.

### `FragmentedRespInputAndErrorPath`

Confirm that `RespParser` is incremental and that malformed input does not
poison the next command.

### `ConcurrentClientsAcrossIoThreads`

Confirm the division of labor between the accept thread and I/O threads.

### `SameKeyConcurrentUpdatesStayConsistent`

Confirm that current same-key correctness comes from the shared key lock, not
from a transactional command layer.

### `RejectsOverloadedWorkerQueue`

Confirm that overload handling is local to one worker queue, which means hot-key
traffic can fail even if other workers are idle.

## Reading Defaults

When learning this codebase, keep these defaults in mind:

- prefer understanding the request path before reading individual helpers
- do not project full Redis semantics onto this code; it implements only a
  small hash-oriented subset
- the two boundaries that matter most are:
  - `Server` owns network progress and connection ownership
  - `WorkerRuntime` plus `KeyLockTable` own execution concurrency
