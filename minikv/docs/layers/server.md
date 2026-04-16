# MiniKV Server Layer

## Scope

This layer is defined by:

- `src/server/server.h`
- `src/server/server.cc`
- `src/server/resp_parser.h`
- `src/server/resp_parser.cc`

It implements the POSIX TCP server path for `minikv_server`.

## Responsibilities

The server layer owns:

- listening socket setup
- accept loop
- per-I/O-thread connection ownership
- nonblocking read/write loops
- RESP request parsing
- response encoding
- connection idle timeout and shutdown behavior
- worker runtime lifetime for the TCP path

The server is not responsible for command semantics or storage layout. It hands
those off to lower layers by constructing `Cmd` objects and submitting them to
its own `WorkerRuntime`.

## Threading Model

The runtime splits into:

- one accept thread
- N I/O threads
- M worker threads owned directly by `Server`

Each accepted connection is assigned to one I/O thread. That I/O thread owns:

- socket fd
- read buffer
- write buffer
- write offset
- pending request count
- request / response sequence numbers
- last activity timestamp

This ownership model avoids cross-I/O-thread socket coordination.

## Request Path

Within one I/O thread, the current path is:

1. read bytes from socket
2. append into `read_buffer`
3. parse complete RESP arrays
4. turn parts into `Cmd`
5. assign a per-connection request sequence
6. submit async work to `WorkerRuntime`
7. receive async completion back on the same I/O thread
8. reorder completions by request sequence
9. encode RESP response
10. append into `write_buffer`
11. flush on writable events

The I/O threads use `poll()` and a wakeup pipe. The wakeup pipe is used for:

- newly accepted connections
- completed worker responses
- shutdown notification

## RESP Model

The parser currently supports:

- RESP array input
- bulk string elements

The encoder currently emits:

- simple strings
- errors
- integers
- arrays of bulk strings

That is enough for the current command set, but it is not a general RESP
implementation.

## Current Design Strengths

- Socket progress is separated from RocksDB work.
- One I/O thread owns one connection at a time.
- Fragmented request input is supported.
- Slow clients do not automatically block all workers.
- Connection-local RESP pipeline order is preserved even when different requests
  complete on different workers.

## Current Design Risks

### Observability

`Server` now exports a process-internal metrics API via
`Server::GetMetricsSnapshot()`. The snapshot is in-memory only for now (no
network status endpoint yet), and it is safe to sample from control paths or
tests without touching the RESP data plane.

Metric definitions and sampling points:

- `worker_queue_depth[i]`:
  - definition: current backlog (`head - tail`) of worker queue `i`
  - sampled when `GetMetricsSnapshot()` calls `WorkerRuntime` aggregation
- `worker_rejections`:
  - definition: total worker submission rejections (`Busy("worker queue full")`)
  - sampled from `WorkerRuntime::rejected_requests_`
- `worker_inflight`:
  - definition: number of accepted worker tasks not yet completed
  - incremented on successful enqueue, decremented in completion callback
- `active_connections`:
  - definition: currently open TCP connections
  - sampled from `Server::connection_count_`
- `accepted_connections`:
  - definition: cumulative accepted connections that passed admission checks
  - incremented in `Server::EnqueueConnection()`
- `closed_connections`:
  - definition: cumulative closed connections
  - incremented in `Server::CloseConnection()`
- `idle_timeout_connections`:
  - definition: cumulative connections closed because idle timeout was hit
  - marked in `Server::CloseIdleConnections()`, counted in `CloseConnection()`
- `errored_connections`:
  - definition: cumulative connections closed due to poll/read/write errors
  - marked in `RunIOThread()` failure branches, counted in `CloseConnection()`
- `parse_errors`:
  - definition: RESP parse failures (malformed request payloads)
  - incremented in `Server::HandleReadable()` when parser returns an error

This gives a minimal viable export surface for overload/backpressure and
connection-health diagnostics. A network `/status` endpoint can be layered on
top later by serializing the same snapshot.

### Event Loop Scalability

`poll()` plus per-thread connection vectors is fine for a compact prototype but
will become a constraint earlier than the storage model if connection count and
feature count keep growing.

### Wakeup Error Handling

Wakeup writes are intentionally ignored today. This keeps the code simple, but
it also means the server has no explicit signal when notification pressure or
shutdown coordination degrades.

## Current Design Conclusion

The server layer is a compact prototype transport. It is good enough to exercise
the storage and command path, but it is not yet a mature protocol runtime.
