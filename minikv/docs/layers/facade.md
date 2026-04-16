# MiniKV Facade Layer

## Scope

This layer is defined by:

- `include/minikv/minikv.h`
- `include/minikv/command.h`
- `include/minikv/config.h`
- `src/minikv.cc`

It is the public entrypoint for embedding `minikv` as a library.

## Responsibilities

`MiniKV` currently owns three internal subsystems:

- `DBEngine`
- `KeyLockTable`
- compatibility `WorkerRuntime`

The facade exposes:

- lifecycle: `MiniKV::Open()`
- generic command path: `Execute()` and `Submit()`
- typed helpers: `HSet()`, `HGetAll()`, `HDel()`

The ownership graph is simple:

`MiniKV -> Impl -> { DBEngine, KeyLockTable, WorkerRuntime }`

`Open()` initializes the engine first and only publishes the `MiniKV` instance
after the RocksDB open path succeeds.

## Current Design Characteristics

- The facade is intentionally thin. It does not contain command semantics.
- The synchronous path executes `Cmd` objects directly while holding the shared
  key lock for that command's route key.
- Typed helpers are implemented by constructing `CommandRequest` values and then
  invoking the unified command path.
- `CommandRequest` is a compatibility surface; the internal path converts it
  into a concrete `Cmd` via `CmdFactory` before execution or async submission.

This gives the code one main execution path instead of duplicating business
logic between direct API calls and server requests.

## Current Design Tension

The facade mixes two roles:

- an embedded storage API
- a command-oriented execution interface shaped around server commands

This is visible in:

- `CommandType`
- `CommandRequest`
- `CommandResponse`
- `ResponseType`

The current public API is therefore not transport-neutral. RESP-oriented
response concepts are part of the core facade contract instead of staying inside
the server layer.

## Current Design Conclusion

For the current prototype, the facade is small and coherent enough. If `minikv`
grows as an embedded library, the next cleanup point is to split:

- domain/storage results
- wire-protocol formatting concerns
