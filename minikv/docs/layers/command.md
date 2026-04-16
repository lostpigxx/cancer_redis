# MiniKV Command Layer

## Scope

This layer is defined by:

- `src/command/cmd.h`
- `src/command/cmd.cc`
- `src/command/cmd_create.h`
- `src/command/cmd_create.cc`
- `src/command/cmd_factory.h`
- `src/command/cmd_factory.cc`
- `src/command/t_kv.h`
- `src/command/t_kv.cc`
- `src/command/t_hash.h`
- `src/command/t_hash.cc`

It maps parsed RESP command parts into executable operations.

## Responsibilities

The command layer owns three steps:

- turn parsed RESP parts or compatibility `CommandRequest` values into `Cmd`
- execute command semantics against `DBEngine`

This keeps the server layer unaware of individual command rules.

## Current Command Surface

Supported commands:

- `PING`
- `HSET`
- `HGETALL`
- `HDEL`

`CmdFactory` owns registration for each supported command.

`CreateCmd()` uses that registration to instantiate concrete `Cmd`
implementations from:

- RESP parts
- compatibility `CommandRequest` values

Current command implementations are grouped by family:

- `PING`
- `HSET`
- `HGETALL`
- `HDEL`

- `t_kv.*`: `PING`
- `t_hash.*`: `HSET`, `HGETALL`, `HDEL`

Each command validates its own input in `DoInitial()`, extracts the parameters
it needs, and runs the actual engine call in `Do()`.

Each registration also carries static flags:

- `PING`: `read | fast`
- `HSET`: `write | fast`
- `HGETALL`: `read | slow`
- `HDEL`: `write | slow`

## Current Design Characteristics

- The command layer is small and easy to extend for additional single-key
  commands.
- Validation and execution now live in the same command object.
- Command output is already normalized into one internal response shape before
  reaching the server encoder.

## Current Design Risks

### Protocol-Shaped Core Types

`CommandResponse` is already close to RESP instead of being a transport-neutral
result type. That keeps the server simple but couples command execution to the
wire format.

### Limited Growth Path For Richer Semantics

The current command path is well suited to straightforward single-key commands.
It does not yet define how to handle:

- cross-key ordering
- multi-step atomicity
- richer result types
- conditional updates

As command coverage grows, the main open design question is how much those
command flags should influence scheduling beyond today's key-lock runtime.

## Current Design Conclusion

Today, the command layer is appropriately small and direct. The next design
pressure point is deciding when command metadata should become active scheduling
policy instead of remaining registration-time description.
