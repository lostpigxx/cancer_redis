# MiniKV Engine Layer

## Scope

This layer is defined by:

- `src/engine/db_engine.h`
- `src/engine/db_engine.cc`
- `src/engine/key_codec.h`
- `src/engine/key_codec.cc`

It owns RocksDB integration and the current typed hash storage model.

## Responsibilities

The engine layer owns:

- opening RocksDB
- ensuring required column families exist
- encoding logical keys into RocksDB keys
- decoding metadata
- implementing hash operations on top of RocksDB primitives

It does not own network protocol concerns or thread routing policy.

## Column Family Model

The engine opens:

- `default`
- `meta`
- `hash`

The effective data model is carried by `meta` and `hash`.

`meta` stores one metadata record per logical user key.

`hash` stores field/value records for one logical hash object, using a prefix
that includes the user key and metadata version.

## Key Encoding Model

`KeyCodec` provides three important encodings:

- meta key
- hash data prefix
- hash data key

The current encoding shape is prefix-oriented so the engine can:

- fetch metadata by exact key
- scan all fields for one hash by prefix iteration

Metadata currently stores:

- `type`
- `encoding`
- `version`
- `size`
- `expire_at_ms`

## Current Hash Semantics

### HSET

- load metadata from `meta`
- reject type mismatch
- probe field existence in `hash`
- adjust `size` only when the field is newly inserted
- write metadata and field value in one `WriteBatch`

### HGETALL

- load metadata
- reject type mismatch
- derive the hash prefix from user key and version
- iterate all matching field/value entries in `hash`

### HDEL

- load metadata
- reject type mismatch
- probe each requested field
- delete matching fields in a `WriteBatch`
- update `size` or remove metadata when the hash becomes empty

## Current Design Strengths

- Typed metadata exists from the start instead of retrofitting later.
- The hash prefix layout works naturally with RocksDB iteration.
- `WriteBatch` is used for per-command grouped updates.
- The engine stays small enough to audit quickly.

## Current Design Risks

### Future Fields Without Current Semantics

`version` and `expire_at_ms` are stored, but current command execution does not
advance version or enforce expiration.

### Single-Key Assumption

Engine methods are designed around one logical key at a time. That matches the
worker model but becomes restrictive if the command surface grows toward
cross-key behavior.

### Tight Coupling To Current Hash Encoding

The current engine is clean for one hash encoding path, but it does not yet
separate:

- logical object semantics
- storage-encoding evolution
- compaction or cleanup strategy for future lifecycle features

## Current Design Conclusion

The engine layer is the most concrete and most mature part of `minikv` today. It
already has a typed storage layout and a coherent RocksDB mapping. Its main gap
is not basic correctness of the current hash model; it is that the metadata and
future evolution hooks are ahead of the implemented semantics.
