# Protobuf Datamodel Transport

This document describes the current protobuf transport used by
[`flotsync_messages`](../flotsync_messages) for
schema/datamodel snapshots and operations.

## Scope

- The transport is specialized to [`UpdateId`](../flotsync_core/src/versions/mod.rs).
- Snapshot decoding is schema-driven. In practice, callers are expected to decode with the same
  logical schema that was used to encode.
- The current canonical history representation is the columnar
  [`HistorySnapshot`](../messages/proto/datamodel.proto), not the
  earlier row-oriented node message shape.

## Compatibility Expectations

- Field identity is by `field_name` string on the wire. Renaming a datamodel field is therefore
  wire-incompatible.
- Field data types are part of the wire contract. Decoding with a schema that assigns a different
  `ReplicatedDataType` to a field will fail validation.
- The snapshot decoder rejects extra fields that are not consumed by the schema row decoder, so
  this transport does not currently provide a lenient forward-compatibility mode for added fields.
- `HistorySnapshot` is only read in its current columnar form. Older node-oriented history
  snapshots are not supported by the current decoder.

## ID Mapping

- `UpdateId` is encoded as `HistoryId { version, node_index, chunk_index? }`.
- Unindexed IDs must omit `chunk_index`.
- Indexed IDs (`IdWithIndex<UpdateId>`) must set `chunk_index`.
- Snapshot origin links (`origin_left_*`, `origin_right_*`) use the same `HistoryId` mapping.
- `SchemaOperation.change_id` is encoded explicitly and is not coupled by the codec to nested
  operation IDs.

## Snapshot Mapping

- `DataSnapshot` contains ordered `RowSnapshot`s.
- Each `RowSnapshot` contains `SnapshotField`s keyed by `field_name`.
- State-based fields are encoded directly in the `SnapshotField.value` oneof:
  - `monotonic_counter`
  - `total_order_register`
  - `total_order_finite_state_register`
- History-based fields (`LatestValueWins`, `LinearString`, `LinearList`) use `HistorySnapshot`.

## HistorySnapshot Layout

`HistorySnapshot` splits metadata from payload buffers:

- `nodes: repeated HistoryNodeMeta`
- `values: oneof { primitive_values, string_values }`

The exact payload interpretation depends on the schema field type:

- `LatestValueWins`
  - primitive payloads consume one entry from `primitive_values`
  - array payloads consume `value_len` entries from `primitive_values`
- `LinearList`
  - each node consumes `value_len` entries from `primitive_values`
- `LinearString`
  - all payloads are concatenated into one `string_values` buffer
  - each node consumes `value_len` UTF-8 bytes from that buffer

Payloads are consumed sequentially in node order. There is no explicit offset on the wire.

## Nulls, Tombstones, and Boundary Nodes

- Explicit nullable register values use `value_is_null = true` and `value_len = 0`.
- Tombstones are represented by `HistoryNodeMeta.deleted = true`.
- Boundary nodes are identified by a missing origin side link:
  - begin node: missing `origin_left_*`
  - end node: missing `origin_right_*`
- Boundary nodes must have `value_len = 0` and `value_is_null = false`.
- Deleted nodes may still carry payload data; deletion is represented by `deleted`, not by dropping
  the payload.

## Operation Mapping

- `SchemaOperation` is encoded as:
  - `change_id`
  - ordered `fields`
- `LatestValueWins` operations encode one `UpdateOperation`.
- `LinearString` and `LinearList` operations encode ordered repeated action batches.
  Order is preserved exactly.
- Linear delete ranges are encoded relative to one logical update:
  - `start` is a full `HistoryId`
  - `end_chunk_index`, when present, extends the delete range within the same logical update
- The operation decoder validates decoded operations against the provided schema before returning
  them.

## Validation

The transport performs both wire-shape validation and datamodel validation:

- required protobuf fields and oneofs must be present
- `UpdateId` vs `IdWithIndex<UpdateId>` `chunk_index` presence must match context
- history payload buffers must match the surrounding field schema
- reconstructed CRDTs from decoded snapshots are validated with `validate_integrity()`

## Benchmarks

Current benchmark baselines for the canonical history format are documented in
[history_snapshot_formats.md](history_snapshot_formats.md).
