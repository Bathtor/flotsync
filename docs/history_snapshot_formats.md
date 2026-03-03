# HistorySnapshot Benchmark Baseline

This document records the current benchmark baseline for the canonical protobuf
[`HistorySnapshot`](../messages/proto/datamodel.proto) format.

It is no longer an experiment comparison document. The purpose of this benchmark is to catch
performance and encoded-size regressions in the shipped columnar history transport.

Transport details and compatibility expectations are documented in
[protobuf_datamodel_transport.md](protobuf_datamodel_transport.md).

## Method

- Command: `cargo bench -p flotsync_messages --bench history_snapshot_formats`
- Criterion config:
  - `sample_size = 30`
  - `warm_up_time = 500 ms`
  - `measurement_time = 2 s`
- Measurements include:
  - encode to protobuf bytes
  - parse + decode from protobuf bytes
  - parse + decode + CRDT reconstruction

## Fixtures

- `lvw_array_medium`
  - `LatestValueWins<[Int]>`
  - 128 updates
  - rotates through `null`, empty arrays, and short non-empty arrays
- `lvw_array_large`
  - same shape as above
  - 1024 updates
- `linear_string_tombstone_medium`
  - 128 fixed-width string chunks
  - delete every 5th chunk in reverse order
- `linear_string_tombstone_large`
  - 1024 fixed-width string chunks
  - delete every 6th chunk in reverse order
- `linear_list_tombstone_medium`
  - 96 chunks of 4 `Int` values
  - delete every 5th chunk in reverse order
- `linear_list_tombstone_large`
  - 256 chunks of 8 `Int` values
  - delete every 6th chunk in reverse order

## Recorded Results

Results below were recorded on March 3, 2026 from the current canonical format.

| Fixture | Encoded bytes | Encode median | Decode median | Decode+reconstruct median |
| --- | ---: | ---: | ---: | ---: |
| `lvw_array_medium` | 2171 | 6.508 us | 10.548 us | 11.639 us |
| `lvw_array_large` | 19241 | 51.521 us | 80.698 us | 91.499 us |
| `linear_string_tombstone_medium` | 3574 | 7.397 us | 13.590 us | 62.790 us |
| `linear_string_tombstone_large` | 28696 | 56.165 us | 108.980 us | 2.804 ms |
| `linear_list_tombstone_medium` | 3155 | 7.271 us | 11.684 us | 37.338 us |
| `linear_list_tombstone_large` | 10485 | 23.799 us | 33.642 us | 344.000 us |

## Notes

- Criterion will report changes relative to the last local benchmark baseline in `target/criterion`.
- Small encode-side improvements and decode-side regressions are expected to show up here as the
  transport implementation changes.
- Encoded byte counts are often the most stable signal when comparing changes across different
  machines.
