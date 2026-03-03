# HistorySnapshot Format Evaluation

This compares the current node-oriented protobuf `HistorySnapshot` with the experimental
`ExperimentalColumnarHistorySnapshot` format.

## Method

- Command: `cargo bench -p flotsync_messages --bench history_snapshot_formats`
- Criterion config:
  - `sample_size = 10`
  - `warm_up_time = 500 ms`
  - `measurement_time = 1 s`
- Measurements include:
  - encode to protobuf bytes
  - parse + decode from protobuf bytes
  - parse + decode + CRDT reconstruction

## Fixtures

- `lvw_array_medium`
  - `LatestValueWins<[Int]>`
  - 256 updates
  - rotates through `null`, empty arrays, and short non-empty arrays
- `lvw_array_large`
  - same shape as above
  - 2048 updates
- `linear_string_tombstone_medium`
  - 256 fixed-width string chunks
  - delete every 3rd chunk in reverse order
- `linear_string_tombstone_large`
  - 1024 fixed-width string chunks
  - delete every 2nd chunk in reverse order
- `linear_list_tombstone_medium`
  - 256 chunks of 4 `Int` values
  - delete every 3rd chunk in reverse order
- `linear_list_tombstone_large`
  - 1024 chunks of 4 `Int` values
  - delete every 2nd chunk in reverse order

## Results

| Fixture | Baseline bytes | Columnar bytes | Size delta | Encode median | Columnar encode | Decode median | Columnar decode | Decode+reconstruct | Columnar decode+reconstruct |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `lvw_array_medium` | 7278 | 5039 | -30.76% | 49.140 us | 16.424 us | 52.062 us | 23.915 us | 55.152 us | 26.727 us |
| `lvw_array_large` | 60012 | 42753 | -28.76% | 393.91 us | 115.89 us | 412.97 us | 177.87 us | 430.26 us | 200.15 us |
| `linear_string_tombstone_medium` | 9733 | 8395 | -13.75% | 48.782 us | 30.055 us | 55.367 us | 46.689 us | 248.73 us | 239.14 us |
| `linear_string_tombstone_large` | 39331 | 34373 | -12.61% | 196.06 us | 114.21 us | 221.31 us | 177.54 us | 3.1325 ms | 3.0852 ms |
| `linear_list_tombstone_medium` | 11520 | 9257 | -19.64% | 52.900 us | 23.733 us | 58.484 us | 41.556 us | 242.31 us | 225.42 us |
| `linear_list_tombstone_large` | 46496 | 37541 | -19.26% | 210.82 us | 90.713 us | 231.03 us | 157.89 us | 3.1091 ms | 3.0235 ms |

## Interpretation

- The columnar format reduced encoded size in every measured case.
- The size win is strongest for `LatestValueWins` histories, around 29% to 31%.
- The size win for tombstone-heavy `LinearString` histories is smaller, around 13%.
- The size win for tombstone-heavy `LinearList<Int>` histories is around 19% to 20%.
- Encode cost improved materially across the board.
- Decode cost also improved in every measured case.
- Full decode+reconstruct gains are strongest for `LatestValueWins` and modest for string/list,
  which suggests the remaining time there is dominated more by CRDT reconstruction than by the
  protobuf layout itself.

## Recommendation

Keep the columnar format and iterate toward making it the primary history snapshot representation.

Reasoning:

- It is strictly smaller on all tested workloads.
- It is strictly faster to encode on all tested workloads.
- It is also faster to decode on all tested workloads.
- There were no measured regressions.
- The strongest wins appear on the most value-heavy case (`LatestValueWins`), which is exactly
  where lifting payloads out of repeated node messages should help.

Follow-up:

1. Replace the current `HistorySnapshot` transport with the columnar layout, or support both
   during a short migration window if wire compatibility matters.
2. If we keep iterating before switching, the next likely optimization target is metadata
   overhead for tombstone-heavy string histories, where the current win is real but smaller.
