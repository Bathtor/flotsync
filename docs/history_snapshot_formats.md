# HistorySnapshot Format Evaluation

This compares the current node-oriented protobuf `HistorySnapshot` with the experimental
`ExperimentalColumnarHistorySnapshot` format.

In the current experimental shape:
- `LatestValueWins` and `LinearList` use a dense `primitive_values` buffer
- `LinearString` uses one concatenated `string_values` buffer plus UTF-8 byte slicing metadata

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
| `lvw_array_medium` | 7278 | 5039 | -30.76% | 46.977 us | 15.859 us | 50.988 us | 23.170 us | 53.768 us | 25.920 us |
| `lvw_array_large` | 60012 | 42753 | -28.76% | 379.37 us | 114.29 us | 405.96 us | 174.02 us | 422.53 us | 193.31 us |
| `linear_string_tombstone_medium` | 9733 | 7976 | -18.05% | 47.182 us | 18.531 us | 54.230 us | 37.150 us | 242.03 us | 223.08 us |
| `linear_string_tombstone_large` | 39331 | 32418 | -17.58% | 189.04 us | 68.709 us | 217.45 us | 136.67 us | 3.0267 ms | 2.9578 ms |
| `linear_list_tombstone_medium` | 11520 | 9257 | -19.64% | 50.771 us | 22.446 us | 56.489 us | 40.053 us | 235.20 us | 217.59 us |
| `linear_list_tombstone_large` | 46496 | 37541 | -19.26% | 205.29 us | 83.768 us | 222.97 us | 154.63 us | 3.0286 ms | 2.9291 ms |

## Interpretation

- The columnar format reduced encoded size in every measured case.
- The size win is strongest for `LatestValueWins` histories, around 29% to 31%.
- The dedicated concatenated string buffer materially improved `LinearString`; the size win for
  tombstone-heavy string histories is now around 18%.
- The size win for tombstone-heavy `LinearList<Int>` histories is around 19% to 20%.
- Encode cost improved materially across the board.
- Decode cost also improved in every measured case.
- Full decode+reconstruct gains are strongest for `LatestValueWins` and now clearly positive for
  both string and list,
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
- The `LinearString` specialization to a concatenated `string_values` buffer closed most of the
  remaining gap for string-heavy histories.

Follow-up:

1. Replace the current `HistorySnapshot` transport with the columnar layout, or support both
   during a short migration window if wire compatibility matters.
2. If we keep iterating before switching, the next likely optimization target is metadata
   overhead itself, since the value-buffer side is now pulling its weight across all three
   history families.
