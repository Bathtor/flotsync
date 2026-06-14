---
type: Protocol
title: UDPour
description: Specifies the multipart datagram protocol for carrying oversized logical payloads over one UDP route.
status: draft
---

# UDPour

This note captures the current protocol draft for `flotsync-p9u`.

It defines a small fixed-header multipart protocol for transporting one logical
payload over one datagram-like route when that payload does not fit into a
single datagram.

This protocol belongs to route transport, not semantic delivery.

## 1. Scope

This note defines:

- a fixed header shared by multipart payload and control messages
- current sender and receiver state responsibilities
- current retransmission and cleanup behavior
- the current policy choices for id reuse, checksum validation, and confusion
  handling

This note does not define:

- transport-internal inbound classification for UDPour frames
- how discovery exposes datagram-route candidates
- any stream framing
- transport security

## 2. Design Goals

The current design goals are:

- keep the on-wire format small and fixed-size
- avoid relying on IP fragmentation
- allow one sender response to satisfy multiple receivers on a shared route
- avoid sender-side per-receiver tracking
- allow sender and receiver state to time out and self-clean

## 3. Core Model

### 3.1 Logical Message Identity

A logical message is currently identified by:

- sender identity
- `message_id`

The sender identity comes from the forwarded UDP source metadata rather than
the multipart header.

The current draft does not add a sender-incarnation nonce.

Instead, it relies on:

- `message_id`
- `part_count`
- `checksum`

to detect accidental confusion between different logical messages that reused
the same sender-local id.

This is intentionally space-efficient, but it does mean collisions degrade into
lossy confusion rather than being ruled out structurally.

### 3.2 Part Numbering

Parts are numbered with zero-based `part_number`.

`part_count` is the total number of parts in the logical message and therefore
must be at least `1`.

The last part is the one where:

- `part_number == part_count - 1`

### 3.3 Regular Part Size

For any multi-part logical message, the sender uses the same payload length for
every part except the last one.

This means:

- the payload length of any non-final part defines the regular part size
- the last part may be shorter
- a single-part message does not need a regular part size

The receiver may not know the regular part size immediately if the first part it
observes happens to be the last part.

That is acceptable.

The receiver can still:

- track part presence
- request missing parts
- delay exact full-buffer preallocation until it has seen a non-final part

### 3.4 Total Length

The header does not carry total length in bytes.

Instead, once the receiver has observed any non-final part, it can calculate a
close upper bound on the total length as:

```text
regular_part_size * part_count
```

The exact total length is simply the sum of all received part payload lengths
and is therefore known once all parts have been received.

## 4. Frame Family

### 4.1 Fixed Base Header

All UDPour frames share the same fixed 20-byte base header:

```text
+--------+---------+-------+----------+
| type   | version | flags | reserved |
+--------+---------+-------+----------+
| message_id                          |
+-------------------------------------+
| part_number                         |
+-------------------------------------+
| part_count                          |
+-------------------------------------+
| checksum                            |
+-------------------------------------+
```

Field sizes:

- `type`: `u8`
- `version`: `u8`
- `flags`: `u8`
- `reserved`: `u8`
- `message_id`: `u32`
- `part_number`: `u32`
- `part_count`: `u32`
- `checksum`: `u32`

All multi-byte integer fields use network byte order.

The fixed 4-byte prefix intentionally leaves room for format evolution.

`message_id`, `part_count`, and `checksum` are present in all frame types so
that control frames can also sanity-check that they refer to the intended
logical message.

### 4.2 `type` Values

The highest bit distinguishes sender-originated frames from
receiver-originated frames:

- sender-originated frames use `0x01..0x7F`
- receiver-originated frames use `0x80..0xFE`

The current frame family is:

- `0x01`: `Payload`
- `0x02`: `NoLongerAvailable`
- `0x81`: `Ack`
- `0x82`: `NeedParts`

`0x00` and `0xFF` are intentionally left unassigned for now.

### 4.3 `checksum`

`checksum` is a whole-message checksum, not a per-part checksum.

Its purpose is:

- detecting accidental corruption
- sanity-checking late or confused packets for reused `message_id` values
- detecting inconsistent reassembly before delivery upward

The checksum algorithm is `CRC32C`.

Reasoning:

- it is cheap
- it is widely implemented
- hardware acceleration is common
- the semantic-delivery payload above this layer already carries stronger
  integrity or authenticity in many cases

This checksum is not meant to provide cryptographic protection.

## 5. Frame Semantics

### 5.1 `Payload`

A `Payload` frame carries one part of the logical message.

The body is the raw payload bytes for that part.

Semantics:

- `part_number` identifies which part this frame carries
- `part_count` identifies the total number of parts for this logical message
- `checksum` identifies the expected whole-message checksum

All `Payload` frames belonging to the same logical message must agree on:

- `message_id`
- `part_count`
- `checksum`

### 5.2 `Ack`

`Ack` means:

- the receiver has all parts
- whole-message checksum validation succeeded

It is advisory.

It allows eager sender cleanup, but correctness must not depend on its delivery.

### 5.3 `NeedParts`

`NeedParts` is receiver-driven negative acknowledgment.

It means:

- the receiver knows the logical message exists
- the receiver is still willing to complete it
- it is currently missing a set of parts

The body carries a representation of the missing part set.

The current preferred representation is:

- `RoaringBitmap` over `u32` part numbers

One `NeedParts` frame is still limited by datagram size.

Therefore a large missing set is expressed as multiple `NeedParts` frames with
disjoint missing-part bitmaps.

### 5.4 `NoLongerAvailable`

`NoLongerAvailable` is sender-driven negative acknowledgment.

It means:

- the sender no longer retains the parts needed to satisfy this logical message
- late `NeedParts` requests can no longer be honored

This is distinct from sender silence.

It allows receivers to stop retrying immediately instead of waiting for their
full give-up timeout.

## 6. Sender Behavior

### 6.1 Message Id Allocation

The required policy is:

- do not reuse a `message_id` while it is still live
- do not reuse a `message_id` immediately after purge
- on wrap-around, skip over any ids that are still live or still cooling down

This avoids eager id reuse without widening the wire identifier while leaving
the exact internal data structures up to the implementation.

### 6.2 Retention

The sender retains:

- the original parts

until a sender retention timeout expires.

After that:

- the sender purges local multipart state
- any later `NeedParts` may be answered with `NoLongerAvailable`

### 6.3 Shared-Route Retransmission

This protocol is explicitly meant to work on shared routes.

The sender does not track missing parts per receiver.

Instead:

- any `NeedParts` is treated as a route-level request
- the sender re-broadcasts the requested parts to the route
- duplicate receipt by other receivers is acceptable

This is a deliberate complexity trade-off in favor of sender simplicity and
shared-route usefulness.

### 6.4 Suggested Reuse Guard

The sender should not reuse a purged `message_id` immediately.

The current suggestion is to keep a reused-id cooldown for at least:

```text
sender_retention_timeout
+ receiver_retry_timeout
+ late_packet_budget
```

`late_packet_budget` is the route-transport estimate for how long an old packet
may still arrive late after normal sender/receiver retry activity. In practice
this should be approximated from worst-case RTT together with some allowance for
reordering and scheduling delay.

This does not eliminate confusion structurally, but it reduces the chance that
late packets collide with a newly created logical message that reused the same
id.

## 7. Receiver Behavior

### 7.1 Reassembly State

The receiver creates reassembly state once it first observes a `Payload` frame
for a logical message.

That state is keyed by:

- sender identity
- `message_id`

and carries:

- observed `part_count`
- observed `checksum`
- the set of received parts
- enough storage to retain payload bytes until completion or timeout

### 7.2 Consistency Checks

If a later `Payload` frame for the same `(sender, message_id)` disagrees with
the current reassembly state on:

- `part_count`
- `checksum`

then the receiver treats that as message confusion or corruption.

The recovery policy is:

- purge the current reassembly state immediately

After that, any later part for the same `(sender, message_id)` is treated as the
start of a fresh reassembly attempt.

### 7.3 Completion

The receiver completes reassembly once it has:

- all parts from `0` to `part_count - 1`
- enough size information to reconstruct the full payload

At that point it computes the whole-message checksum.

If checksum validation succeeds:

- deliver the logical payload upward
- emit `Ack`
- purge local multipart state immediately

If checksum validation fails:

- do not deliver upward
- purge local multipart state immediately

Whole-message checksum failure is treated the same way as other message
confusion: purge and wait for the next send.

### 7.4 Receiver Timeouts

The receiver retains incomplete reassembly state until a receiver give-up
timeout expires.

This timeout resets whenever the receiver accepts a new `Payload` part for that
logical message.

Before that it may send:

- one or more `NeedParts`

After that:

- it purges local state
- it stops retrying

## 8. Current Summary

The current draft chooses:

- a 20-byte fixed base header
- `u32` message ids
- `u32` part numbers and part counts
- one regular payload size per logical message except for the final part
- `CRC32C` whole-message checksum
- receiver-driven `NeedParts`
- sender-driven `NoLongerAvailable`
- multiple disjoint `NeedParts` frames instead of multipart `NeedParts`
- route-level retransmission rather than per-receiver sender tracking
- checksum-and-timeout based id confusion handling instead of a sender
  incarnation nonce
