# Semantic Delivery to Route Transport Mapping

This note captures the currently settled design decisions for `flotsync-187`.

It describes how the high-level semantic delivery primitives:

- `GroupBroadcast`
- `ReliableDelivery`

map onto a lower transport-aware layer without baking UDP/TCP assumptions into
the semantic layer itself.

## 1. Scope

This note defines:

- the terminology split between semantic delivery and route transport
- what discovery publishes to the route-transport layer
- who owns live route/session lifecycle
- how shared endpoint UDP indications are consumed
- current route-preference rules
- current framing, ack, and backpressure decisions

This note does not define:

- the UDPour protocol for oversized logical payloads
- transport-internal decode details beyond endpoint frame classification
- the exact code shape for route-candidate descriptors

Those remain separate follow-up items.

## 2. Terminology

### 2.1 Semantic Delivery

Semantic delivery is the high-level layer consisting of:

- `GroupBroadcast`
- `ReliableDelivery`

It owns:

- message meaning
- delivery-class behavior
- route retry policy
- relay-store waiting
- recipient-ack semantics

It does not own:

- socket/session lifecycle
- protobuf framing over concrete transports
- transport-local fragmentation or reassembly

### 2.2 Route Transport

Route transport is the lower transport-aware layer beneath semantic delivery.

It owns:

- serializing logical messages to protobuf bytes
- acquiring egress-pool memory early
- opening and maintaining live route/session handles
- translating concrete transport outcomes into route-local `Ack`/`Nack`
- transport-local framing and fragmentation details

It does not own:

- semantic `Received` or `Processed` acknowledgments
- delivery-class retry policy
- relay-store completion semantics

### 2.3 Discovery

Discovery owns:

- learning what routes are currently possible
- endpoint identity verification
- route candidacy updates as the environment changes
- socket configuration for multicast/broadcast style discovery-owned paths

Discovery does not own:

- long-lived route/session management for delivery traffic
- delivery retries
- delivery keepalive policy
- binding the runtime endpoint socket used by replication delivery

## 3. Settled Decisions

### 3.1 Discovery Publishes Route Candidates, Not Live Routes

Discovery should publish route candidates/descriptors.

It should not publish already-open live route handles as the main abstraction.

Reasoning:

- delivery must be able to own session reuse and garbage collection
- delivery must avoid duplicate keepalives and duplicate live connections
- the abstraction should remain open to future transports beyond UDP/TCP

### 3.2 Route Candidates Are Capability-Based, Not Protocol-Based

Route candidates should be described in terms of capabilities rather than named
protocols such as UDP or TCP.

The endpoint role is not part of the route candidate:

- peer vs relay is determined by endpoint identity
- peer and relay ids live in non-overlapping identity spaces

The minimum questions the route-transport layer must be able to answer for one
route candidate are:

- does this candidate naturally preserve message boundaries, or does it require
  stream framing?
- does this candidate support round-trip control exchange well enough for local
  transport control messages?
- is this candidate worth keeping alive and reusing, or should it be opened on
  demand?
- does equality with another candidate imply shared coverage for one logical
  send?

That is intentionally more abstract than naming concrete transports.

### 3.3 Equality Defines Shared Coverage

Shared-coverage collapsing is defined by route-candidate equality.

If several peer endpoints currently resolve to equal route candidates:

- the route-transport layer may send one logical payload once
- semantic delivery may treat that as covering all peer routes mapped to that
  candidate

For now this rule applies only to peer routes.

Relay routes are not collapsed in v1.

### 3.4 Route Transport Owns Live Route Lifecycle

Route transport owns:

- opening live route handles
- reusing live handles
- keepalive policy for those handles
- garbage-collecting unused handles

Discovery only says what is possible right now.

Some coordination between discovery and route transport will still be needed so
they do not duplicate keepalive-like work, but live route ownership belongs in
route transport.

### 3.5 Route Choice Is Preference-Based, Not Admission-Based

There is no hard rule that some semantic-delivery message classes are forbidden
from certain route kinds.

Instead, route transport applies preferences.

Current preference rules are:

- `BestEffort` should prefer the route candidate with the widest shared peer
  coverage
- `Durable` should prefer route candidates with cleaner round-trip behavior and
  cheaper confirmation handling
- if a datagram-like route is the only available option, it is still usable

This means “prefer stream-like exchange when available” is a policy, not a type
system rule.

### 3.6 Serialization Happens Immediately in Route Transport

Route transport should serialize to protobuf bytes as early as possible after a
semantic-delivery request is received.

Reasoning:

- this lets the layer hand memory off to the egress pool early
- it avoids duplicating serialization work across several concrete send attempts
- it centralizes resource-failure handling in the transport-aware layer

If route transport cannot serialize or cannot acquire the required memory or
spill resources, it should `Nack` upward to semantic delivery.

### 3.7 Stream Framing Uses Protobuf Delimited Messages

For stream-like routes, the current choice is protobuf length-delimited framing.

Concretely:

- one protobuf message is prefixed by its encoded byte length
- the length uses protobuf's normal base-128 varint encoding

This is usually referred to as protobuf "delimited messages" or
"length-delimited protobuf framing".

It is a protobuf convention rather than a standalone RFC.

This should be preferred over inventing a custom stream framing.

### 3.8 UDPour Is Defined Separately

Handling logical payloads that do not fit into a single datagram is defined in a
separate design note:

- [UDPour](./datagram_multipart_transfer.md)

That work remains tracked in:

- `flotsync-p9u` `Design logical-payload transfer over datagram routes`

The multipart design remains iterative, but it is no longer wholly deferred.

### 3.9 Route Transport Surfaces Only `Sent`

Route transport should only surface a route-local `Sent` style acknowledgment,
which corresponds to the current `Ack` / `Nack` boundary on a route send.

It should not try to surface:

- `Received`
- `Processed`

Those are semantic-delivery concerns and, in many cases, are impossible to infer
from transport signals alone.

### 3.10 Backpressure Does Not Fail Routes

Backpressure should not cause route transport to declare a route dead or switch
to another route automatically.

Instead:

- the affected send is `Nack`ed upward
- semantic delivery retries later under its existing bounded retry policy

This avoids duplicating retry logic in the lower layer.

### 3.11 Discovery Owns Multicast/Broadcast Socket Configuration

Discovery should own socket configuration for discovery-managed shared paths,
including multicast or broadcast style paths.

That matches the current `flotsync_io` reality that shared UDP socket options
are bridge-wide mutable state and do not have built-in membership reference
counting.

Peer-announcement sockets are discovery-owned paths with a single maintaining
peer-announcement component per runtime process. The runtime endpoint socket is
not discovery-owned: route establishment uses that endpoint only after the setup
layer reports the current endpoint binding.

### 3.12 Shared Endpoint Indications Are Filtered By Consumers

There is no central endpoint demultiplexer for the runtime endpoint UDP
indication stream.

Components connected to the shared `UdpPort` classify the messages they
understand and ignore well-formed endpoint traffic for other sub-protocols.
Route establishment picks out discovery introduction frames. Route transport
picks out delivery frames.

The setup layer reports new runtime endpoint bindings because only the component
that initiated a bind knows what a fresh `UdpIndication::Bound` event is for.
Closure does not need a separate setup notification; endpoint users can observe
socket closure from the UDP indication stream.

### 3.13 Relay Confirmation Correlates by `message_id + relay_id`

Relay-store confirmation should correlate semantically by:

- `message_id`
- relay identity

This is the right level for the semantic-delivery layer.

Relay-issued receipt ids are optional extra data, but they are not required for
the semantic key.

Reasoning:

- semantic delivery cares that a given relay confirmed storage for a given
  logical message
- it does not need to distinguish which concrete transport attempt caused that
  confirmation
- relay identity is already available from the signed relay response

## 4. Mapping to Current `flotsync_io`

The current `flotsync_io` APIs remain relevant as concrete implementations
underneath the abstract route-transport layer.

At a high level:

- datagram-like route implementations can map onto `UdpPort`,
  `UdpRequest::Send`, and `UdpSendResult`
- stream-like route implementations can map onto `OpenTcpSession`,
  `TcpSessionRef`, and `TcpSessionEvent::SendAck` / `SendNack`

The important point is that semantic delivery should not depend on those API
shapes directly.

Only route transport should know which concrete `flotsync_io` mechanisms a
candidate currently maps to.

## 5. Deferred Items

The main deferred items are:

- `flotsync-p9u` for UDPour design and protocol survey
- transport-internal inbound decode details after each endpoint consumer has
  selected the frame family it owns
