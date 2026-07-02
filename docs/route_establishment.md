---
type: Protocol
title: Route Establishment Protocol
description: Specifies non-mDNS route establishment for verified replication route candidates.
status: draft
---

# Route Establishment Protocol

This document specifies the non-mDNS route establishment protocol used by the
replication runtime route source.

Peer announcement is a separate discovery protocol. Route establishment consumes
peer-announcement observations as untrusted route candidates, but it does not
own the peer-announcement UDP port.

## 1. Scope

This protocol discovers local-network Flotsync peers and turns verified peer
introductions into replication route candidates.

It defines:

- how plaintext peer announcements feed route establishment
- peer-announcement socket binding ownership
- receiver-driven introduction probes
- signed introduction claims
- receiver-owned freshness and retry policy
- route publication rules for verified discovery routes

It does not define:

- mDNS discovery
- signed `IntroductionRequest` messages
- sender-provided route TTLs
- persistent discovery sessions
- sequence numbers or long replay windows

## 2. Terminology

### 2.1 Peer Instance

A peer instance is one running Flotsync process identified by a 16 byte
`instance_uuid`.

The instance id is not a replication identity. It is only used to correlate
peer announcements, probes, and introductions from the same running discovery component.

### 2.2 Listening Route

A listening route is one endpoint advertised in `Peer.listening_on`.

For the current route establishment path, replication consumes only UDP unicast routes.
The protocol shape remains explicit about route protocol so TCP or other route
families can be introduced later.

### 2.3 Introduction Claim

An introduction claim says that one `member_id` and public-key bundle
fingerprint is reachable through one probed route for one or more replication
groups.

Claims are signed per member identity. This lets one discovery component
advertise different identities and group memberships on different routes without
making the whole introduction depend on a single global signer.

### 2.4 Runtime Endpoint Frame

The replication endpoint carries `flotsync.endpoint.v1.EndpointFrame` messages.
Delivery frames and discovery introduction frames share this envelope because
introduction verifies the same endpoint that later carries delivery traffic.

`Peer` announcements are not carried inside `EndpointFrame`; they arrive through
the peer-announcement protocol on the peer-announcement UDP port. The default
peer-announcement port is `52156` for announcement listening and broadcast
targets.

Route establishment frames are carried on the runtime endpoint port advertised
in `Peer.listening_on`. That endpoint port is also used by replication delivery
traffic. Route establishment therefore does not bind or own the runtime endpoint
socket; it is told which current endpoint socket and local address to use by the
setup layer that initiated the bind.

## 3. Component Wiring And Port Ownership

### 3.1 Peer Announcement Components

The peer-announcement protocol has one socket maintainer for one runtime
process. The maintainer is not a separate component. Instead,
`PeerAnnouncementComponent` and `PeerAnnouncementObservationComponent` both
have a configuration flag that says whether that component is responsible for
maintaining the peer-announcement UDP socket while it is running.

The component configured as the maintainer binds the peer-announcement UDP
socket, applies the agreed socket reuse and broadcast options, and records the
resulting `SocketId` for its own sends or receive filtering. Other local
components observe inbound UDP indications through the same shared `UdpPort`;
there is no separate peer-announcement binding report.

In the code structure, this should be modelled as:

- `PeerAnnouncementSocketMaintenance::Maintain` means this component binds,
  configures, reports, closes, and rebinds the peer-announcement UDP socket.
- `PeerAnnouncementSocketMaintenance::Observe` means this component waits for
  another local peer-announcement component to maintain the socket.
- No additional peer-announcement socket binding port is needed for the
  standard setup helpers. `UdpPort` indications are already broadcast to every
  local component connected to the same bridge.
- A non-maintaining observer infers the maintained peer-announcement `SocketId`
  from `UdpIndication::Bound` by matching the bound `local_addr.port()` against
  the configured peer-announcement bind port. Once it has inferred the socket
  id, it filters `UdpIndication::Received` by that socket id before attempting
  to decode `Peer` payloads.
- `PeerAnnouncementComponent` sends on the current peer-announcement `SocketId`.
  When configured with `Maintain`, it obtains that socket id from its own bind.
  When configured with `Observe`, it waits for a compatible socket id from the
  shared UDP indication stream. The standard announcement-and-observation setup
  helper makes the sending component the maintainer and the observation
  component the non-maintaining observer.
- `PeerAnnouncementObservationComponent` binds and filters by its maintained
  `SocketId` when configured with `Maintain`. When configured with `Observe`,
  it infers the maintained `SocketId` from the shared UDP indication stream and
  filters peer-announcement packets without owning socket lifecycle.

Setup helpers should encode the valid local configurations:

- announcement-only: create `PeerAnnouncementComponent` with `Maintain`
- observation-only: create `PeerAnnouncementObservationComponent` with
  `Maintain`
- announcement-and-observation: create `PeerAnnouncementComponent` with
  `Maintain`, create `PeerAnnouncementObservationComponent` with `Observe`, and
  connect both components to the same IO `UdpPort` before the maintainer binds

`PeerAnnouncementComponent` owns sending plaintext `Peer` announcements on the
current peer-announcement socket.

`PeerAnnouncementObservationComponent` owns observing plaintext `Peer`
announcements on the current peer-announcement socket. It
provides `PeerAnnouncementObservationPort` with decoded
`PeerAnnouncementObserved` indications.

When a runtime runs both peer-announcement sending and observation locally, they
share the maintained peer-announcement UDP socket. Duplicate binds are not the
target design because they can silently diverge on socket options or bind
addresses. Setup helpers should therefore make exactly one local component the
maintainer for each peer-announcement socket.

The maintaining component owns close and rebind lifecycle for the
peer-announcement socket. Non-maintaining observation components clear their
inferred socket id when they observe `UdpIndication::Closed` for it, then wait
for the next `UdpIndication::Bound` that matches the configured
peer-announcement bind port.

### 3.2 Runtime Endpoint Owner

The runtime setup layer owns the delivery endpoint bind. In the current host
shape, this is represented by the local endpoint manager.

The endpoint owner is responsible for:

- binding the configured endpoint socket
- knowing that a fresh `UdpIndication::Bound` event belongs to the runtime
  endpoint bind it initiated
- reporting each new endpoint binding to route establishment with the current
  `SocketId` and concrete local `SocketAddr`

Only new bindings need to be reported through this setup path. Route
establishment observes endpoint socket closure itself from the shared `UdpPort`
indication stream.

### 3.3 Route Establishment Component

`RouteEstablishmentComponent` requires:

- `PeerAnnouncementObservationPort` for untrusted candidate routes
- the IO `UdpPort` service for endpoint send/receive indications
- an externally supplied current endpoint binding
- async discovery credentials for signing and verifying claim payloads
- the core shared group membership snapshot
- the local member identity used to select local claim material

It must not bind the runtime endpoint socket. It sends `IntroductionRequest` and
`Introduction` frames using the supplied endpoint `SocketId`.

There is no central endpoint demultiplexer. Components that receive shared
endpoint UDP indications classify the messages they understand and ignore
well-formed traffic for other endpoint frame families. Route establishment picks
out discovery introduction frames; replication transport picks out delivery
frames.

`RouteEstablishmentComponent` provides verified discovery route updates through
the discovery route publication port.

### 3.4 Replication Adapter

The replication adapter requires the discovery route publication port and
provides `RouteDiscoveryPort<TransportRouteKey>`.

It owns the replication-specific mapping from verified discovery routes to
transport route candidates, including:

- `TransportRouteKey::Udp`
- `DatagramRouteScope::Unicast`
- local endpoint bind selection
- route preference rank
- route sharing policy

Route establishment does not own those replication route policy decisions.

## 4. Messages

### 4.1 `Peer`

`Peer` is a plaintext peer announcement.

It contains:

- `instance_uuid`
- `listening_on`

It must not contain member ids or group ids. A receiver may track every
Flotsync-speaking instance it observes, but replication route publication is
based only on verified introductions.

### 4.2 `IntroductionRequest`

`IntroductionRequest` is a plaintext receiver-generated probe sent to one
advertised `listening_on` endpoint.

It contains:

- `request_nonce`

`request_nonce` must be fresh for each probe attempt. Receivers should use at
least 128 bits of randomness.

Responders send the `Introduction` back to the datagram source of the
`IntroductionRequest`. The request intentionally has no plaintext response
redirection field; callers choose the desired return path by choosing the
outbound socket or interface.

### 4.3 `Introduction`

`Introduction` is the response to one `IntroductionRequest`.

It contains:

- `instance_uuid`
- `request_nonce`
- `claims`

The top-level `instance_uuid` and `request_nonce` are duplicated into each
signed claim. They exist at the top level for cheap rejection and logging, while
the signed claim fields are authoritative for verification.

### 4.4 `IntroductionClaimPayload`

`IntroductionClaimPayload` is the signable payload for one probed route.

It contains:

- `member_id`
- `key_fingerprint`
- `instance_uuid`
- `request_nonce`
- `route`
- `group_ids`

`member_id` and `key_fingerprint` are signed selector fields. Receivers use
them to find the exact stored public key material that must verify the claim.
`key_fingerprint` is the raw 32 byte `KeyFingerprint` of the member public key
bundle.

`group_ids` contains one or more 16 byte replication group ids. Repeating groups
inside one claim is invalid. The same member may appear in several signed claims
when the route context differs.

### 4.5 `SignedIntroductionClaim`

`SignedIntroductionClaim` contains one encoded `IntroductionClaimPayload` and a
`DiscoverySignature`. Identity selectors live inside the signed payload, not in
outer duplicated fields.

For the first release, the only valid signature scheme is `SCHEME_ED25519PH`.

## 5. Signed Claim Payload

The signature proves control of the claimed member identity key for the exact
route probe being answered.

The signature is computed over `SignedIntroductionClaim.claim_payload` exactly
as received. `claim_payload` is the protobuf encoding of one
`IntroductionClaimPayload`.

Receivers first decode a conservative borrowed view of `claim_payload` only far
enough to extract `member_id` and `key_fingerprint`. Those fields are untrusted
key selectors at this point. The receiver uses the exact `(member_id,
key_fingerprint)` pair to find stored public key material and verifies the
signature over the original `claim_payload` bytes. It must not decode and
re-encode the claim for verification.

After signature verification, receivers fully decode the payload and confirm
that the fully decoded `member_id` and `key_fingerprint` match the pre-verification
selectors. Unknown fields may be ignored within the bounded conservative decode
limits because the original payload bytes, including unknown fields, are covered
by the signature.

Receivers must reject a claim when:

- `member_id` is empty or malformed
- `key_fingerprint` is not exactly 32 bytes
- `claim_payload` does not decode as an `IntroductionClaimPayload`
- the conservative selector decode exceeds its recursion, unknown-field, or
  payload-size limits
- `instance_uuid` is not exactly 16 bytes
- `instance_uuid` does not match the active probe or top-level `Introduction`
- `request_nonce` is empty or does not match the active probe
- `route` does not match the endpoint being probed
- `group_ids` is empty
- any `group_id` is not exactly 16 bytes
- any group id is repeated inside the claim
- the signature scheme is unsupported
- no stored, unblocked public key material exists for the exact `(member_id,
  key_fingerprint)` pair
- the signature over `claim_payload` does not verify against that exact public
  signing key
- the fully decoded `member_id` or `key_fingerprint` differs from the
  pre-verification selector

## 6. Receiver-Driven Exchange

### 6.1 Peer Announcement Handling

When route establishment receives a decoded peer-announcement observation, it
records every advertised route as `Known` internal state.

No replication route is published from a peer announcement alone.

### 6.2 Introduction Probe

For each route it wants to verify, the tracker sends an `IntroductionRequest` to
the advertised endpoint with a fresh `request_nonce`.

The tracker sends from the current runtime endpoint socket supplied by the setup
layer. It records the probed endpoint, expected instance id, and nonce until the
probe completes or times out.

### 6.3 Introduction Verification

When an `Introduction` response arrives, the tracker first checks:

- source endpoint matches the active probe
- top-level `instance_uuid` matches the expected peer instance
- top-level `request_nonce` matches the active probe

Then it verifies each signed claim independently.

Accepted claims are checked against the shared group membership snapshot. A
claim is relevant only when it names a group that the local runtime knows and
where `member_id` is a member. The same membership snapshot supplies the local
group ids placed into outgoing claims for groups hosted by the local member.

Publishing a route candidate from a verified claim requires
`MemberRoutePublication` authority for the exact `(member_id, key_fingerprint)`
pair. This authority covers only publishing the discovered route for that member
identity. It does not grant signature-verification capability, replication
runtime traffic authority, bootstrap activation, group installation, member
introduction, or trust promotion.

### 6.4 Freshness Refresh

The sender does not provide route TTLs.

The tracker owns:

- introduction refresh interval
- probe timeout
- reachable lease duration
- stale grace period
- retry/backoff after failed probes

When the reachable lease expires, the tracker performs the full
`IntroductionRequest` / `Introduction` exchange again. It does not extend
reachability from passive peer announcements alone because reachability and group
memberships may have changed.

## 7. Tracking State

### 7.1 `Unknown`

There is no active record for an instance or endpoint.

`Unknown` is normally represented by absence from the tracker table.

### 7.2 `Known`

A plaintext `Peer` announcement advertised an endpoint, but no signed introduction has
verified the endpoint for a local group member.

`Known` is internal tracker state only.

### 7.3 `Reachable`

At least one signed introduction claim verified for the probed endpoint and
matched the local shared group membership snapshot.

Only `Reachable` state may publish usable peer routes to replication.

### 7.4 `Stale`

A previously `Reachable` endpoint failed refresh, timed out, or exceeded its
receiver-owned reachable lease.

`Stale` is internal tracker state. The tracker must withdraw any previously
published replication route for members that no longer have a reachable claim.

## 8. Route Publication

For each accepted claim that matches local shared group membership, route
establishment includes a route in the next discovery route update for the
claimed member.

The discovery route publication surface exposes the current reachable route list
per member. It is intentionally separate from the route-establishment algorithm
so other verified discovery sources can publish through the same boundary.

The replication adapter maps each accepted UDP discovery route to:

- `TransportRouteKey::Udp`
- `UdpRouteKey.remote_addr` from the verified route endpoint
- `UdpRouteKey.scope = DatagramRouteScope::Unicast`
- `UdpRouteKey.local_bind` set to the runtime's bound local delivery endpoint
- `RouteSharingKind::Exclusive`

`DiscoveryRouteUpdate` exposes the current reachable route list, not the
tracker's internal reachability classifications. When a route goes stale, the
tracker publishes a fresh route list without that route. When a member loses its
last reachable verified route, the tracker publishes a `PeerRoutes` update for
that member with no routes, causing group broadcast and reliable delivery to
drop the direct route.

Peers that do not share any local group remain visible only to discovery
diagnostics and must not be exposed through the replication
`RouteDiscoveryPort`.

## 9. Failure Handling

Malformed peer announcements are ignored.

Malformed introduction responses, unsupported signatures, failed signatures,
nonce mismatches, route mismatches, and claims outside local shared group
membership do not publish routes.

If a previously reachable member's refresh fails, the tracker moves the
endpoint/member route to `Stale` and withdraws the published route.

Probe failures should be retried according to receiver-owned backoff policy so a
temporarily unavailable peer can become reachable again after later peer announcements or
scheduled retries.
