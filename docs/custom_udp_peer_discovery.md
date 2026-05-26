# Custom UDP Peer Discovery Protocol

This document specifies the custom non-mDNS peer-discovery protocol used by the
replication runtime route source.

## 1. Scope

This protocol discovers local-network Flotsync peers and turns verified peer
introductions into replication route candidates.

It defines:

- plaintext peer beacons
- receiver-driven introduction probes
- signed introduction claims
- receiver-owned freshness and retry policy
- route publication rules for `RouteDiscoveryPort`

It does not define:

- mDNS discovery
- signed `Peer` beacons
- signed `IntroductionRequest` messages
- sender-provided route TTLs
- persistent discovery sessions
- sequence numbers or long replay windows

## 2. Terminology

### 2.1 Peer Instance

A peer instance is one running Flotsync process identified by a 16 byte
`instance_uuid`.

The instance id is not a replication identity. It is only used to correlate
beacons, probes, and introductions from the same running discovery component.

### 2.2 Listening Route

A listening route is one endpoint advertised in `Peer.listening_on`.

For the current custom UDP path, replication consumes only UDP unicast routes.
The protocol shape remains explicit about route protocol so TCP or other route
families can be introduced later.

### 2.3 Introduction Claim

An introduction claim says that one `member_id` is reachable through one probed
route for one or more replication groups.

Claims are signed per member identity. This lets one discovery component
advertise different identities and group memberships on different routes without
making the whole introduction depend on a single global signer.

### 2.4 Runtime Endpoint Frame

The replication endpoint carries `flotsync.endpoint.v1.EndpointFrame` messages.
Delivery frames and discovery introduction frames share this envelope because
introduction verifies the same endpoint that later carries delivery traffic.

`Peer` beacons are not carried inside `EndpointFrame`; they arrive on the
custom discovery beacon port.

## 3. Messages

### 3.1 `Peer`

`Peer` is a plaintext beacon.

It contains:

- `instance_uuid`
- `listening_on`

It must not contain member ids or group ids. A receiver may track every
Flotsync-speaking instance it observes, but replication route publication is
based only on verified introductions.

### 3.2 `IntroductionRequest`

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

### 3.3 `Introduction`

`Introduction` is the response to one `IntroductionRequest`.

It contains:

- `instance_uuid`
- `request_nonce`
- `claims`

The top-level `instance_uuid` and `request_nonce` are duplicated into each
signed claim. They exist at the top level for cheap rejection and logging, while
the signed claim fields are authoritative for verification.

### 3.4 `IntroductionClaimPayload`

`IntroductionClaimPayload` is the signable payload for one probed route.

It contains:

- `instance_uuid`
- `request_nonce`
- `route`
- `group_ids`

`group_ids` contains one or more 16 byte replication group ids. Repeating groups
inside one claim is invalid. The same member may appear in several signed claims
when the route context differs.

### 3.5 `SignedIntroductionClaim`

`SignedIntroductionClaim` contains one claimed `member_id`, one encoded
`IntroductionClaimPayload`, and a `DiscoverySignature`.

For the first release, the only valid signature scheme is `SCHEME_ED25519PH`.

## 4. Signed Claim Payload

The signature proves control of the claimed member identity key for the exact
route probe being answered.

The signature is computed over `SignedIntroductionClaim.claim_payload` exactly
as received. `claim_payload` is the protobuf encoding of one
`IntroductionClaimPayload`.

Receivers use the outer `SignedIntroductionClaim.member_id` as an untrusted key
selector. They must not trust or act on that member id or any decoded payload
field until the signature over the original `claim_payload` bytes has been
verified. They must not decode and re-encode the claim for verification.

Receivers must reject a claim when:

- `member_id` is empty or malformed
- `claim_payload` does not decode as an `IntroductionClaimPayload`
- `instance_uuid` is not exactly 16 bytes
- `instance_uuid` does not match the active probe or top-level `Introduction`
- `request_nonce` is empty or does not match the active probe
- `route` does not match the endpoint being probed
- `group_ids` is empty
- any `group_id` is not exactly 16 bytes
- any group id is repeated inside the claim
- the signature scheme is unsupported
- the signature over `claim_payload` does not verify against the trusted public
  signing key for `member_id`

## 5. Receiver-Driven Exchange

### 5.1 Beacon Handling

When a tracker receives a plaintext `Peer`, it records every advertised route as
`Known` internal state.

No replication route is published from a beacon alone.

### 5.2 Introduction Probe

For each route it wants to verify, the tracker sends an `IntroductionRequest` to
the advertised endpoint with a fresh `request_nonce`.

The tracker records the probed endpoint, expected instance id, and nonce until
the probe completes or times out.

### 5.3 Introduction Verification

When an `Introduction` response arrives, the tracker first checks:

- source endpoint matches the active probe
- top-level `instance_uuid` matches the expected peer instance
- top-level `request_nonce` matches the active probe

Then it verifies each signed claim independently.

Accepted claims are filtered against local replication group membership. A claim
is relevant only for groups that the local runtime knows and where `member_id`
is a member.

### 5.4 Freshness Refresh

The sender does not provide route TTLs.

The tracker owns:

- introduction refresh interval
- probe timeout
- reachable lease duration
- stale grace period
- retry/backoff after failed probes

When the reachable lease expires, the tracker performs the full
`IntroductionRequest` / `Introduction` exchange again. It does not extend
reachability from passive beacons alone because reachability and group
memberships may have changed.

## 6. Tracking State

### 6.1 `Unknown`

There is no active record for an instance or endpoint.

`Unknown` is normally represented by absence from the tracker table.

### 6.2 `Known`

A plaintext `Peer` beacon advertised an endpoint, but no signed introduction has
verified the endpoint for a local group member.

`Known` is internal tracker state only.

### 6.3 `Reachable`

At least one signed introduction claim verified for the probed endpoint and
intersected local group membership.

Only `Reachable` state may publish usable peer routes to replication.

### 6.4 `Stale`

A previously `Reachable` endpoint failed refresh, timed out, or exceeded its
receiver-owned reachable lease.

`Stale` is internal tracker state. The tracker must withdraw any previously
published replication route for members that no longer have a reachable claim.

## 7. Route Publication

For each accepted claim whose `group_ids` intersect local group membership, the
tracker includes a route in the next `DiscoveryRouteUpdate::PeerRoutes` for the
claimed member.

For the custom UDP route source, each published route candidate should use:

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
diagnostics and must not be exposed through `RouteDiscoveryPort`.

## 8. Failure Handling

Malformed beacons are ignored.

Malformed introduction responses, unsupported signatures, failed signatures,
nonce mismatches, route mismatches, and non-intersecting claims do not publish
routes.

If a previously reachable member's refresh fails, the tracker moves the
endpoint/member route to `Stale` and withdraws the published route.

Probe failures should be retried according to receiver-owned backoff policy so a
temporarily unavailable peer can become reachable again after later beacons or
scheduled retries.
