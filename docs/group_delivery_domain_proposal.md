---
type: Decision
title: Group Delivery Domain Proposal
description: Proposes shared group delivery domain boundaries and crate placement decisions.
status: draft
---

# Group Delivery Domain Proposal

This document is the review companion for `flotsync-biq`.

The goal is to make the delivery-domain model concrete enough to review and use
inside the crate before we wire it into runtime logic or commit to transport
bindings in `flotsync_io`.

## 1. Scope

This proposal covers:

- shared delivery-domain identifiers and state types
- `GroupBroadcast` envelope and work-item shapes
- `ReliableDelivery` envelope and mailbox shapes
- Kompact-style port and message contracts between replication, discovery, and
  the delivery domain
- async storage contracts for durable work only

This proposal intentionally does not cover:

- integration into existing `flotsync_replication` modules
- wire codec generation or `flotsync_messages` placement
- concrete UDP/TCP actor wiring

## 2. File Layout

The current code lives in:

- `flotsync_replication/src/delivery/mod.rs`
- `flotsync_replication/src/delivery/shared.rs`
- `flotsync_replication/src/delivery/group_broadcast.rs`
- `flotsync_replication/src/delivery/reliable_delivery.rs`
- `flotsync_replication/src/delivery/contracts.rs`

## 3. Shared Domain Decisions

### 3.1 Envelope Identity

The shared key shape is:

- work scope
- `message_id`
- logical `route_id`

That matches the queue-model doc and gives us a stable key for dedupe, retries,
and in-memory route bookkeeping.

### 3.2 Route Terminology

The proposal uses `route` everywhere, not `target`.

`RouteEndpoint` stays deliberately minimal:

- `Peer(MemberIdentity)`
- `Relay(RelayIdentity)`

`RelayIdentity` is currently just an alias to `MemberIdentity`. That is an
explicit temporary choice so we do not invent a relay-id abstraction before the
transport/discovery task proves we need one.

### 3.3 Logical Routes Versus Discovery Routes

The proposal now separates:

- logical routes owned by the scheduler
- opaque discovery-published send routes used to actually reach peers/relays

The scheduler does not know whether one send route is UDP, TCP, multicast,
broadcast, mailbox, or something else.

The only thing it can do is compare send routes for equality and build coverage
classes from that. That is the intended mechanism for handling:

- multicast/broadcast
- several peers sharing one network path
- several peers or relays sharing one relay-facing path

### 3.4 Active State and Cleanup

The proposal keeps the queue model intact:

- active routes use `Queued`, `AttemptingDirect`, `AwaitingRelayStore`, and
  `PendingRoute`
- `Delivered`, `StoredAtRelay`, and `Expired` are terminal outcomes
- active route entries are removed once a terminal outcome is reached
- terminal outcomes are not part of the persisted durable state

## 4. Group Broadcast Proposal

`GroupBroadcastSubmit` is the replication-to-broadcast boundary.

It carries:

- the local `delivery_class`
- the immutable `GroupMessageEnvelope`
- no explicit retention policy

`GroupMessageEnvelope` is now split explicitly into:

- plaintext header
- encrypted payload
- plaintext signed footer

Group messages are also sender-signed in this proposal.

`GroupBroadcastWorkItem` remains the in-memory expansion result:

- one immutable submit request
- active member-route records
- active relay-route records
- submission timestamp

`GroupMessageRef` was removed from the proposal. It can come back later if the
relay design actually needs it.

## 5. Reliable Delivery Proposal

The reliable-delivery side reuses the same shared route/state substrate where it
actually matches, but it keeps the more explicit bootstrap metadata:

- sender signatures on the envelope
- recipient-signed completion acks

`ReliableDeliveryWorkItem` therefore tracks:

- one recipient route
- zero or more relay mailbox routes
- recipient-ack status

Completion is not derived from direct-send acceptance or relay storage.
Completion requires `RecipientAck`.

The mailbox model stays intentionally small:

- `MailboxFetch`
- `MailboxBatch`
- `MailboxAck`
- `MailboxItem`

The relay implementation remains abstract.

## 6. Internal Contract Proposal

The contract split in `contracts.rs` is now:

- `GroupBroadcastPort`
- `ReliableDeliveryPort`
- `ResolvedSendRoute`
- `RouteSend` / `RouteSendResult`
- `DiscoveryRouteUpdate`
- `DeliveryWorkStore`

The intent is:

- replication submits work through fire-and-forget Kompact-port requests
- both delivery ports emit inbound `Deliver(...)` indications for messages
  received from the network
- discovery publishes opaque send routes instead of being queried through async
  traits
- the scheduler groups peers by equal send routes without inspecting transport
  details
- route sends use actor-style `Ack`/`Nack` responses with a caller-supplied
  correlation id and reply recipient
- only the durable work records remain behind an async store boundary

This keeps `flotsync-biq` transport-agnostic while still giving
`flotsync-187` a concrete semantic boundary to map onto `flotsync_io`.

## 7. Crate Placement Decision

For now these types stay in `flotsync_replication`.

Reasoning:

- they are internal domain-model scaffolding, not settled wire contracts
- the transport task may still split some of them differently across crates
- keeping them local makes the review cheaper and avoids a premature
  `flotsync_messages` dependency decision

## 8. Deliberately Deferred Questions

This proposal still leaves these open:

- whether relays need a dedicated identity type separate from `MemberIdentity`
- whether `DeliveryReceipt` should later become signed or remain advisory only
- whether the eventual compiled version should keep using actual
  `kompact::Port` definitions in this crate or a smaller crate-local adapter
  layer
