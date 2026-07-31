---
type: Protocol
title: Flotsync Security
description: Defines the Flotsync security model for identities, group secrets, encryption, and signatures.
status: draft
---

# Flotsync Security

This document records the high-level security protocol and ownership boundaries
implemented across `flotsync_security` and `flotsync_replication`. It is
maintained as the protocol evolves; concrete Rust types and wire definitions
remain authoritative for implementation details.

## 1. Scope

Flotsync secures replication delivery envelopes by default:

- runtime payloads are encrypted before they leave the replication runtime
- delivery envelopes are signed by the claimed sender
- group secrets are installed during bootstrap and stored with group metadata,
  encrypted by a device-local application database secret
- local private identities are encrypted in the replication store, while peer
  public identities are observed and assessed through the replication API

The security protocol does not yet define key rotation, revocation, certificate
infrastructure, relay session encryption, or migration recovery UX.

## 2. Crate Boundary

Security logic lives in a dedicated `flotsync_security` crate.

The crate owns cryptographic operations and data structures: key parsing, typed
key material, key generation helpers, signing, verification, symmetric group
encryption, HPKE bootstrap encryption, transcript construction, and crypto
errors.

It must not own runtime topology, transport, storage, or application config
parsing. `flotsync_replication` consumes `flotsync_security`; `flotsync_io` and
`flotsync_messages` remain crypto-free.

Rationale: keeping crypto behind a narrow crate boundary makes the dependency
direction clear and prevents Kompact, storage, or transport concerns from
leaking into low-level security code.

## 3. Setup Boundary

`flotsync_security` accepts typed key material and protocol inputs. It does not
parse TOML or Kompact configuration.

The replicated-checklist example reads the store path and store-secret profile
from application config. For a new or empty store, startup asks for a local
member identity and passes it to the public
`flotsync_replication::provision_local_identity` setup API. The API commits the
identity, its encrypted private bundle, and the matching public-key binding in
one store transaction. Other key operations run through the already-unlocked
replication runtime.

Setup uses `LocalIdentityProvisioningStore`, which tolerates the absence of a
local identity. A provisioned store is activated as a `ReplicationStore`; that
ready interface always exposes the authoritative identity loaded from
`local_members`. Activation rejects missing, malformed, or ambiguous stored
identities. Provisioning an already provisioned store also fails rather than
silently replacing its identity.

The encrypted local-private bundle is authoritative for the local member's key
material. Runtime security derives the matching public keys from that bundle
and idempotently restores a missing local public-key binding during startup.
An identity without its corresponding private bundle is outside the ready-store
contract; runtime loading reports that malformed custom-store state as an
internal security failure rather than offering provisioning as recovery.

The replicated-checklist store-secret profile is scoped to the example
application id and selects a device-local store-secret slot. The secret is held
in OS-backed local storage and is created only after the user accepts first-run
setup. The profile is intentionally not tied to member identity. The current
storage contract permits one local identity, while allowing several key records
for that identity in a future key-evolution design.

Replication runtime reads provisioned identity, trust, block, and group-security
state from `ReplicationStore` with normal group metadata.

Rationale: the project should not grow multiple independent config parsers for
the same application configuration, and security state that belongs to a group
should enter runtime through the same store path as the rest of the group state.

## 4. Identity Keys

Each local private identity bundle contains two OKP keys:

- Ed25519 identity keys used with Ed25519ph for signing and signature verification
- X25519 for public-key encryption and HPKE key transport

The matching identity-free public bundle contains the two public keys and is
encoded into a pasteable representation for transfer to peers. Private bundles
are encrypted in the local replication store rather than retained as
application configuration files.

Rationale: signing and encryption keys have different roles and compromise
properties, but a single identity bundle keeps the setup surface small.

## 5. Key Generation

`flotsync_security` provides helper functions to generate a local private bundle
and its matching public bundle for a `MemberIdentity`.

The replicated-checklist example uses these helpers so users can initialise
usable local identity material without external crypto tooling.

Rationale: relying on manual OpenSSL or ad hoc key generation would make the
first secure example harder to run and easier to misconfigure.

## 6. Trust Model

Flotsync stores observed member/public-key bindings, explicit local trust
evidence, and globally blocked fingerprints in the replication store. The
replicated-checklist assesses pasteable public bundles through the running
runtime before recording explicit trust for one member or blocking the bundle's
fingerprint.

Bootstrap messages also carry member public keys. Recipients evaluate those
keys against current local trust and block state before granting the authority
required by the bootstrap flow.

Rationale: this preserves the long-term bootstrap shape while avoiding an
unauthenticated "trust whatever the bootstrap says" model.

## 7. Group Secrets

Group symmetric keys belong to the replication group, not to files.

Each local store keeps sensitive group-security material in encrypted columns or
an opaque encrypted BLOB next to the existing `replication_groups` metadata. The
material is encrypted at rest with a device-local application database secret.

The replicated-checklist loads or creates that database secret through
OS-backed secure storage via the `keyring` crate. Group creation and
membership-change flows generate and distribute group secrets through the
replication runtime; the checklist does not accept a plaintext shared group
secret in application config.

The stored group-security material includes the group symmetric key, cipher
suite metadata, and member public keys needed to verify and open group traffic
without a global trust lookup.

The group id is the group key epoch. Membership or key changes create a new
group id through migration rather than mutating the old group's key material.

Initial snapshots used to activate a migrated or invited group belong to the
target group. Migration proposals may be authorised in the old group context,
but inline snapshot payloads for the new group are encrypted with the new group
key so all accepted target-group members can consume one shared representation.

Rationale: group metadata, membership, version state, and group key material are
normally needed together. Storing encrypted group-security material with the
group record keeps that lifecycle explicit.

## 8. Group Payload Encryption

Group messages use `ChaCha20-Poly1305` with a symmetric group key.

The nonce is derived from existing public envelope context: group id, frame
kind, sender identity, and delivery message id. The nonce is public information
and is not transmitted separately.

Delivery message ids are required to be unique under one group key. Nonce
derivation hashes the immutable context with SHA-256 and uses the leftmost 12
bytes as the ChaCha20-Poly1305 nonce. Fixed byte positions in a secure digest
are not treated as weaker than other fixed positions; the relevant residual risk
is digest-prefix collision probability, which is accepted under the message-id
uniqueness requirement.

The public routing header stays clear so delivery ingress can perform cheap
local-interest classification, but the same header is included as authenticated
encryption context.

Rationale: group messages are high-volume compared with bootstrap traffic, so
per-message overhead should stay small. The agreed target is the cipher's
authentication tag plus the sender signature, without repeating group-level
algorithm ids, key ids, epochs, or nonces on every message.

## 9. Signatures

Delivery envelopes are signed with Ed25519ph.

The group-message signature prehash transcript is streamed through SHA-512 and
covers a domain separator, frame kind, canonical public header, and the
ciphertext including the authentication tag.

Rationale: group ciphertexts may become large. Ed25519ph avoids building a
second contiguous signing buffer containing the full ciphertext while preserving
a standard Ed25519-family signature mode. The authentication tag proves that the
ciphertext and authenticated context were produced by someone with the group
key. The signature separately proves that the claimed sender signed this exact
sealed envelope.

## 10. Bootstrap Encryption

Reliable bootstrap and recipient-specific key transport use single-shot HPKE
with X25519.

The sender generates a fresh HPKE ephemeral key for each recipient bootstrap
envelope, transmits the encapsulated public key material, encrypts the bootstrap
payload, signs the envelope, and discards the ephemeral private key.

The bootstrap payload carries:

- group id
- ordered members
- each member's Ed25519 public signing key
- each member's X25519 public encryption key
- group cipher suite
- group symmetric key

Rationale: bootstrap happens before recipients share the group key. HPKE gives a
standard public-key encryption path for exactly that case, while keeping
multi-message session state out of the bootstrap path.

Inbound bootstrap stores accepted group-security material through the
`ReplicationStore` sensitive-column path before installing membership.

## 11. Acknowledgements

Reliable-delivery recipient acknowledgements are signed by the recipient using
the same Ed25519ph transcript convention.

The sender only treats an acknowledgement as completing work after verifying the
signature against the expected recipient public key and delivery message id.

Rationale: direct send success or relay storage is not the semantic completion
signal. The recipient must prove that it accepted the message.

## 12. Test Posture

The normal runtime has no insecure "off" mode.

Tests use crate-internal deterministic identities, deterministic group keys, and
`flotsync_security` test-support RNG helpers instead of plaintext placeholders.

Rationale: keeping a plaintext mode in the production path would complicate the
security boundary. Deterministic fixtures give tests the convenience they need
without preserving insecure runtime behaviour.

## 13. Out of Scope

The following require separate protocol and product design:

- key rotation and revocation
- certificate or PKI-based trust
- relay or TCP session encryption
- migration recovery UX

Rationale: each item affects user experience or long-term lifecycle semantics
enough to require its own design pass.
