---
type: Protocol
title: Flotsync Security MVP
description: Defines the initial Flotsync security model for identities, group secrets, encryption, and signatures.
status: draft
---

# Flotsync Security MVP

This document records the high-level design decisions for the first
`flotsync_security` slice. It is intentionally a baseline for review and
implementation, not a full protocol specification.

## 1. Scope

The MVP secures replication delivery envelopes by default:

- runtime payloads are encrypted before they leave the replication runtime
- delivery envelopes are signed by the claimed sender
- group secrets are installed during bootstrap and stored with group metadata,
  encrypted by a device-local application database secret
- local identities and trusted peer identities come from external key files

The first pass does not try to solve discovery trust, key rotation, revocation,
certificate infrastructure, relay session encryption, or migration UX.

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

For this MVP slice, the replicated-checklist example reads setup inputs from
its application config before replication starts: a local store-secret profile,
a local private JWKS path, and trusted public JWKS paths.
`ensure_configured_group` parses and validates those files, provisions the
replication store, and then starts replication.

The replicated-checklist store-secret profile is scoped to the example
application id and selects a device-local store-secret slot. The current
implementation stores that secret through OS-backed local storage and creates it
on first run. The profile is intentionally not tied to member identity, so later
multi-identity stores can share the same encrypted-store secret.

Replication runtime reads provisioned security state from `ReplicationStore`
with normal group metadata.

Rationale: the project should not grow multiple independent config parsers for
the same application configuration, and security state that belongs to a group
should enter runtime through the same store path as the rest of the group state.

## 4. Identity Keys

Identity key files use JWKS/JWK, parsed through `jose-jwk`.

Each local private identity file contains two OKP keys:

- Ed25519 identity keys used with Ed25519ph for signing and signature verification
- X25519 for public-key encryption and HPKE key transport

The matching public JWKS contains the two public keys and is intended to be easy
to copy to trusted peers.

Rationale: signing and encryption keys have different roles and compromise
properties, but a single identity key file keeps MVP configuration small.

## 5. Key Generation

`flotsync_security` provides helper functions to generate a local private JWKS
and its matching public JWKS for a `MemberIdentity`.

The replicated-checklist example should use these helpers so users can create
usable key files without external crypto tooling.

Rationale: relying on manual OpenSSL or ad hoc key generation would make the
first secure example harder to run and easier to misconfigure.

## 6. Trust Model

The MVP uses trusted public JWKS files supplied during replicated-checklist
setup.

Bootstrap messages also carry member public keys. For now, recipients validate
bootstrap-carried keys against the public keys provisioned during setup when
both are available, and reject mismatches.

Rationale: this preserves the long-term bootstrap shape while avoiding an
unauthenticated "trust whatever the bootstrap says" model.

## 7. Group Secrets

Group symmetric keys belong to the replication group, not to files.

Each local store keeps sensitive group-security material in encrypted columns or
an opaque encrypted BLOB next to the existing `replication_groups` metadata. The
material is encrypted at rest with a device-local application database secret.

For this MVP slice, replicated-checklist loads or creates that database secret
through OS-backed secure storage via the `keyring` crate. The shared
static-group secret still comes from plaintext application config during setup
and is derived with an example-local domain-separated hash. Long term, static
group secret provisioning should be replaced by the next setup shape.

The stored group-security material includes the group symmetric key, cipher
suite metadata, and member public keys needed to verify and open group traffic
without a global trust lookup.

The group id is the group key epoch. Membership or key changes create a new
group id through migration rather than mutating the old group's key material.

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
is digest-prefix collision probability, which is acceptable for this MVP under
the message-id uniqueness requirement.

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
multi-message session state out of the first slice.

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

## 13. Deferred Work

The following are deliberately outside this MVP and tracked separately:

- key rotation and revocation
- passphrase-protected identity files
- OS keyring storage for the local database secret
- discovery-published public keys
- certificate or PKI-based trust
- relay or TCP session encryption
- secure migration UX

Rationale: each item affects user experience or long-term lifecycle semantics
enough to deserve its own design pass after the secure envelope foundation is in
place.
