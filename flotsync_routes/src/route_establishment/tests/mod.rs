//! Route-establishment component tests.

use super::{
    component::{claim_matches_group_memberships, local_claim_group_ids},
    *,
};
use crate::{
    DatagramRouteScope,
    DiscoveryRouteUpdate,
    InboundTransportMeta,
    RouteDiscoveryPort,
    RouteEndpointBinding,
    RouteEndpointLifecycle,
    RouteEndpointUnavailableReason,
    RoutePreferenceRank,
    RouteSharingKind,
    RouteTransportActorMessage,
    RouteTransportInboundDeliver,
    RouteTransportSend,
    TransportRouteKey,
    UdpRouteKey,
    key_material_discovery::{FetchKeyMaterial, KeyMaterialDiscoveryPort},
    protocol::{DiscoveryEndpointFrameView, decode_endpoint_discovery_frame_from_buf},
    test_support::{
        RouteTransportRecorderComponent,
        TestRouteTransportPort,
        assert_udp_transport_route,
        encode_transport_payload,
        endpoint_payload,
        member,
    },
};
use flotsync_core::{
    GroupId,
    member::TrieSet,
    membership::{GroupMembers, GroupMemberships, SharedGroupMemberships},
};
use flotsync_discovery::{
    endpoint_selection::EndpointSelection,
    protocol::DiscoveryRoute,
    services::PeerAnnouncementObserved,
};
use flotsync_io::{
    prelude::{IoPayload, MAX_UDP_PAYLOAD_BYTES, SocketId},
    test_support::{
        build_test_kompact_system,
        eventually_component_state,
        kill_component,
        start_component,
    },
};
use flotsync_messages::{
    buffa::{Message as _, MessageField},
    discovery as discovery_proto,
    proto::{DecodeProto, EncodeProto},
    wire::{
        group_id_to_wire_bytes,
        member_identity_to_wire_format,
        uuid_from_wire_bytes,
        uuid_to_wire_bytes,
    },
};
use flotsync_security::{FrameSignature, KeyFingerprint, PublicKeyBundle, SIGNATURE_LENGTH};
use flotsync_utils::{
    BoxError,
    kompact_testing::{PortTesterComponent, PortTestingExt, PortTestingRefExt},
};
use futures_util::FutureExt as _;
use kompact::prelude::*;
use std::{
    cell::Cell,
    collections::HashSet,
    io,
    net::SocketAddr,
    sync::{Arc, mpsc},
    time::Duration,
};
use uuid::Uuid;

mod claims;
mod fixtures;
mod lifecycle;
mod watches;
