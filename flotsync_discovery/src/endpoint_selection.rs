//! Selection of concrete local endpoints for LAN discovery.
//!
//! The runtime may bind its UDP endpoint to a concrete address or to a wildcard address. Concrete
//! binds are already explicit local endpoints, so selection returns exactly that endpoint. Wildcard
//! binds need interface inspection: the policy selects addresses from interfaces that are up, match
//! the required state flags, and do not belong to excluded categories such as loopback or
//! point-to-point links.

use bitflags::bitflags;
use pnet_datalink as datalink;
use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

/// Source for snapshots of local network interfaces.
pub trait InterfaceSnapshotProvider {
    /// Return the current local interface snapshot.
    fn snapshot(&self) -> InterfaceSnapshot;
}

/// Concrete local UDP endpoints selected for discovery announcements and signed introductions.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EndpointSelection {
    /// Concrete UDP socket addresses that local peers may try to verify.
    pub endpoints: BTreeSet<SocketAddr>,
}

impl EndpointSelection {
    /// Build a selection from an endpoint iterator, sorting and deduplicating the addresses.
    #[must_use]
    pub fn from_endpoints(endpoints: impl IntoIterator<Item = SocketAddr>) -> Self {
        Self {
            endpoints: endpoints.into_iter().collect(),
        }
    }

    /// Return whether no concrete local endpoints are currently selected.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }
}

/// Interface snapshot provider backed by `pnet_datalink::interfaces`.
#[derive(Clone, Copy, Debug, Default)]
pub struct PnetInterfaceSnapshotProvider;

impl InterfaceSnapshotProvider for PnetInterfaceSnapshotProvider {
    fn snapshot(&self) -> InterfaceSnapshot {
        InterfaceSnapshot {
            interfaces: datalink::interfaces()
                .into_iter()
                .map(InterfaceSnapshotEntry::from_pnet)
                .collect(),
        }
    }
}

/// Current local network interface snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InterfaceSnapshot {
    /// Interfaces visible to the process.
    pub interfaces: Vec<InterfaceSnapshotEntry>,
}

impl InterfaceSnapshot {
    /// Build a snapshot from explicit interface entries.
    #[must_use]
    pub fn new(interfaces: impl IntoIterator<Item = InterfaceSnapshotEntry>) -> Self {
        Self {
            interfaces: interfaces.into_iter().collect(),
        }
    }
}

/// One local network interface and the IP addresses assigned to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterfaceSnapshotEntry {
    /// Stable OS-level interface name where available.
    pub name: String,
    /// Interface state and category flags used by endpoint selection.
    pub flags: InterfaceFlags,
    /// Assigned IP addresses.
    pub addresses: Vec<InterfaceAddress>,
}

impl InterfaceSnapshotEntry {
    /// Build one interface snapshot entry.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        flags: InterfaceFlags,
        addresses: impl IntoIterator<Item = InterfaceAddress>,
    ) -> Self {
        Self {
            name: name.into(),
            flags,
            addresses: addresses.into_iter().collect(),
        }
    }

    /// Convert from a `pnet_datalink` interface without leaking that type into callers.
    fn from_pnet(interface: datalink::NetworkInterface) -> Self {
        let mut flags = InterfaceFlags::empty();
        if interface.is_up() {
            flags.insert(InterfaceFlags::UP);
        }
        if interface_is_running(&interface) {
            flags.insert(InterfaceFlags::RUNNING);
        }
        if interface.is_loopback() {
            flags.insert(InterfaceFlags::LOOPBACK);
        }
        if interface.is_point_to_point() {
            flags.insert(InterfaceFlags::POINT_TO_POINT);
        }
        Self {
            name: interface.name,
            flags,
            addresses: interface
                .ips
                .into_iter()
                .map(|network| InterfaceAddress {
                    ip: network.ip(),
                    prefix: network.prefix(),
                })
                .collect(),
        }
    }
}

bitflags! {
    /// Interface state and category flags used by endpoint selection.
    ///
    /// Bits shared with [`EndpointSelectionPolicy`] intentionally have the same raw values. This
    /// lets selection compare observed interface categories against allowed policy categories
    /// using flag intersections instead of bespoke boolean chains.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct InterfaceFlags: u16 {
        /// Interface is administratively up.
        const UP = 0b0000_0001;
        /// Interface is currently running, where the platform exposes that flag.
        const RUNNING = RUNNING_FLAG_BITS;
        /// Interface represents local loopback.
        const LOOPBACK = LOOPBACK_FLAG_BITS;
        /// Interface is point-to-point rather than broadcast/LAN-like.
        const POINT_TO_POINT = POINT_TO_POINT_FLAG_BITS;
    }
}

/// One IP address assigned to an interface.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InterfaceAddress {
    /// Assigned unicast address.
    pub ip: IpAddr,
    /// Network prefix length reported with the address.
    pub prefix: u8,
}

impl InterfaceAddress {
    /// Build one interface address entry.
    #[must_use]
    pub const fn new(ip: IpAddr, prefix: u8) -> Self {
        Self { ip, prefix }
    }
}

bitflags! {
    /// Policy flags controlling endpoint selection from wildcard binds.
    ///
    /// The default LAN-oriented policy requires interfaces to be `RUNNING` and excludes loopback,
    /// point-to-point, IPv4 link-local, and IPv6 link-local endpoints. Set an `INCLUDE_*` flag to
    /// opt into that category. Remove `REQUIRE_RUNNING` to allow interfaces that are up but do not
    /// currently report the running flag.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct EndpointSelectionPolicy: u16 {
        /// Require interfaces to report the running flag before their addresses are selected.
        const REQUIRE_RUNNING = RUNNING_FLAG_BITS;
        /// Include loopback interfaces and addresses.
        const INCLUDE_LOOPBACK = LOOPBACK_FLAG_BITS;
        /// Include point-to-point interfaces.
        const INCLUDE_POINT_TO_POINT = POINT_TO_POINT_FLAG_BITS;
        /// Include IPv4 link-local addresses.
        const INCLUDE_IPV4_LINK_LOCAL = IPV4_LINK_LOCAL_FLAG_BITS;
        /// Include IPv6 link-local addresses.
        const INCLUDE_IPV6_LINK_LOCAL = IPV6_LINK_LOCAL_FLAG_BITS;
    }
}

impl EndpointSelectionPolicy {
    /// Select concrete UDP endpoints for a local endpoint bind.
    ///
    /// Port `0` is not remotely reachable and therefore selects no endpoints. A concrete bind
    /// selects only itself, because the caller already chose the externally meaningful local
    /// address and expanding it through interface discovery would advertise unrelated endpoints.
    /// A wildcard bind selects addresses from the supplied interface snapshot when:
    ///
    /// - the interface is up;
    /// - all required policy flags, currently only `REQUIRE_RUNNING`, are present;
    /// - interface categories such as loopback or point-to-point are allowed by policy; and
    /// - the address family matches the bind and address categories such as link-local are allowed.
    #[must_use]
    pub fn select_endpoints(
        self,
        local_bound_addr: SocketAddr,
        interfaces: &InterfaceSnapshot,
    ) -> EndpointSelection {
        if local_bound_addr.port() == 0 {
            EndpointSelection::default()
        } else if local_bound_addr.ip().is_unspecified() {
            self.select_wildcard_endpoints(local_bound_addr, interfaces)
        } else {
            EndpointSelection::from_endpoints([local_bound_addr])
        }
    }

    /// Select concrete interface addresses for a wildcard endpoint bind.
    fn select_wildcard_endpoints(
        self,
        local_bound_addr: SocketAddr,
        interfaces: &InterfaceSnapshot,
    ) -> EndpointSelection {
        let mut endpoints = BTreeSet::new();
        for interface in &interfaces.interfaces {
            if !self.interface_conforms_to_policy(interface) {
                continue;
            }
            for address in &interface.addresses {
                if let Some(endpoint) = self.endpoint_for_address(local_bound_addr, *address) {
                    endpoints.insert(endpoint);
                }
            }
        }
        EndpointSelection { endpoints }
    }

    /// Return whether an interface state/category snapshot conforms to this policy.
    fn interface_conforms_to_policy(self, interface: &InterfaceSnapshotEntry) -> bool {
        if !interface.flags.contains(InterfaceFlags::UP) {
            return false;
        }

        let required_flags = InterfaceFlags::from_bits_retain(
            self.bits() & EndpointSelectionPolicy::REQUIRE_RUNNING.bits(),
        );
        if !interface.flags.contains(required_flags) {
            return false;
        }

        let observed_categories = EndpointSelectionPolicy::from_bits_retain(
            interface.flags.bits() & POLICY_CONTROLLED_INTERFACE_FLAG_BITS,
        );
        observed_categories.difference(self).is_empty()
    }

    /// Convert an interface address into a concrete endpoint if it conforms to this policy.
    fn endpoint_for_address(
        self,
        local_bound_addr: SocketAddr,
        address: InterfaceAddress,
    ) -> Option<SocketAddr> {
        if same_address_family(local_bound_addr.ip(), address.ip)
            && self.address_conforms_to_policy(address.ip)
        {
            Some(SocketAddr::new(address.ip, local_bound_addr.port()))
        } else {
            None
        }
    }

    /// Return whether an IP address state/category snapshot conforms to this policy.
    fn address_conforms_to_policy(self, address: IpAddr) -> bool {
        let Some(categories) = address_policy_categories(address) else {
            return false;
        };
        categories.difference(self).is_empty()
    }
}

impl Default for EndpointSelectionPolicy {
    fn default() -> Self {
        Self::REQUIRE_RUNNING
    }
}

#[cfg(feature = "kompact-runtime")]
pub use kompact_port::EndpointSelectionPort;

/// Raw bit shared by interface and endpoint-selection policy flags for running interfaces.
const RUNNING_FLAG_BITS: u16 = 0b0000_0010;
/// Raw bit shared by interface and endpoint-selection policy flags for loopback endpoints.
const LOOPBACK_FLAG_BITS: u16 = 0b0000_0100;
/// Raw bit shared by interface and endpoint-selection policy flags for point-to-point endpoints.
const POINT_TO_POINT_FLAG_BITS: u16 = 0b0000_1000;
/// Raw policy bit for IPv4 link-local addresses.
const IPV4_LINK_LOCAL_FLAG_BITS: u16 = 0b0001_0000;
/// Raw policy bit for IPv6 link-local addresses.
const IPV6_LINK_LOCAL_FLAG_BITS: u16 = 0b0010_0000;
/// Interface category bits controlled by endpoint-selection policy flags with matching values.
const POLICY_CONTROLLED_INTERFACE_FLAG_BITS: u16 = LOOPBACK_FLAG_BITS | POINT_TO_POINT_FLAG_BITS;

/// Return the selected address categories, or `None` when the address is never publishable.
fn address_policy_categories(address: IpAddr) -> Option<EndpointSelectionPolicy> {
    match address {
        IpAddr::V4(address) => ipv4_policy_categories(address),
        IpAddr::V6(address) => ipv6_policy_categories(address),
    }
}

/// Return selected IPv4 categories, or `None` when the address is never publishable.
fn ipv4_policy_categories(address: Ipv4Addr) -> Option<EndpointSelectionPolicy> {
    if address.is_unspecified() || address.is_broadcast() || address.is_multicast() {
        return None;
    }

    let mut categories = EndpointSelectionPolicy::empty();
    if address.is_loopback() {
        categories.insert(EndpointSelectionPolicy::INCLUDE_LOOPBACK);
    }
    if address.is_link_local() {
        categories.insert(EndpointSelectionPolicy::INCLUDE_IPV4_LINK_LOCAL);
    }
    Some(categories)
}

/// Return selected IPv6 categories, or `None` when the address is never publishable.
fn ipv6_policy_categories(address: Ipv6Addr) -> Option<EndpointSelectionPolicy> {
    if address.is_unspecified() || address.is_multicast() {
        return None;
    }

    let mut categories = EndpointSelectionPolicy::empty();
    if address.is_loopback() {
        categories.insert(EndpointSelectionPolicy::INCLUDE_LOOPBACK);
    }
    if address_is_ipv6_link_local(address) {
        categories.insert(EndpointSelectionPolicy::INCLUDE_IPV6_LINK_LOCAL);
    }
    Some(categories)
}

/// Return whether two IP addresses belong to the same address family.
fn same_address_family(left: IpAddr, right: IpAddr) -> bool {
    matches!(
        (left, right),
        (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_))
    )
}

/// Return whether this IPv6 address is in `fe80::/10`.
fn address_is_ipv6_link_local(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    (segments[0] & 0xffc0) == 0xfe80
}

/// Return the platform running flag when available; otherwise treat up interfaces as running.
fn interface_is_running(interface: &datalink::NetworkInterface) -> bool {
    #[cfg(unix)]
    {
        interface.is_running()
    }
    #[cfg(not(unix))]
    {
        interface.is_up()
    }
}

#[cfg(feature = "kompact-runtime")]
mod kompact_port {
    //! Kompact port for publishing endpoint-selection updates.

    use super::EndpointSelection;

    /// Kompact port carrying the current selected local discovery endpoints.
    pub struct EndpointSelectionPort;

    impl kompact::prelude::Port for EndpointSelectionPort {
        type Indication = EndpointSelection;
        type Request = kompact::prelude::Never;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn concrete_bound_address_is_selected_without_interface_snapshot() {
        let policy = EndpointSelectionPolicy::default();
        let local_addr = SocketAddr::from(([192, 168, 1, 20], 45_100));

        let selection = policy.select_endpoints(local_addr, &InterfaceSnapshot::default());

        assert_eq!(selection.endpoints, endpoints([local_addr]));
    }

    #[test]
    fn wildcard_bound_address_selects_plausible_lan_ipv4_endpoints() {
        let policy = EndpointSelectionPolicy::default();
        let snapshot = InterfaceSnapshot::new([
            interface(
                "lo",
                InterfaceFlags::UP | InterfaceFlags::RUNNING | InterfaceFlags::LOOPBACK,
                [addr(Ipv4Addr::LOCALHOST, 8)],
            ),
            interface(
                "en0",
                InterfaceFlags::UP | InterfaceFlags::RUNNING,
                [
                    addr(Ipv4Addr::new(192, 168, 1, 20), 24),
                    addr(Ipv4Addr::new(169, 254, 1, 2), 16),
                    addr(Ipv4Addr::new(224, 0, 0, 1), 24),
                ],
            ),
            interface(
                "down0",
                InterfaceFlags::empty(),
                [addr(Ipv4Addr::new(10, 0, 0, 5), 24)],
            ),
            interface(
                "not-running0",
                InterfaceFlags::UP,
                [addr(Ipv4Addr::new(10, 0, 0, 6), 24)],
            ),
        ]);

        let selection =
            policy.select_endpoints(SocketAddr::from(([0, 0, 0, 0], 45_100)), &snapshot);

        assert_eq!(
            selection.endpoints,
            endpoints([SocketAddr::from(([192, 168, 1, 20], 45_100))])
        );
    }

    #[test]
    fn wildcard_bound_address_can_include_not_running_interface_when_policy_allows_it() {
        let policy = EndpointSelectionPolicy::empty();
        let snapshot = InterfaceSnapshot::new([interface(
            "en0",
            InterfaceFlags::UP,
            [addr(Ipv4Addr::new(192, 168, 1, 20), 24)],
        )]);

        let selection =
            policy.select_endpoints(SocketAddr::from(([0, 0, 0, 0], 45_100)), &snapshot);

        assert_eq!(
            selection.endpoints,
            endpoints([SocketAddr::from(([192, 168, 1, 20], 45_100))])
        );
    }

    #[test]
    fn wildcard_bound_address_can_include_loopback_when_policy_allows_it() {
        let policy = EndpointSelectionPolicy::default() | EndpointSelectionPolicy::INCLUDE_LOOPBACK;
        let snapshot = InterfaceSnapshot::new([interface(
            "lo",
            InterfaceFlags::UP | InterfaceFlags::RUNNING | InterfaceFlags::LOOPBACK,
            [addr(Ipv4Addr::LOCALHOST, 8)],
        )]);

        let selection =
            policy.select_endpoints(SocketAddr::from(([0, 0, 0, 0], 45_100)), &snapshot);

        assert_eq!(
            selection.endpoints,
            endpoints([SocketAddr::from(([127, 0, 0, 1], 45_100))])
        );
    }

    #[test]
    fn wildcard_bound_address_selects_ipv6_family_only_for_ipv6_bind() {
        let policy = EndpointSelectionPolicy::default();
        let snapshot = InterfaceSnapshot::new([interface(
            "en0",
            InterfaceFlags::UP | InterfaceFlags::RUNNING,
            [
                addr(Ipv4Addr::new(192, 168, 1, 20), 24),
                InterfaceAddress::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 128),
                InterfaceAddress::new(IpAddr::V6("fd00::20".parse().unwrap()), 64),
                InterfaceAddress::new(IpAddr::V6("fe80::20".parse().unwrap()), 64),
            ],
        )]);

        let selection = policy.select_endpoints(
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 45_100),
            &snapshot,
        );

        assert_eq!(
            selection.endpoints,
            endpoints([SocketAddr::new(
                IpAddr::V6("fd00::20".parse().unwrap()),
                45_100
            )])
        );
    }

    #[test]
    fn wildcard_bound_address_without_eligible_interfaces_returns_empty_selection() {
        let policy = EndpointSelectionPolicy::default();
        let snapshot = InterfaceSnapshot::new([interface(
            "utun0",
            InterfaceFlags::UP | InterfaceFlags::RUNNING | InterfaceFlags::POINT_TO_POINT,
            [addr(Ipv4Addr::new(10, 20, 30, 40), 24)],
        )]);

        let selection =
            policy.select_endpoints(SocketAddr::from(([0, 0, 0, 0], 45_100)), &snapshot);

        assert!(selection.is_empty());
    }

    fn interface(
        name: &str,
        flags: InterfaceFlags,
        addresses: impl IntoIterator<Item = InterfaceAddress>,
    ) -> InterfaceSnapshotEntry {
        InterfaceSnapshotEntry::new(name, flags, addresses)
    }

    fn addr(address: Ipv4Addr, prefix: u8) -> InterfaceAddress {
        InterfaceAddress::new(IpAddr::V4(address), prefix)
    }

    fn endpoints(addresses: impl IntoIterator<Item = SocketAddr>) -> BTreeSet<SocketAddr> {
        addresses.into_iter().collect()
    }
}
