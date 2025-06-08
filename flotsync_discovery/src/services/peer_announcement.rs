use super::*;
use flotsync_messages::discovery::Peer;
use itertools::Itertools;
// use socket2::{Domain, Protocol, Socket, Type};
use pnet::{
    datalink::{self, NetworkInterface},
    util::MacAddr,
};
use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio::{net::UdpSocket, time::Interval};
use uuid::Uuid;

use flotsync_messages::protobuf::Message;

#[derive(Clone, Debug, PartialEq)]
pub struct Options {
    bind_addr: IpAddr,
    bind_port: Port,
    port: Port,
    announcement_interval: Duration,
}
impl Options {
    pub const DEFAULT: Self = Self {
        bind_addr: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        bind_port: Port(0),
        port: Port(52156),
        announcement_interval: Duration::from_secs(5),
    };
}
impl Default for Options {
    fn default() -> Self {
        Self::DEFAULT
    }
}

pub struct PeerAnnouncementService {
    socket: UdpSocket,
    broadcast_port: Port,
    broadcast_addresses: HashMap<MacAddr, SocketAddr>,
    interval: Interval,
    instance_id: Uuid,
}
impl PeerAnnouncementService {
    /// The default options for this service:
    ///
    /// - bind to '0.0.0.0:0'
    /// - announce to '255.255.255.255:52156'
    /// - announce every 5s
    pub const DEFAULT_OPTIONS: Options = Options::DEFAULT;

    pub async fn setup(options: Options) -> Result<Self> {
        let bind_address = SocketAddr::new(options.bind_addr, *options.bind_port);
        let socket = UdpSocket::bind(bind_address).await.context(IoSnafu)?;

        // TODO: Add a allow-reuse option and then use this code again, if necessary.
        // let socket =
        //     Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).context(IoSnafu)?;
        // socket.set_reuse_address(true).context(IoSnafu)?;
        // socket.set_reuse_port(true).context(IoSnafu)?; // macOS-specific
        // socket.bind(&addr.into()).context(IoSnafu)?;
        // socket.set_nonblocking(true).context(IoSnafu)?;

        // let socket = UdpSocket::from_std(socket.into()).context(IoSnafu)?;
        // let socket = UdpSocket::bind(addr).await.context(IoSnafu)?;

        socket.set_broadcast(true).context(IoSnafu)?;

        log::info!(
            "Sending announcement service messages from {}",
            socket.local_addr().context(IoSnafu)?
        );

        // let broadcast_address =
        //     SocketAddr::new(Ipv4Addr::new(255, 255, 255, 255).into(), *options.port);

        let interval = tokio::time::interval(options.announcement_interval);

        Ok(Self {
            socket,
            broadcast_port: options.port,
            broadcast_addresses: HashMap::new(),
            interval,
            instance_id: Uuid::new_v4(),
        })
    }

    fn get_active_broadcast_interfaces() -> Vec<NetworkInterface> {
        datalink::interfaces()
            .into_iter()
            .filter(|i| {
                i.mac.is_some()
                    && i.is_up()
                    && !i.ips.is_empty()
                    && (i.is_loopback() || i.is_broadcast())
            })
            .collect()
    }

    fn get_broadcast_address_for_interface(
        &self,
        interface: &NetworkInterface,
    ) -> Option<SocketAddr> {
        if interface.ips.len() > 1 {
            log::trace!(
                "Interface {} has {} IP ranges.\n{}\nArbitrarily picking the first IPv4.",
                interface.name,
                interface.ips.len(),
                interface.ips.iter().join(", ")
            );
        }
        interface.ips.iter().find(|net| net.is_ipv4()).map(|net| {
            let broadcast_addr = net.broadcast();
            SocketAddr::new(broadcast_addr, *self.broadcast_port)
        })
    }

    fn update_broadcast_addresses(&mut self) {
        let active_interfaces = Self::get_active_broadcast_interfaces();
        log::trace!(
            "There are {} active interfaces: {}",
            active_interfaces.len(),
            active_interfaces.iter().map(|i| &i.name).join(", ")
        );
        let deactivated_interfaces = self
            .broadcast_addresses
            .keys()
            .filter(|mac| {
                let mac_opt = Some(*mac);
                !active_interfaces.iter().any(|i| i.mac.as_ref() == mac_opt)
            })
            .cloned()
            .collect_vec();
        for mac in deactivated_interfaces {
            self.broadcast_addresses.remove(&mac);
            log::debug!("Removed no-longer available interface with MAC={mac}");
        }
        for i in active_interfaces {
            // We filter for interfaces that have MAC addresses, so unwrap is fine here.
            let mac = i.mac.unwrap();
            if let Some(broadcast_address) = self.get_broadcast_address_for_interface(&i) {
                let _entry = self
                    .broadcast_addresses
                    .entry(mac)
                    .and_modify(|addr| {
                        if addr != &broadcast_address {
                            *addr = broadcast_address;
                            log::debug!(
                                "Updated interface with MAC={mac} to broadcast_address={broadcast_address}"
                            );
                        }
                    })
                    .or_insert_with(|| {
                        log::debug!("Added interface with MAC={mac} and broadcast_address={broadcast_address}");
                        broadcast_address
                    });
            } else {
                log::trace!("Could not find a broadcast address for interface: {i}");
            }
        }
    }

    fn broadcast_message(&self) -> Peer {
        Peer {
            instance_uuid: self.instance_id.as_bytes().to_vec(),
            ..Default::default()
        }
    }

    fn encoded_broadcast_message(&self) -> Result<Vec<u8>> {
        self.broadcast_message()
            .write_to_bytes()
            .context(ProtoSnafu)
    }
}

#[async_trait]
impl Service for PeerAnnouncementService {
    type Options = Options;

    async fn run(&mut self) -> Result<()> {
        // Wait out the announcement interval.
        self.interval.tick().await;

        // Check if something has changed since last time we ran.
        self.update_broadcast_addresses();

        // Deduplicate the addresses, since multiple interfaces may be in the same network.
        let broadcast_addresses: HashSet<SocketAddr> =
            self.broadcast_addresses.values().cloned().collect();

        let broadcast_bytes = self.encoded_broadcast_message()?;

        // Send the announcement at each broadcast address.
        for broadcast_address in broadcast_addresses {
            match self
                .socket
                .send_to(&broadcast_bytes, broadcast_address)
                .await
            {
                Ok(_) => {
                    log::trace!("Sent message to {broadcast_address}. Did you receive it?");
                }
                Err(e) => {
                    log::debug!("Could not send message on socket {broadcast_address}: {e}");
                }
            }
        }
        Ok(())
    }

    async fn shutdown(self) -> Result<()> {
        drop(self.socket);
        Ok(())
    }
}
