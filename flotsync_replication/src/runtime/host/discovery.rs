use super::*;

#[derive(Debug)]
pub(super) enum RuntimeDiscoverySourceMessage {
    Publish(DiscoveryRouteUpdate<TransportRouteKey>),
}

/// Temporary route-discovery source used until the replication runtime is
/// wired to the real discovery component.
#[derive(ComponentDefinition)]
pub(super) struct RuntimeDiscoverySource {
    ctx: ComponentContext<Self>,
    discovery: ProvidedPort<RouteDiscoveryPort<TransportRouteKey>>,
}

impl RuntimeDiscoverySource {
    pub(super) fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            discovery: ProvidedPort::uninitialised(),
        }
    }
}

ignore_lifecycle!(RuntimeDiscoverySource);

impl Provide<RouteDiscoveryPort<TransportRouteKey>> for RuntimeDiscoverySource {
    fn handle(&mut self, _request: Never) -> Handled {
        unreachable!("runtime discovery source is indication-only")
    }
}

impl LocalActor for RuntimeDiscoverySource {
    type Message = RuntimeDiscoverySourceMessage;

    fn receive(&mut self, msg: Self::Message) -> Handled {
        match msg {
            RuntimeDiscoverySourceMessage::Publish(update) => {
                self.discovery.trigger(update);
                Handled::Ok
            }
        }
    }
}

impl_local_actor!(RuntimeDiscoverySource);
