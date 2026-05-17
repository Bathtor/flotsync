use super::{BindLocalEndpointSnafu, ControlFutureSnafu, RuntimeControlError, RuntimeHostError};
use flotsync_io::prelude::{
    SocketId,
    UdpIndication,
    UdpLocalBind,
    UdpOpenRequestId,
    UdpPort,
    UdpRequest,
};
use flotsync_utils::FutureTimeoutExt as _;
use futures_util::FutureExt as _;
use kompact::prelude::*;
use snafu::ResultExt;
use std::{error::Error as StdError, net::SocketAddr, sync::Arc, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LocalEndpointBinding {
    pub(super) socket_id: SocketId,
    pub(super) local_addr: SocketAddr,
}

#[derive(Debug)]
pub(super) enum LocalEndpointManagerMessage {
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "runtime host local endpoint binding is temporarily unreachable while the public loader fails fast for security provisioning"
        )
    )]
    EnsureBound(Ask<(), Result<LocalEndpointBinding, RuntimeControlError>>),
}

/// Single local delivery-endpoint owner for the current runtime slice.
///
/// This is a deliberate stub until `flotsync-665` lands. It owns one
/// configured UDP bind, records the concrete local address assigned by the
/// system, and is the place where later rebinding/network-change logic should
/// grow rather than scattering bind handling across the host.
#[derive(ComponentDefinition)]
pub(super) struct LocalEndpointManager {
    ctx: ComponentContext<Self>,
    udp: RequiredPort<UdpPort>,
    configured_bind_addr: SocketAddr,
    state: LocalEndpointManagerState,
}

impl LocalEndpointManager {
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "runtime host topology construction is temporarily unreachable while the public loader fails fast for security provisioning"
        )
    )]
    pub(super) fn new(configured_bind_addr: SocketAddr) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            udp: RequiredPort::uninitialised(),
            configured_bind_addr,
            state: LocalEndpointManagerState::Unbound,
        }
    }
}

ignore_lifecycle!(LocalEndpointManager);

impl Require<UdpPort> for LocalEndpointManager {
    fn handle(&mut self, indication: UdpIndication) -> HandlerResult {
        let current_state = std::mem::replace(&mut self.state, LocalEndpointManagerState::Unbound);
        self.state = match (current_state, indication) {
            (
                LocalEndpointManagerState::Binding {
                    request_id,
                    promise,
                },
                UdpIndication::Bound {
                    request_id: indicated_request_id,
                    socket_id,
                    local_addr,
                },
            ) if request_id == indicated_request_id => {
                let binding = LocalEndpointBinding {
                    socket_id,
                    local_addr,
                };
                if promise.fulfil(Ok(binding)).is_ok() {
                    LocalEndpointManagerState::Bound(binding)
                } else {
                    LocalEndpointManagerState::Unbound
                }
            }
            (
                LocalEndpointManagerState::Binding {
                    request_id,
                    promise,
                },
                UdpIndication::BindFailed {
                    request_id: indicated_request_id,
                    local_addr,
                    reason,
                },
            ) if request_id == indicated_request_id => {
                let _ = promise.fulfil(Err(RuntimeControlError::failed(format!(
                    "bind at {local_addr} failed: {reason:?}"
                ))));
                LocalEndpointManagerState::Unbound
            }
            (state, _) => state,
        };
        Handled::OK
    }
}

impl Actor for LocalEndpointManager {
    type Message = LocalEndpointManagerMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            LocalEndpointManagerMessage::EnsureBound(ask) => {
                let (promise, ()) = ask.take();
                match &self.state {
                    LocalEndpointManagerState::Unbound => {
                        let request_id = UdpOpenRequestId::new();
                        self.state = LocalEndpointManagerState::Binding {
                            request_id,
                            promise,
                        };
                        self.udp.trigger(UdpRequest::Bind {
                            request_id,
                            bind: UdpLocalBind::Exact(self.configured_bind_addr),
                        });
                    }
                    LocalEndpointManagerState::Binding { .. } => {
                        let _ = promise.fulfil(Err(RuntimeControlError::failed(
                            "local endpoint bind already in progress",
                        )));
                    }
                    LocalEndpointManagerState::Bound(binding) => {
                        let _ = promise.fulfil(Ok(*binding));
                    }
                }
                Handled::OK
            }
        }
    }
}

enum LocalEndpointManagerState {
    Unbound,
    Binding {
        request_id: UdpOpenRequestId,
        promise: KPromise<Result<LocalEndpointBinding, RuntimeControlError>>,
    },
    Bound(LocalEndpointBinding),
}

/// Ask the local endpoint manager to bind its configured UDP endpoint.
///
/// The caller must pass a live component whose actor ref can still be upgraded
/// to a strong ref. `control_timeout` bounds the whole ask/response exchange,
/// including the UDP bind indication that fulfils the manager promise.
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "runtime host local endpoint binding is temporarily unreachable while the public loader fails fast for security provisioning"
    )
)]
pub(super) async fn ensure_local_endpoint_bound(
    local_endpoint_manager: &Arc<Component<LocalEndpointManager>>,
    control_timeout: Duration,
) -> Result<LocalEndpointBinding, RuntimeHostError> {
    let local_endpoint_ref = local_endpoint_manager
        .actor_ref()
        .hold()
        .expect("local endpoint manager must expose a strong actor ref");
    let future = local_endpoint_ref
        .ask_with(|promise| LocalEndpointManagerMessage::EnsureBound(Ask::new(promise, ())));
    future
        .map(flatten_local_endpoint_ask_result)
        .timeout_fold_err(control_timeout)
        .await
        .context(BindLocalEndpointSnafu)
}

#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "runtime host local endpoint binding is temporarily unreachable while the public loader fails fast for security provisioning"
    )
)]
fn flatten_local_endpoint_ask_result<E>(
    result: Result<Result<LocalEndpointBinding, RuntimeControlError>, E>,
) -> Result<LocalEndpointBinding, RuntimeControlError>
where
    E: StdError + Send + Sync + 'static,
{
    result.boxed().context(ControlFutureSnafu)?
}
