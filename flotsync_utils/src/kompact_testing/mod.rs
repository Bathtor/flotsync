//! Test helpers for observing and injecting Kompact port traffic.
//!
//! [`PortTesterComponent`] owns one provided port and one required port of the
//! same Kompact [`Port`] type. Tests can connect it to components under test
//! and then use its actor reference to inject requests or indications, or to
//! wait until matching traffic has been observed.
//!
//! In sidecar mode, the tester only records traffic. This relies on Kompact
//! port fan-out: a connected observer receives the same events as the
//! production recipient but does not forward them further.
//!
//! In forwarding mode, the tester acts as an inline proxy. Each event observed
//! on one side is recorded and then triggered on the opposite side.
//!
//! Observation results carry their zero-based event-log index. Use a positive
//! observation as the synchronisation point before extracting the complete log
//! from the tester at the end of a test.
//!
//! The tester intentionally exposes cursor-based observation before bulk log
//! export through actor messages. If a downstream test needs the complete event
//! sequence at the end of the test, extract it from the component definition
//! with [`PortTesterComponent::event_log_snapshot`] before shutting the system
//! down.
//!
//! # Example
//!
//! ```rust
//! # use std::time::Duration;
//! # use kompact::prelude::*;
//! # use flotsync_utils::kompact_testing::{PortTestingExt, PortTestingRefExt, TestEvent};
//! # struct TestPort;
//! # impl Port for TestPort {
//! #     type Indication = String;
//! #     type Request = String;
//! # }
//! # let system = KompactConfig::default().build().wait().expect("system");
//! # let source = system.create(TestPort::tester_component_sidecar);
//! # let observer = system.create(TestPort::tester_component_sidecar);
//! # biconnect_components(&source, &observer).expect("connection");
//! # system
//! #     .start_notify(&source)
//! #     .wait_timeout(Duration::from_secs(1))
//! #     .expect("source start");
//! # system
//! #     .start_notify(&observer)
//! #     .wait_timeout(Duration::from_secs(1))
//! #     .expect("observer start");
//! let source_ref = source.actor_ref();
//! let observer_ref = observer.actor_ref();
//! let final_event = observer_ref.observe_indication(|event| event == "second");
//!
//! source_ref.inject_indication("first".to_owned());
//! source_ref.inject_indication("second".to_owned());
//!
//! final_event
//!     .wait_timeout(Duration::from_secs(1))
//!     .expect("final event before timeout")
//!     .expect("tester promise still live");
//!
//! let log = observer.on_definition(|component| component.event_log_snapshot());
//! assert_eq!(log.len(), 2);
//! assert!(matches!(log[0].as_ref(), TestEvent::Indication(value) if value == "first"));
//! assert!(matches!(log[1].as_ref(), TestEvent::Indication(value) if value == "second"));
//! # system.shutdown().wait().expect("shutdown");
//! ```

use crate::NonOwningPhantomData;
use futures_util::FutureExt as _;
use kompact::prelude::*;
use snafu::{ResultExt as _, Whatever};
use std::{fmt, marker::PhantomData, sync::Arc, time::Duration};

mod component;
mod events;
mod extensions;
mod waits;

#[cfg(test)]
mod tests;

pub use component::{PortTestMsg, PortTesterComponent};
pub use events::{
    AnyEvent,
    EventAbsencePredicate,
    EventObservationPredicate,
    EventPredicate,
    IndicationEvent,
    ObservedEvent,
    RequestEvent,
    TestEvent,
};
pub use extensions::{PortTestingExt, PortTestingRefExt};
pub use waits::{eventually, eventually_component_state};

const EVENTUALLY_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Future returned by typed observation helpers.
///
/// The wrapped future resolves to `Err` only when the tester-side promise is
/// dropped before completion. Synchronous tests can use Kompact's
/// [`BlockingFutureExt::wait_timeout`] on this type.
pub type ObservedFuture<T> = crate::BoxFuture<'static, Result<T, Whatever>>;

/// One request selected from a [`PortTesterComponent`] event log.
pub type ObservedRequest<PortType> = ObservedEvent<PortType, RequestEvent>;

/// One indication selected from a [`PortTesterComponent`] event log.
pub type ObservedIndication<PortType> = ObservedEvent<PortType, IndicationEvent>;
