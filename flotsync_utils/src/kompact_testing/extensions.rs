//! Port and actor-reference extensions for test interaction.

use super::*;

/// Extension trait for creating tester components from a port type.
pub trait PortTestingExt: Port + Sized + Send + Sync + 'static
where
    Self::Indication: Send + Sync + 'static,
    Self::Request: Send + Sync + 'static,
{
    /// Create a passive sidecar tester for this port type.
    #[must_use]
    fn tester_component_sidecar() -> PortTesterComponent<Self>;

    /// Create an inline forwarding tester for this port type.
    #[must_use]
    fn tester_component_forwarding() -> PortTesterComponent<Self>;
}

impl<P> PortTestingExt for P
where
    P: Port + Sized + Send + Sync + 'static,
    P::Indication: Send + Sync + 'static,
    P::Request: Send + Sync + 'static,
{
    fn tester_component_sidecar() -> PortTesterComponent<Self> {
        PortTesterComponent::new(false)
    }

    fn tester_component_forwarding() -> PortTesterComponent<Self> {
        PortTesterComponent::new(true)
    }
}

/// Extension trait for controlling a [`PortTesterComponent`] through its actor reference.
pub trait PortTestingRefExt<PortType>
where
    PortType: Port + Sized + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Inject `request` through the tester's required port.
    ///
    /// When connected to a component under test, this makes the component
    /// observe the request on its provided port.
    fn inject_request(&self, request: PortType::Request);

    /// Inject `indication` through the tester's provided port.
    ///
    /// When connected to a component under test, this makes the component
    /// observe the indication on its required port.
    fn inject_indication(&self, indication: PortType::Indication);

    /// Wait until the tester has observed an event matching `predicate`.
    ///
    /// Existing logged events are checked before future events. The returned
    /// future resolves to the first match in observed order.
    fn observe<F>(&self, predicate: F) -> KFuture<ObservedEvent<PortType>>
    where
        F: Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static;

    /// Wait until the tester has observed a matching request.
    fn observe_request<F>(&self, predicate: F) -> ObservedFuture<ObservedRequest<PortType>>
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static;

    /// Wait until the tester has observed a matching request at or after `start_index`.
    fn observe_request_from<F>(
        &self,
        start_index: usize,
        predicate: F,
    ) -> ObservedFuture<ObservedRequest<PortType>>
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static;

    /// Wait until the tester has observed a matching indication.
    fn observe_indication<F>(&self, predicate: F) -> ObservedFuture<ObservedIndication<PortType>>
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static;

    /// Wait until the tester has observed a matching indication at or after `start_index`.
    fn observe_indication_from<F>(
        &self,
        start_index: usize,
        predicate: F,
    ) -> ObservedFuture<ObservedIndication<PortType>>
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static;

    /// Fail if the tester observes a matching event during `duration`.
    ///
    /// Existing log entries are ignored. Use
    /// [`Self::fail_if_observed_from`] when a check should include
    /// events from a known cursor.
    fn fail_if_observed<F>(
        &self,
        duration: Duration,
        predicate: F,
    ) -> KFuture<Result<(), ObservedEvent<PortType>>>
    where
        F: Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static;

    /// Fail if the tester observes a matching event at or after `start_index`
    /// during `duration`.
    fn fail_if_observed_from<F>(
        &self,
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> KFuture<Result<(), ObservedEvent<PortType>>>
    where
        F: Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static;

    /// Fail if the tester observes a matching request during `duration`.
    fn fail_if_request_observed<F>(
        &self,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedRequest<PortType>>>
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static;

    /// Fail if the tester observes a matching request at or after `start_index`
    /// during `duration`.
    fn fail_if_request_observed_from<F>(
        &self,
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedRequest<PortType>>>
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static;

    /// Fail if the tester observes a matching indication during `duration`.
    fn fail_if_indication_observed<F>(
        &self,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedIndication<PortType>>>
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static;

    /// Fail if the tester observes a matching indication at or after
    /// `start_index` during `duration`.
    fn fail_if_indication_observed_from<F>(
        &self,
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedIndication<PortType>>>
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static;

    // TODO(flotsync-e0wg): Add a combined observe-or-fail helper that removes
    // the losing observation branch when the other branch resolves.
}

impl<P> PortTestingRefExt<P> for ActorRef<PortTestMsg<P>>
where
    P: Port + Sized + Send + Sync + 'static,
    P::Indication: Send + Sync + 'static,
    P::Request: Send + Sync + 'static,
{
    fn inject_request(&self, request: <P as Port>::Request) {
        self.tell(PortTestMsg::inject_request(request));
    }

    fn inject_indication(&self, indication: <P as Port>::Indication) {
        self.tell(PortTestMsg::inject_indication(indication));
    }

    fn observe<F>(&self, predicate: F) -> KFuture<ObservedEvent<P>>
    where
        F: Fn((usize, &TestEvent<P>)) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::observe(predicate);
        self.tell(msg);
        future
    }

    fn observe_request<F>(&self, predicate: F) -> ObservedFuture<ObservedRequest<P>>
    where
        F: Fn(&<P as Port>::Request) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::observe_request(predicate);
        self.tell(msg);
        future
    }

    fn observe_request_from<F>(
        &self,
        start_index: usize,
        predicate: F,
    ) -> ObservedFuture<ObservedRequest<P>>
    where
        F: Fn(&<P as Port>::Request) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::observe_request_from(start_index, predicate);
        self.tell(msg);
        future
    }

    fn observe_indication<F>(&self, predicate: F) -> ObservedFuture<ObservedIndication<P>>
    where
        F: Fn(&<P as Port>::Indication) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::observe_indication(predicate);
        self.tell(msg);
        future
    }

    fn observe_indication_from<F>(
        &self,
        start_index: usize,
        predicate: F,
    ) -> ObservedFuture<ObservedIndication<P>>
    where
        F: Fn(&<P as Port>::Indication) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::observe_indication_from(start_index, predicate);
        self.tell(msg);
        future
    }

    fn fail_if_observed<F>(
        &self,
        duration: Duration,
        predicate: F,
    ) -> KFuture<Result<(), ObservedEvent<P>>>
    where
        F: Fn((usize, &TestEvent<P>)) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::fail_if_observed(duration, predicate);
        self.tell(msg);
        future
    }

    fn fail_if_observed_from<F>(
        &self,
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> KFuture<Result<(), ObservedEvent<P>>>
    where
        F: Fn((usize, &TestEvent<P>)) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::fail_if_observed_from(start_index, duration, predicate);
        self.tell(msg);
        future
    }

    fn fail_if_request_observed<F>(
        &self,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedRequest<P>>>
    where
        F: Fn(&<P as Port>::Request) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::fail_if_request_observed(duration, predicate);
        self.tell(msg);
        future
    }

    fn fail_if_request_observed_from<F>(
        &self,
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedRequest<P>>>
    where
        F: Fn(&<P as Port>::Request) -> bool + Send + 'static,
    {
        let (msg, future) =
            PortTestMsg::fail_if_request_observed_from(start_index, duration, predicate);
        self.tell(msg);
        future
    }

    fn fail_if_indication_observed<F>(
        &self,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedIndication<P>>>
    where
        F: Fn(&<P as Port>::Indication) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::fail_if_indication_observed(duration, predicate);
        self.tell(msg);
        future
    }

    fn fail_if_indication_observed_from<F>(
        &self,
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedIndication<P>>>
    where
        F: Fn(&<P as Port>::Indication) -> bool + Send + 'static,
    {
        let (msg, future) =
            PortTestMsg::fail_if_indication_observed_from(start_index, duration, predicate);
        self.tell(msg);
        future
    }
}
