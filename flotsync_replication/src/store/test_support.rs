//! Controllable replication-store wrapper shared by store consumer tests.

use crate::{
    api::{
        ReplicationStore,
        ReplicationStoreReadTransaction,
        ReplicationStoreTransaction,
        StoreError,
        StoreErrorClassification,
    },
    delivery::{
        contracts::{
            ReliableDeliveryStore,
            StoredReliableDeliveryWork,
            StoredReliableDeliveryWorkMetadata,
        },
        shared::MessageId,
    },
};
use flotsync_core::MemberIdentity;
use flotsync_utils::BoxFuture;
use futures_util::FutureExt as _;
use kompact::prelude::{KFuture, KPromise, promise};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard, mpsc},
};

/// Construct one store error with an explicit test classification.
pub(crate) fn classified_store_error(
    classification: StoreErrorClassification,
    description: &'static str,
) -> StoreError {
    StoreError::new(classification, std::io::Error::other(description))
}

/// One-shot full-load result injected before delegating later calls.
#[derive(Clone, Copy)]
pub(crate) enum ReliableDeliveryLoadFault {
    /// Report that the selected row is unexpectedly absent.
    Missing,
    /// Return a store error with the supplied classification.
    Error(StoreErrorClassification),
}

/// Store wrapper providing shared observation, fault injection, and asynchronous gates for tests.
pub(crate) struct ControlledStore<S> {
    /// Store receiving every call not intercepted by a configured test behaviour.
    inner: Arc<S>,
    /// Test controls and observations accessed synchronously at the wrapper boundary.
    state: Mutex<ControlledStoreState>,
}

impl<S> ControlledStore<S> {
    /// Build an observing wrapper that delegates all operations.
    pub(crate) fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            state: Mutex::new(ControlledStoreState::default()),
        }
    }

    /// Inject an error for the first read-transaction start.
    pub(crate) fn failing_first_read_transaction(
        inner: Arc<S>,
        classification: StoreErrorClassification,
    ) -> Self {
        let store = Self::new(inner);
        store
            .lock_state()
            .read_transaction_failures
            .push_back(classification);
        store
    }

    /// Inject an error for the first reliable-delivery persistence call.
    pub(crate) fn failing_first_persistence(
        inner: Arc<S>,
        classification: StoreErrorClassification,
    ) -> Self {
        let store = Self::new(inner);
        store.lock_state().store_failures.push_back(classification);
        store
    }

    /// Inject one full-envelope load result before delegating later calls.
    pub(crate) fn with_full_load_fault(inner: Arc<S>, fault: ReliableDeliveryLoadFault) -> Self {
        let store = Self::new(inner);
        store.lock_state().full_load_faults.push_back(fault);
        store
    }

    /// Inject an error for the first reliable-delivery stored-row removal.
    pub(crate) fn failing_first_removal(
        inner: Arc<S>,
        classification: StoreErrorClassification,
    ) -> Self {
        let store = Self::new(inner);
        store
            .lock_state()
            .removal_failures
            .push_back(classification);
        store
    }

    /// Gate a prefix of full-envelope loads and return their controls.
    pub(crate) fn gating_full_loads(
        inner: Arc<S>,
        gate_count: usize,
    ) -> (Self, mpsc::Receiver<MessageId>, Vec<KPromise<()>>) {
        let mut full_load_releases = VecDeque::with_capacity(gate_count);
        let mut gate_promises = Vec::with_capacity(gate_count);
        for _gate_index in 0..gate_count {
            let (gate_promise, gate_future) = promise();
            gate_promises.push(gate_promise);
            full_load_releases.push_back(gate_future);
        }
        let (full_load_started, full_load_started_rx) = mpsc::channel();
        let store = Self {
            inner,
            state: Mutex::new(ControlledStoreState {
                full_load_releases,
                full_load_started: Some(full_load_started),
                ..ControlledStoreState::default()
            }),
        };
        (store, full_load_started_rx, gate_promises)
    }

    /// Return every encoded work value supplied to persistence in call order.
    pub(crate) fn store_attempts(&self) -> Vec<StoredReliableDeliveryWork> {
        self.lock_state().store_attempts.clone()
    }

    /// Return every full-envelope message id supplied in call order.
    pub(crate) fn full_loads(&self) -> Vec<MessageId> {
        self.lock_state().full_loads.clone()
    }

    /// Return how many full-envelope loads crossed the wrapper boundary.
    pub(crate) fn full_load_calls(&self) -> usize {
        self.lock_state().full_loads.len()
    }

    /// Return how many stored-row removals crossed the wrapper boundary.
    pub(crate) fn removal_calls(&self) -> usize {
        self.lock_state().removal_calls
    }

    /// Lock the short-lived synchronous test state.
    fn lock_state(&self) -> MutexGuard<'_, ControlledStoreState> {
        self.state
            .lock()
            .expect("controlled-store state lock should not be poisoned")
    }
}

impl<S> ReliableDeliveryStore for ControlledStore<S>
where
    S: ReliableDeliveryStore + 'static,
{
    fn load_reliable_delivery_work_metadata(
        &self,
    ) -> BoxFuture<'_, Result<Vec<StoredReliableDeliveryWorkMetadata>, StoreError>> {
        self.inner.load_reliable_delivery_work_metadata()
    }

    fn load_reliable_delivery_work(
        &self,
        message_id: MessageId,
    ) -> BoxFuture<'_, Result<Option<StoredReliableDeliveryWork>, StoreError>> {
        let (fault, full_load_release, full_load_started) = {
            let mut state = self.lock_state();
            state.full_loads.push(message_id);
            let fault = state.full_load_faults.pop_front();
            let (full_load_release, full_load_started) = if fault.is_none() {
                (
                    state.full_load_releases.pop_front(),
                    state.full_load_started.clone(),
                )
            } else {
                (None, None)
            };
            (fault, full_load_release, full_load_started)
        };
        match fault {
            Some(ReliableDeliveryLoadFault::Missing) => {
                return async move { Ok(None) }.boxed();
            }
            Some(ReliableDeliveryLoadFault::Error(classification)) => {
                let error = classified_store_error(classification, "injected full-load failure");
                return async move { Err(error) }.boxed();
            }
            None => {}
        }
        let inner = self.inner.clone();
        async move {
            if let Some(full_load_started) = full_load_started {
                full_load_started
                    .send(message_id)
                    .expect("full-load observer must stay live");
            }
            if let Some(full_load_release) = full_load_release {
                full_load_release
                    .await
                    .expect("full-load release promise must complete");
            }
            inner.load_reliable_delivery_work(message_id).await
        }
        .boxed()
    }

    fn store_reliable_delivery_work(
        &self,
        work: StoredReliableDeliveryWork,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        let classification = {
            let mut state = self.lock_state();
            state.store_attempts.push(work.clone());
            state.store_failures.pop_front()
        };
        if let Some(classification) = classification {
            let error = classified_store_error(classification, "injected persistence failure");
            return async move { Err(error) }.boxed();
        }
        self.inner.store_reliable_delivery_work(work)
    }

    fn remove_reliable_delivery_work(
        &self,
        message_id: MessageId,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        let classification = {
            let mut state = self.lock_state();
            state.removal_calls += 1;
            state.removal_failures.pop_front()
        };
        if let Some(classification) = classification {
            let error =
                classified_store_error(classification, "injected stored-row removal failure");
            return async move { Err(error) }.boxed();
        }
        self.inner.remove_reliable_delivery_work(message_id)
    }
}

impl<S> ReplicationStore for ControlledStore<S>
where
    S: ReplicationStore + 'static,
{
    fn local_member_identity(&self) -> BoxFuture<'_, Result<MemberIdentity, StoreError>> {
        self.inner.local_member_identity()
    }

    fn begin_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreTransaction>, StoreError>> {
        self.inner.begin_transaction()
    }

    fn begin_read_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreReadTransaction>, StoreError>> {
        let classification = self.lock_state().read_transaction_failures.pop_front();
        if let Some(classification) = classification {
            let error = classified_store_error(classification, "injected read-transaction failure");
            return async move { Err(error) }.boxed();
        }
        self.inner.begin_read_transaction()
    }
}

/// Mutable controls and observations protected by [`ControlledStore::state`].
#[derive(Default)]
struct ControlledStoreState {
    /// Ordered read-transaction failures injected before delegating later calls.
    read_transaction_failures: VecDeque<StoreErrorClassification>,
    /// Exact work values observed at the persistence boundary.
    store_attempts: Vec<StoredReliableDeliveryWork>,
    /// Ordered persistence failures injected before delegating later calls.
    store_failures: VecDeque<StoreErrorClassification>,
    /// Message ids supplied to every full-envelope load in call order.
    full_loads: Vec<MessageId>,
    /// Ordered full-load results injected before delegating later calls.
    full_load_faults: VecDeque<ReliableDeliveryLoadFault>,
    /// Ordered gates consumed by the first full-envelope loads.
    full_load_releases: VecDeque<KFuture<()>>,
    /// Optional observation channel notified before each full-envelope load.
    full_load_started: Option<mpsc::Sender<MessageId>>,
    /// Ordered removal failures injected before delegating later calls.
    removal_failures: VecDeque<StoreErrorClassification>,
    /// Number of stored-row removal calls observed by this wrapper.
    removal_calls: usize,
}
