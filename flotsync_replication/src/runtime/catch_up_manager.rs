use super::messages::{
    NeedRangeMessage,
    RuntimeMessage,
    UpdateBatchMessage,
    UpdateMessage,
    UpdateRangeMessage,
    WireRuntimeMessage,
};
use crate::{
    api::{ReplicationStore, ReplicationUpdateFilter, StoreError},
    delivery::{
        contracts::{GroupBroadcastPort, GroupBroadcastPortIndication, GroupBroadcastPortRequest},
        group_broadcast::GroupBroadcastDeliver,
        shared::DeliveryClass,
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    membership::SharedGroupMemberships,
    versions::UpdateId,
};
use flotsync_messages::proto::{DecodeProtoView, EncodeProto};
use flotsync_utils::{OptionExt as _, ResultExt as _};
use interval::prelude::{Bounded, Difference, IntervalSet, IsEmpty, Range, Union};
use itertools::Itertools;
use kompact::{KompactLogger, prelude::*};
use snafu::{Location, prelude::*};
use std::{
    collections::{
        BTreeMap,
        HashMap,
        btree_map::Entry as BTreeEntry,
        hash_map::Entry as HashEntry,
    },
    future::Future,
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(1);
const DEFAULT_MAX_UPDATES_PER_BATCH: usize = 16;

mod config_keys {
    use super::{DEFAULT_MAX_UPDATES_PER_BATCH, DEFAULT_RETRY_DELAY};
    use kompact::{
        config::{DurationValue, UsizeValue},
        kompact_config,
    };

    kompact_config! {
        CATCH_UP_NEED_RANGE_RETRY_DELAY,
        key = "flotsync.replication.runtime.catch-up.need-range-retry-delay",
        type = DurationValue,
        default = DEFAULT_RETRY_DELAY,
        doc = "Delay before rebroadcasting still-needed catch-up ranges.",
        version = "0.1.0"
    }

    kompact_config! {
        CATCH_UP_MAX_UPDATES_PER_BATCH,
        key = "flotsync.replication.runtime.catch-up.max-updates-per-batch",
        type = UsizeValue,
        default = DEFAULT_MAX_UPDATES_PER_BATCH,
        doc = "Maximum number of updates included in one catch-up UpdateBatch. Set to 0 to send all locally available requested updates in one response.",
        version = "0.1.0"
    }
}

/// Local messages understood by [`CatchUpManagerComponent`].
#[derive(Debug)]
pub(super) enum CatchUpManagerMessage {
    /// Record that local state needs the provided producer ranges.
    NeedVersions(NeedVersions),
    /// Record producer versions already stored by the local runtime.
    ObservedAvailable(ObservedAvailable),
}

/// Concrete missing producer ranges for one replication group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct NeedVersions {
    pub(super) group_id: GroupId,
    pub(super) ranges: Vec<UpdateRangeMessage>,
}

/// Producer versions already stored by the replication runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ObservedAvailable {
    pub(super) group_id: GroupId,
    pub(super) ranges: Vec<UpdateRangeMessage>,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)), module(catch_up))]
enum CatchUpError {
    #[snafu(display("Replication-store access failed at {location}: {source}"))]
    StoreAccess {
        source: StoreError,
        #[snafu(implicit)]
        location: Location,
    },
}

/// Exact producer-version intervals grouped by canonical member index.
///
/// Each `IntervalSet` contains inclusive versions for one producer. Adding
/// ranges unions overlapping or adjacent intervals; subtracting available
/// ranges may split intervals to preserve holes instead of collapsing back to a
/// broad request. Callers must pass only protocol-valid bounds because
/// `intervallum` cannot represent the reserved maximum protocol version.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ProducerVersionSets {
    ranges: BTreeMap<MemberIndex, IntervalSet<u64>>,
}

impl ProducerVersionSets {
    fn new() -> Self {
        Self {
            ranges: BTreeMap::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Normalise potentially overlapping protocol ranges into per-producer sets.
    fn from_ranges(ranges: &[UpdateRangeMessage]) -> Self {
        let mut sets = Self::new();
        sets.insert_ranges(ranges);
        sets
    }

    /// Build the available-version view represented by persisted update ids.
    fn from_update_ids(update_ids: &[UpdateId]) -> Self {
        let mut sets = Self::new();
        sets.insert_update_ids(update_ids);
        sets
    }

    /// Insert already-validated protocol ranges into this normalised set.
    fn insert_ranges(&mut self, ranges: &[UpdateRangeMessage]) {
        for range in ranges {
            self.insert_range(*range);
        }
    }

    /// Insert one inclusive producer-version range.
    fn insert_range(&mut self, range: UpdateRangeMessage) {
        let producer_index = MemberIndex::new(range.producer_index);
        let incoming = version_set_from_range(range);
        self.ranges
            .entry(producer_index)
            .and_modify(|existing| *existing = existing.union(&incoming))
            .or_insert(incoming);
    }

    /// Insert all versions from another set and report whether anything changed.
    ///
    /// Returns `true` when at least one incoming version was not already
    /// present. Returns `false` when this set already covered every incoming
    /// version.
    fn insert_sets(&mut self, available: &ProducerVersionSets) -> bool {
        let mut changed = false;
        for (producer_index, incoming_versions) in &available.ranges {
            match self.ranges.entry(*producer_index) {
                BTreeEntry::Occupied(mut entry) => {
                    let merged = entry.get().union(incoming_versions);
                    if &merged != entry.get() {
                        entry.insert(merged);
                        changed = true;
                    }
                }
                BTreeEntry::Vacant(entry) => {
                    entry.insert(incoming_versions.clone());
                    changed = true;
                }
            }
        }
        changed
    }

    /// Insert singleton availability ranges represented by persisted update ids.
    fn insert_update_ids(&mut self, update_ids: &[UpdateId]) {
        for update_id in update_ids {
            self.insert_range(UpdateRangeMessage::from(*update_id));
        }
    }

    /// Remove all versions present in `available`, preserving remaining holes.
    fn subtract_sets(&mut self, available: &ProducerVersionSets) {
        for (producer_index, available_versions) in &available.ranges {
            if let Some(existing) = self.ranges.get_mut(producer_index) {
                *existing = existing.difference(available_versions);
            }
        }
        self.ranges.retain(|_, versions| !versions.is_empty());
    }

    /// Convert normalised intervals back into wire/runtime range messages.
    fn to_message_ranges(&self) -> Vec<UpdateRangeMessage> {
        self.ranges
            .iter()
            .flat_map(|(producer_index, versions)| {
                versions.iter().map(|range| UpdateRangeMessage {
                    producer_index: producer_index.as_u32(),
                    start_version: range.lower(),
                    end_version: range.upper(),
                })
            })
            .collect()
    }
}

/// Outstanding repair demand for one group.
struct PendingNeed {
    /// Versions still missing locally after subtracting observed availability.
    needed_versions: ProducerVersionSets,
    /// Active retry timer that owns the next rebroadcast for this group.
    retry_timer: Option<ScheduledTimer>,
}

impl PendingNeed {
    fn new() -> Self {
        Self {
            needed_versions: ProducerVersionSets::new(),
            retry_timer: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.needed_versions.is_empty()
    }

    /// Add newly missing versions to this pending need.
    ///
    /// Returns `true` when at least one incoming version was newly added to the
    /// pending set. Returns `false` when the request is already fully covered.
    /// Callers use `false` to suppress immediate `NeedRange` echoes for inbound
    /// requests; repeated broadcasts should come from the retry timer instead.
    fn insert_sets(&mut self, versions: &ProducerVersionSets) -> bool {
        self.needed_versions.insert_sets(versions)
    }

    /// Mark observed versions as no longer needed by this pending request.
    fn subtract_sets(&mut self, available: &ProducerVersionSets) {
        self.needed_versions.subtract_sets(available);
    }

    /// Return the ranges that should be included in the next `NeedRange`.
    fn to_message_ranges(&self) -> Vec<UpdateRangeMessage> {
        self.needed_versions.to_message_ranges()
    }
}

/// Subtract known-available producer versions from still-needed ranges.
pub(super) fn subtract_available_ranges(
    needed_ranges: &[UpdateRangeMessage],
    available_ranges: &[UpdateRangeMessage],
) -> Vec<UpdateRangeMessage> {
    let mut needed_versions = ProducerVersionSets::from_ranges(needed_ranges);
    let available_versions = ProducerVersionSets::from_ranges(available_ranges);
    needed_versions.subtract_sets(&available_versions);
    needed_versions.to_message_ranges()
}

/// Convert one protocol range into the interval type used for set arithmetic.
fn version_set_from_range(range: UpdateRangeMessage) -> IntervalSet<u64> {
    debug_assert!(
        range.start_version <= range.end_version,
        "update ranges must be normalised before catch-up tracking"
    );
    IntervalSet::new(range.start_version, range.end_version)
}

/// Owns the best-effort catch-up request and fulfilment protocol.
#[derive(ComponentDefinition)]
pub(super) struct CatchUpManagerComponent {
    ctx: ComponentContext<Self>,
    group_broadcast: RequiredPort<GroupBroadcastPort>,
    local_member: MemberIdentity,
    group_memberships: SharedGroupMemberships,
    store: Arc<dyn ReplicationStore>,
    /// Delay between rebroadcasts while any pending need remains unsatisfied.
    retry_delay: Duration,
    /// Per-response update limit; `None` means the configured zero/unlimited mode.
    max_updates_per_batch: Option<NonZeroUsize>,
    /// Group-scoped missing ranges that should be retried until observed locally.
    pending_needs: HashMap<GroupId, PendingNeed>,
    /// Group-scoped update-log availability known from storage and runtime observations.
    known_available: HashMap<GroupId, ProducerVersionSets>,
}

impl CatchUpManagerComponent {
    pub(super) fn new(
        local_member: MemberIdentity,
        group_memberships: SharedGroupMemberships,
        store: Arc<dyn ReplicationStore>,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            group_broadcast: RequiredPort::uninitialised(),
            local_member,
            group_memberships,
            store,
            retry_delay: DEFAULT_RETRY_DELAY,
            max_updates_per_batch: NonZeroUsize::new(DEFAULT_MAX_UPDATES_PER_BATCH),
            pending_needs: HashMap::new(),
            known_available: HashMap::new(),
        }
    }

    /// Read the retry delay from the component's Kompact config.
    fn read_retry_delay_from_config(&self) -> Duration {
        match self
            .ctx
            .config()
            .read_or_default(&config_keys::CATCH_UP_NEED_RANGE_RETRY_DELAY)
        {
            Ok(delay) => delay,
            Err(error) => {
                warn!(
                    self.log(),
                    "failed to read catch-up retry delay config; using default {:?}: {}",
                    DEFAULT_RETRY_DELAY,
                    error
                );
                DEFAULT_RETRY_DELAY
            }
        }
    }

    /// Read the per-response batch limit; config value `0` means unlimited.
    fn read_max_updates_per_batch_from_config(&self) -> Option<NonZeroUsize> {
        match self
            .ctx
            .config()
            .read_or_default(&config_keys::CATCH_UP_MAX_UPDATES_PER_BATCH)
        {
            Ok(0) => None,
            Ok(limit) => NonZeroUsize::new(limit),
            Err(error) => {
                warn!(
                    self.log(),
                    "failed to read catch-up batch size config; using default {}: {}",
                    DEFAULT_MAX_UPDATES_PER_BATCH,
                    error
                );
                NonZeroUsize::new(DEFAULT_MAX_UPDATES_PER_BATCH)
            }
        }
    }

    fn broadcast_need_range(&mut self, group_id: GroupId, ranges: Vec<UpdateRangeMessage>) {
        if ranges.is_empty() {
            return;
        }
        let message = RuntimeMessage::NeedRange(NeedRangeMessage { group_id, ranges });
        self.group_broadcast.trigger(
            GroupBroadcastPortRequest::build_submit(DeliveryClass::BestEffort)
                .for_member_in_group(self.local_member.clone(), group_id)
                .with_payload(message.encode_proto_to_bytes()),
        );
    }

    /// Record new needed versions, broadcast them once, and reset retry timing.
    ///
    /// Duplicate inbound demand is intentionally absorbed here. If the local
    /// pending set already covers every requested version, the retry timer owns
    /// future rebroadcasts so two peers that are missing the same update do not
    /// immediately echo identical `NeedRange` messages at each other.
    fn record_needed_version_sets(
        &mut self,
        group_id: GroupId,
        mut needed_versions: ProducerVersionSets,
    ) {
        if let Some(available) = self.known_available.get(&group_id) {
            needed_versions.subtract_sets(available);
        }
        if needed_versions.is_empty() {
            return;
        }
        let all_needed_ranges = {
            let pending = self
                .pending_needs
                .entry(group_id)
                .or_insert_with(PendingNeed::new);
            if !pending.insert_sets(&needed_versions) {
                return;
            }
            pending.to_message_ranges()
        };
        self.broadcast_need_range(group_id, all_needed_ranges);
        self.reset_retry_timer(group_id);
    }

    /// Schedule one retry timer for a group that still has pending demand.
    fn schedule_retry_if_needed(&mut self, group_id: GroupId) {
        let Some(pending) = self.pending_needs.get(&group_id) else {
            return;
        };
        if pending.is_empty() || pending.retry_timer.is_some() {
            return;
        }
        let retry_timer = self.schedule_once(self.retry_delay, move |component, expected_timer| {
            component.handle_retry(group_id, &expected_timer)
        });
        let pending = self
            .pending_needs
            .get_mut(&group_id)
            .expect("pending need must still exist after scheduling retry");
        pending.retry_timer = Some(retry_timer);
    }

    /// Restart the retry delay after a fresh broadcast for the same group.
    fn reset_retry_timer(&mut self, group_id: GroupId) {
        let existing_timer = self
            .pending_needs
            .get_mut(&group_id)
            .and_then(|pending| pending.retry_timer.take());
        if let Some(timer) = existing_timer {
            self.cancel_timer(timer);
        }
        self.schedule_retry_if_needed(group_id);
    }

    /// Cancel all retry timers during component shutdown.
    fn cancel_retry_timers(&mut self) {
        let timers = self
            .pending_needs
            .values_mut()
            .filter_map(|pending| pending.retry_timer.take())
            .collect_vec();
        for timer in timers {
            self.cancel_timer(timer);
        }
    }

    /// Rebroadcast still-missing ranges when the owning retry timer fires.
    fn handle_retry(
        &mut self,
        group_id: GroupId,
        expected_timer: &ScheduledTimer,
    ) -> HandlerResult {
        let Some(pending) = self.pending_needs.get_mut(&group_id) else {
            return Handled::OK;
        };
        if pending.retry_timer.as_ref() != Some(expected_timer) {
            // A newer broadcast reset the retry timer, so this stale timer no
            // longer owns the pending need.
            return Handled::OK;
        }
        pending.retry_timer = None;
        let ranges = pending.to_message_ranges();
        if ranges.is_empty() {
            self.pending_needs.remove(&group_id);
            return Handled::OK;
        }
        self.broadcast_need_range(group_id, ranges);
        self.schedule_retry_if_needed(group_id);
        Handled::OK
    }

    fn handle_need_versions(&mut self, need: &NeedVersions) -> HandlerResult {
        debug_assert!(
            !need.ranges.is_empty(),
            "local catch-up needs should not be empty"
        );
        if need.ranges.is_empty() {
            warn!(
                self.log(),
                "ignoring empty local catch-up need for group {}", need.group_id
            );
            return Handled::OK;
        }
        let needed_versions = ProducerVersionSets::from_ranges(&need.ranges);
        self.record_needed_version_sets(need.group_id, needed_versions);
        Handled::OK
    }

    /// Absorb newly observed availability and shrink any matching pending demand.
    fn handle_observed_available(&mut self, observed: &ObservedAvailable) -> HandlerResult {
        debug_assert!(
            !observed.ranges.is_empty(),
            "observed catch-up availability should not be empty"
        );
        if observed.ranges.is_empty() {
            warn!(
                self.log(),
                "ignoring empty observed catch-up availability for group {}", observed.group_id
            );
            return Handled::OK;
        }
        let available_versions = ProducerVersionSets::from_ranges(&observed.ranges);
        self.known_available
            .entry(observed.group_id)
            .or_default()
            .insert_sets(&available_versions);
        let mut timer_to_cancel = None;
        let needs_retry_timer = match self.pending_needs.entry(observed.group_id) {
            HashEntry::Occupied(mut entry) => {
                let pending = entry.get_mut();
                pending.subtract_sets(&available_versions);
                if pending.is_empty() {
                    timer_to_cancel = pending.retry_timer.take();
                    entry.remove();
                    false
                } else {
                    pending.retry_timer.is_none()
                }
            }
            HashEntry::Vacant(_) => false,
        };
        if let Some(timer) = timer_to_cancel {
            self.cancel_timer(timer);
        }
        if needs_retry_timer {
            self.schedule_retry_if_needed(observed.group_id);
        }
        Handled::OK
    }

    fn handle_group_delivery(&mut self, deliver: &GroupBroadcastDeliver) -> HandlerResult {
        let message =
            WireRuntimeMessage::decode_proto_view_from_slice(&deliver.envelope.payload.bytes)
                .with_whatever_benign(|_| {
                    format!(
                        "dropping inbound catch-up candidate from {} after decode error",
                        deliver.envelope.header.sender
                    )
                })?;
        match message {
            WireRuntimeMessage::NeedRange(message) => {
                self.handle_inbound_need_range(&deliver.envelope.header.sender, message)
            }
            WireRuntimeMessage::BootstrapGroup(_)
            | WireRuntimeMessage::Update(_)
            | WireRuntimeMessage::SummaryRequest(_)
            | WireRuntimeMessage::Summary(_)
            | WireRuntimeMessage::UpdateBatch(_) => Handled::OK,
        }
    }

    /// Validate, absorb, and fulfil one inbound catch-up request.
    fn handle_inbound_need_range(
        &mut self,
        sender: &MemberIdentity,
        message: NeedRangeMessage,
    ) -> HandlerResult {
        let memberships = self.group_memberships.snapshot();
        let members = memberships
            .members(&message.group_id)
            .with_whatever_benign(|| {
                format!(
                    "dropping NeedRange for unknown group {} from {}",
                    message.group_id, sender
                )
            })?;
        if !members.contains(sender) {
            warn!(
                self.log(),
                "dropping NeedRange for group {} from non-member {}", message.group_id, sender
            );
            return Handled::OK;
        }
        let group_id = message.group_id;
        let member_count = members.len();
        let mut ranges = Vec::with_capacity(message.ranges.len());
        let mut invalid_producer_indexes = Vec::new();
        for range in message.ranges {
            if (range.producer_index as usize) < member_count {
                ranges.push(range);
            } else {
                invalid_producer_indexes.push(range.producer_index);
            }
        }
        if !invalid_producer_indexes.is_empty() {
            warn!(
                self.log(),
                "dropping invalid NeedRange producer indexes {:?} for group {} from {}",
                invalid_producer_indexes,
                group_id,
                sender
            );
        }
        if ranges.is_empty() {
            return Handled::OK;
        }

        let requested_versions = ProducerVersionSets::from_ranges(&ranges);
        let mut missing_locally = requested_versions.clone();
        if let Some(available) = self.known_available.get(&group_id) {
            missing_locally.subtract_sets(available);
        }
        if !missing_locally.is_empty() {
            self.record_needed_version_sets(group_id, missing_locally.clone());
        }

        let mut answerable_versions = requested_versions;
        // `missing_locally` is `requested_versions - locally_available`, so
        // subtracting it leaves the requested versions we currently believe we
        // can serve.
        answerable_versions.subtract_sets(&missing_locally);
        let answerable_ranges = answerable_versions.to_message_ranges();
        if answerable_ranges.is_empty() {
            debug!(
                self.log(),
                "cannot fulfil NeedRange for group {} from {} because none of the requested ranges are locally available",
                group_id,
                sender
            );
            return Handled::OK;
        }

        self.spawn_local(move |mut async_self| async move {
            async_self
                .fulfil_need_range_from_store(group_id, answerable_ranges)
                .await;
            Handled::OK
        });
        Handled::OK
    }

    /// Load and broadcast one catch-up response for ranges believed locally available.
    async fn fulfil_need_range_from_store(
        &mut self,
        group_id: GroupId,
        answerable_ranges: Vec<UpdateRangeMessage>,
    ) {
        let reply = self
            .load_update_batch_from_store(group_id, answerable_ranges)
            .await;
        match reply {
            Ok(updates) if updates.is_empty() => {
                debug_assert!(
                    !updates.is_empty(),
                    "known catch-up availability should produce at least one update"
                );
                if let Err(error) = self.refresh_known_available_from_store().await {
                    warn!(
                        self.log(),
                        "failed to refresh stale catch-up availability after empty response for group {}: {}",
                        group_id,
                        error
                    );
                }
                // Don't try to fulfill again now. We can answer it next time they ask.
            }
            Ok(updates) => {
                let message = RuntimeMessage::UpdateBatch(UpdateBatchMessage { group_id, updates });
                self.group_broadcast.trigger(
                    GroupBroadcastPortRequest::build_submit(DeliveryClass::BestEffort)
                        .for_member_in_group(self.local_member.clone(), group_id)
                        .with_payload(message.encode_proto_to_bytes()),
                );
            }
            Err(error) => {
                warn!(
                    self.log(),
                    "failed to fulfil NeedRange for group {}: {}", group_id, error
                );
            }
        }
    }

    /// Rebuild the local availability cache from the store after detecting stale state.
    async fn refresh_known_available_from_store(&mut self) -> Result<(), CatchUpError> {
        let mut transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(catch_up::StoreAccessSnafu)?;
        let groups = transaction
            .load_replication_groups()
            .await
            .context(catch_up::StoreAccessSnafu)?;
        let mut available_by_group = HashMap::new();
        for group in groups {
            let update_ids = transaction
                .load_replication_update_ids(&group.group_id, ReplicationUpdateFilter::All, None)
                .await
                .context(catch_up::StoreAccessSnafu)?;
            let available = ProducerVersionSets::from_update_ids(&update_ids);
            if !available.is_empty() {
                available_by_group.insert(group.group_id, available);
            }
        }
        self.known_available = available_by_group;
        Ok(())
    }

    /// Load one bounded or unlimited response batch without borrowing component state across await.
    fn load_update_batch_from_store(
        &self,
        group_id: GroupId,
        ranges: Vec<UpdateRangeMessage>,
    ) -> impl Future<Output = Result<Vec<UpdateMessage>, CatchUpError>> + Send + 'static {
        load_update_batch_from_store(
            self.store.clone(),
            group_id,
            ranges,
            self.max_updates_per_batch,
            self.log().clone(),
        )
    }
}

/// Load one bounded or unlimited response batch for already-filtered producer ranges.
async fn load_update_batch_from_store(
    store: Arc<dyn ReplicationStore>,
    group_id: GroupId,
    ranges: Vec<UpdateRangeMessage>,
    max_updates_per_batch: Option<NonZeroUsize>,
    logger: KompactLogger,
) -> Result<Vec<UpdateMessage>, CatchUpError> {
    let mut transaction = store
        .begin_read_transaction()
        .await
        .context(catch_up::StoreAccessSnafu)?;
    let mut updates = Vec::new();
    for range in ranges {
        let limit = if let Some(max_updates_per_batch) = max_updates_per_batch {
            let remaining = max_updates_per_batch.get().saturating_sub(updates.len());
            if remaining == 0 {
                break;
            }
            Some(NonZeroUsize::new(remaining).unwrap())
        } else {
            None
        };
        let requested_limit = limit.map(NonZeroUsize::get);
        let loaded = transaction
            .load_replication_updates(
                &group_id,
                ReplicationUpdateFilter::ProducerRange {
                    producer_index: MemberIndex::new(range.producer_index),
                    start_version: range.start_version,
                    end_version: range.end_version,
                },
                limit,
            )
            .await
            .context(catch_up::StoreAccessSnafu)?;
        let loaded_count = loaded.len();
        updates.extend(loaded.into_iter().map(UpdateMessage::from));
        if let Some(requested_limit) = requested_limit
            && loaded_count > requested_limit
        {
            warn!(
                logger,
                "store implementation violated catch-up request limit for group {} by returning {} updates after limit {}; truncating response",
                group_id,
                loaded_count,
                requested_limit
            );
        }
        // The store API should honour the per-query limit above. This
        // defensive branch only runs if an implementation violates that
        // contract and returns more records than requested.
        if let Some(max_updates_per_batch) = max_updates_per_batch
            && updates.len() > max_updates_per_batch.get()
        {
            updates.truncate(max_updates_per_batch.get());
            break;
        }
    }
    Ok(updates)
}

impl ComponentLifecycle for CatchUpManagerComponent {
    fn on_start(&mut self) -> HandlerResult {
        self.retry_delay = self.read_retry_delay_from_config();
        self.max_updates_per_batch = self.read_max_updates_per_batch_from_config();
        Handled::block_on(self, async move |mut async_self| {
            async_self
                .refresh_known_available_from_store()
                .await
                .whatever_unrecoverable("catch-up manager startup failed")?;
            Handled::OK
        })
    }

    fn on_stop(&mut self) -> HandlerResult {
        self.cancel_retry_timers();
        Handled::OK
    }

    fn on_kill(&mut self) -> HandlerResult {
        self.on_stop()
    }
}

impl Require<GroupBroadcastPort> for CatchUpManagerComponent {
    fn handle(&mut self, indication: GroupBroadcastPortIndication) -> HandlerResult {
        let GroupBroadcastPortIndication::Deliver(deliver) = indication;
        self.handle_group_delivery(&deliver)
    }
}

impl Actor for CatchUpManagerComponent {
    type Message = CatchUpManagerMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match &msg {
            CatchUpManagerMessage::NeedVersions(need) => self.handle_need_versions(need),
            CatchUpManagerMessage::ObservedAvailable(observed) => {
                self.handle_observed_available(observed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        SqliteReplicationStore,
        api::{
            DatasetId,
            DatasetUpdateRecord,
            ReplicationGroupRecord,
            ReplicationUpdateRecord,
            current_slice_placeholder_group_security_material,
        },
    };
    use flotsync_core::{member::Identifier, versions::VersionVector};
    use flotsync_io::test_support::{build_test_kompact_system, wait_for_future};
    use flotsync_messages::datamodel as datamodel_proto;
    use uuid::Uuid;

    const STORE_FUTURE_TIMEOUT: Duration = Duration::from_secs(5);

    fn update_range(
        producer_index: u32,
        start_version: u64,
        end_version: u64,
    ) -> UpdateRangeMessage {
        UpdateRangeMessage {
            producer_index,
            start_version,
            end_version,
        }
    }

    fn wait_for_store_future<F>(future: F) -> F::Output
    where
        F: std::future::Future,
    {
        wait_for_future(
            STORE_FUTURE_TIMEOUT,
            future,
            "timed out waiting for catch-up store future",
        )
    }

    fn local_member() -> MemberIdentity {
        Identifier::from_array(["catch-up", "alice"])
    }

    fn remote_member() -> MemberIdentity {
        Identifier::from_array(["catch-up", "bob"])
    }

    fn sample_group(group_id: GroupId) -> ReplicationGroupRecord {
        ReplicationGroupRecord {
            group_id,
            members: vec![local_member(), remote_member()],
            local_member_index: MemberIndex::new(0),
            version_vector: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
            security_material: current_slice_placeholder_group_security_material(group_id),
        }
    }

    fn sample_update(group_id: GroupId, version: u64) -> ReplicationUpdateRecord {
        ReplicationUpdateRecord {
            group_id,
            update_id: UpdateId {
                node_index: 0,
                version,
            },
            sender: local_member(),
            read_versions: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
            dataset_updates: vec![DatasetUpdateRecord {
                dataset_id: DatasetId::try_new("docs").expect("dataset id should build"),
                operations: vec![datamodel_proto::SchemaOperation::default()],
            }],
            applied_locally: true,
        }
    }

    fn persist_group_with_updates(
        store: &Arc<dyn ReplicationStore>,
        group_id: GroupId,
        update_count: u64,
    ) {
        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
            .expect("group should persist");
        for version in 1..=update_count {
            wait_for_store_future(
                transaction.append_replication_update(sample_update(group_id, version)),
            )
            .expect("update should persist");
        }
        wait_for_store_future(transaction.commit()).expect("transaction should commit");
    }

    #[test]
    fn producer_version_sets_preserve_holes() {
        let mut versions = ProducerVersionSets::new();
        versions.insert_ranges(&[update_range(0, 1, 2), update_range(0, 5, 6)]);

        assert_eq!(
            versions.to_message_ranges(),
            vec![update_range(0, 1, 2), update_range(0, 5, 6)]
        );
    }

    #[test]
    fn producer_version_sets_subtract_available_versions() {
        let mut needed = ProducerVersionSets::new();
        needed.insert_ranges(&[update_range(0, 1, 6)]);
        let available =
            ProducerVersionSets::from_ranges(&[update_range(0, 2, 4), update_range(0, 6, 6)]);

        needed.subtract_sets(&available);

        assert_eq!(
            needed.to_message_ranges(),
            vec![update_range(0, 1, 1), update_range(0, 5, 5)]
        );
    }

    #[test]
    fn pending_need_reports_only_new_missing_versions() {
        let mut pending = PendingNeed::new();
        let first_request = ProducerVersionSets::from_ranges(&[update_range(0, 1, 3)]);
        let duplicate_request = ProducerVersionSets::from_ranges(&[update_range(0, 1, 3)]);
        let overlapping_request = ProducerVersionSets::from_ranges(&[update_range(0, 2, 4)]);

        assert!(pending.insert_sets(&first_request));
        assert!(!pending.insert_sets(&duplicate_request));
        assert!(pending.insert_sets(&overlapping_request));
        assert_eq!(pending.to_message_ranges(), vec![update_range(0, 1, 4)]);
    }

    #[test]
    fn load_update_batch_honours_unlimited_batch_mode() {
        let group_id = GroupId(Uuid::from_u128(80_001));
        let store: Arc<dyn ReplicationStore> = Arc::new(
            SqliteReplicationStore::in_memory(local_member()).expect("store should build"),
        );
        persist_group_with_updates(&store, group_id, 20);
        let system = build_test_kompact_system();
        let logger = system.logger().clone();

        let bounded_updates = wait_for_store_future(load_update_batch_from_store(
            store.clone(),
            group_id,
            vec![update_range(0, 1, 20)],
            NonZeroUsize::new(DEFAULT_MAX_UPDATES_PER_BATCH),
            logger.clone(),
        ))
        .expect("bounded batch should load");
        assert_eq!(bounded_updates.len(), DEFAULT_MAX_UPDATES_PER_BATCH);

        let unlimited_updates = wait_for_store_future(load_update_batch_from_store(
            store,
            group_id,
            vec![update_range(0, 1, 20)],
            None,
            logger,
        ))
        .expect("unlimited batch should load");
        let loaded_versions = unlimited_updates
            .iter()
            .map(|update| update.update_id.version)
            .collect::<Vec<_>>();
        assert_eq!(loaded_versions, (1..=20).collect::<Vec<_>>());
    }
}
