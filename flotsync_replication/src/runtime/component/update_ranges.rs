//! Computes inbound causal gaps and clamps catch-up ranges to group final-version cuts.

use crate::{
    api::ReplicationGroupLifecycle,
    codecs::messages::UpdateRangeMessage,
    runtime::errors::{InboundDeliveryError, inbound},
};
use flotsync_core::{
    GroupId,
    MemberIndex,
    versions::{UpdateId, VersionVector},
};
use itertools::Itertools;
use snafu::prelude::*;

/// Return catch-up ranges required before an inbound update can apply.
///
/// The update can be blocked both by versions in its read frontier and by
/// earlier versions from the same producer. The latter are not present in
/// the read frontier because producer updates do not read themselves.
/// Range endpoints are inclusive. An empty result means no prerequisites are missing;
/// an out-of-group producer index reports `UpdateProducerIndexNotInGroup`.
pub(super) fn missing_ranges_for_update_frontier(
    local_versions: &VersionVector,
    group_id: GroupId,
    update_id: UpdateId,
    read_versions: &VersionVector,
) -> Result<Vec<UpdateRangeMessage>, InboundDeliveryError> {
    let mut ranges = local_versions
        .missing_version_ranges_to(read_versions)
        .into_iter()
        .map(UpdateRangeMessage::from)
        .collect_vec();
    let producer_index = update_id.node_index as usize;
    ensure!(
        producer_index < local_versions.num_members().get(),
        inbound::UpdateProducerIndexNotInGroupSnafu {
            group_id,
            producer_index: MemberIndex::new(update_id.node_index),
        }
    );
    let expected_producer_version = local_versions.version_at(producer_index) + 1;
    if update_id.version > expected_producer_version {
        ranges.push(UpdateRangeMessage {
            producer_index: update_id.node_index,
            start_version: expected_producer_version,
            end_version: update_id.version - 1,
        });
    }
    Ok(ranges)
}

/// Clamp producer ranges to the immutable replay cut of a non-open group.
///
/// Open groups retain their input ranges. Other groups drop out-of-group producers
/// and ranges starting beyond the cut, and clip inclusive end versions to the cut.
pub(super) fn bound_update_ranges(
    lifecycle: &ReplicationGroupLifecycle,
    ranges: Vec<UpdateRangeMessage>,
) -> Vec<UpdateRangeMessage> {
    let Some(final_versions) = lifecycle.final_versions() else {
        return ranges;
    };
    ranges
        .into_iter()
        .filter_map(|mut range| {
            let producer_index = range.producer_index as usize;
            if producer_index >= final_versions.num_members().get() {
                return None;
            }
            let final_version = final_versions.version_at(producer_index);
            if range.start_version > final_version {
                None
            } else {
                range.end_version = range.end_version.min(final_version);
                Some(range)
            }
        })
        .collect()
}
