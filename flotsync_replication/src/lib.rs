//! Application-facing replication runtime and API contracts.
use arc_swap::ArcSwap;
use std::{collections::HashSet, sync::Arc};

pub mod api;
pub mod delivery;
pub mod runtime;

pub use api::*;
pub use runtime::load_replication_runtime;

/// Shared snapshot of the currently active groups that should be admitted into
/// the local delivery stack.
type SharedActiveGroups = Arc<ArcSwap<HashSet<GroupId>>>;
