#![feature(iterator_try_collect)]

// Re-export buffa, so we use a consistent version.
pub use buffa;
pub use uuid::Uuid;

pub mod codecs;
pub mod proto;
pub mod serialisation;
pub mod snapshots;
pub mod wire;

mod generated {
    #![allow(
        clippy::pedantic,
        reason = "generated Buffa code is linted at the generator level, not by editing OUT_DIR output"
    )]

    include!(concat!(env!("OUT_DIR"), "/flotsync_messages.rs"));
}

pub use generated::{
    flotsync,
    flotsync::{
        datamodel::v1 as datamodel,
        delivery::v1 as delivery,
        discovery::v1 as discovery,
        endpoint::v1 as endpoint,
        replication::v1 as replication,
        security::v1 as security,
        versions::v1 as versions,
    },
};

pub type InMemoryStateData = flotsync_data_types::schema::datamodel::InMemoryStateData<
    Uuid,
    flotsync_core::versions::UpdateId,
>;
pub type SchemaOperation<'a> = flotsync_data_types::schema::datamodel::SchemaOperation<
    'a,
    Uuid,
    flotsync_core::versions::UpdateId,
>;

#[cfg(test)]
mod tests {
    use crate::versions::*;

    #[test]
    fn construct_versions() {
        let synced = SyncedVersionVector {
            group_version: 1,
            ..SyncedVersionVector::default()
        };
        assert_eq!(synced.group_version, 1);

        let v = VersionVector {
            versions: Some(version_vector::Versions::Synced(Box::new(synced))),
            ..VersionVector::default()
        };
        assert!(matches!(
            v.versions,
            Some(version_vector::Versions::Synced(ref value)) if value.group_version == 1
        ));
    }
}
