#![feature(iterator_try_collect)]

// Re-export protobuf, so we use a consistent version.
pub use protobuf;
pub use uuid::Uuid;

pub mod codecs;
pub mod snapshots;

// Export the generated modules.
include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));

pub type InMemoryData =
    flotsync_data_types::schema::datamodel::InMemoryData<Uuid, flotsync_core::versions::UpdateId>;
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
        let mut synced = SyncedVersionVector::new();
        synced.group_version = 1;
        assert_eq!(synced.group_version, 1);

        let mut v = VersionVector::new();
        v.set_synced(synced);
        assert_eq!(v.synced().group_version, 1);
    }
}
