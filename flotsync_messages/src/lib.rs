// Re-export protobuf, so we use a consistent version.
pub use protobuf;

// Export the generated modules.
include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));

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
