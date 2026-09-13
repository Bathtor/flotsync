use flotsync_core::ApplicationId;
use flotsync_replication::{
    ApplicationSchemas,
    ReplicationApi,
    ReplicationConfig,
    ReplicationEventListener,
    ReplicationRuntime,
    ReplicationRuntimeBuilder,
    ReplicationSecuritySecrets,
    ReplicationStore,
};
use std::sync::Arc;

fn assert_send<T: Send>(_: T) {}

fn main() {
    let normal_load = |application_id: ApplicationId,
                       application_schemas: &'static ApplicationSchemas,
                       store: Arc<dyn ReplicationStore>,
                       listener: Arc<dyn ReplicationEventListener>,
                       config: ReplicationConfig,
                       security_secrets: ReplicationSecuritySecrets| {
        let builder = ReplicationRuntime::builder(application_id)
            .application_schemas(application_schemas)
            .store(store)
            .listener(listener)
            .config(config)
            .security_secrets(security_secrets);
        assert_send(builder.load());
    };
    let configured_load = |builder: ReplicationRuntimeBuilder| {
        let builder = {
            let runtime_config_toml = String::new();
            builder.runtime_config_toml(&runtime_config_toml)
        };
        assert_send(builder.load());
    };
    let api_operation = |api: &dyn ReplicationApi| {
        assert_send(api.local_public_key_bundle());
    };

    let _compile_checks = (normal_load, configured_load, api_operation);
}
