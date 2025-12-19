use tonic_reflection::server::{v1, v1alpha};

// Expose our generated code to other crates in the workspace.
// https://doc.rust-lang.org/reference/items/modules.html#the-path-attribute
#[path = "collector/collector.rs"]
pub mod collector;

/// File descriptor set for gRPC reflection
pub const COLLECTOR_FD_SET: &[u8] = include_bytes!("collector/file_descriptor_set.bin");

/// v1 reflection; stabilized, newer version
pub fn collector_reflection_service_v1()
-> anyhow::Result<v1::ServerReflectionServer<impl v1::ServerReflection>> {
    tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(COLLECTOR_FD_SET)
        .build_v1()
        .map_err(anyhow::Error::new)
}

/// v1alpha reflection, for backwards compat.
pub fn collector_reflection_service_v1alpha()
-> anyhow::Result<v1alpha::ServerReflectionServer<impl v1alpha::ServerReflection>> {
    tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(COLLECTOR_FD_SET)
        .build_v1alpha()
        .map_err(anyhow::Error::new)
}
