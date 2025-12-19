fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir("src/collector")
        .file_descriptor_set_path("src/collector/file_descriptor_set.bin")
        .compile_protos(&["proto/collector.proto"], &["proto/"])?;

    Ok(())
}
