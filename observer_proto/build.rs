fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir("src/collector")
        .compile_protos(&["proto/collector.proto"], &["proto/"])?;

    Ok(())
}
