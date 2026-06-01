fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(&["proto/stric_flow.proto"], &["proto/"])?;
    Ok(())
}
