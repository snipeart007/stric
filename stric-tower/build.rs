fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(&["proto/stric_tower_wire.proto"], &["proto/"])?;
    Ok(())
}
