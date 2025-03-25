use std::io::Result;

fn main() -> Result<()> {
    #[cfg(feature = "datadog")]
    prost_build::compile_protos(&["proto/datadog/agent_payload.proto"], &["proto/datadog"])?;
    Ok(())
}
