use std::io::Result;

fn main() -> Result<()> {
    println!("cargo::rerun-if-changed=proto/datadog");
    prost_build::compile_protos(&["proto/datadog/agent_payload.proto"], &["proto/datadog"])?;
    Ok(())
}
