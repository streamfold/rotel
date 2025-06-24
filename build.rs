use std::env;
use std::io::Result;

fn main() -> Result<()> {
    println!("cargo::rerun-if-changed=proto/datadog");
    prost_build::compile_protos(&["proto/datadog/agent_payload.proto"], &["proto/datadog"])?;

    // Expose OUT_DIR for env!("OUT_DIR") calls in source code after build script runs.
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");
    println!("cargo:rustc-env=OUT_DIR={}", out_dir);
    Ok(())
}
