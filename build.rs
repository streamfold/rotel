use std::io::Result;

fn main() -> Result<()> {
    println!("cargo::rustc-check-cfg=cfg(kafka_integration_tests, values(\"true\"))");
    println!("cargo::rerun-if-env-changed=KAFKA_INTEGRATION_TESTS");
    println!("cargo::rerun-if-changed=proto/datadog");
    prost_build::compile_protos(&["proto/datadog/agent_payload.proto"], &["proto/datadog"])?;

    let env = std::env::var("KAFKA_INTEGRATION_TESTS").unwrap_or("false".to_string());
    println!("cargo:rustc-cfg=kafka_integration_tests=\"{}\"", env);

    Ok(())
}
