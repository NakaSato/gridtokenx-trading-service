fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = &["../../../gridtokenx-iam-service/crates/iam-protocol/proto/identity.proto"];
    let includes = &["../../../gridtokenx-iam-service/crates/iam-protocol/proto"];

    connectrpc_build::Config::new()
        .files(protos)
        .includes(includes)
        .include_file("_identity_include.rs")
        .compile()?;

    Ok(())
}
