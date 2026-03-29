fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = &["proto/trading.proto"];
    let includes = &["proto"];

    connectrpc_build::Config::new()
        .files(protos)
        .includes(includes)
        .include_file("_trading_include.rs")
        .compile()?;

    Ok(())
}
