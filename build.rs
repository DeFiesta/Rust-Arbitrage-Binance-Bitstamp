fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .compile(&["proto/orderbook.proto"], &["proto/"])
        .unwrap();
    Ok(())
}
