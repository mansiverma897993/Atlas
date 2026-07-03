//! Compiles the `.proto` contracts into Rust via tonic/prost.
//!
//! Uses a **vendored** `protoc` (from the `protoc-bin-vendored` crate) so the build does not
//! require a system-installed protobuf compiler — keeping `cargo build` hermetic in CI and on
//! developer machines that lack `protoc`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Point prost-build at the vendored protoc binary.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let protos = ["proto/identity.proto", "proto/ledger.proto"];
    for p in &protos {
        println!("cargo:rerun-if-changed={p}");
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&protos, &["proto"])?;

    Ok(())
}
