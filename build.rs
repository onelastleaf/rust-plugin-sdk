use std::{env, error::Error};

fn main() -> Result<(), Box<dyn Error>> {
    for path in [
        "proto/oll/common.proto",
        "proto/oll/config.proto",
        "proto/oll/document.proto",
        "proto/oll/plugin.proto",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // Cargo build scripts are single-threaded; configure protoc before codegen.
    unsafe { env::set_var("PROTOC", protoc) };
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        // The RPC is itself named Connect, so the generated endpoint helper
        // would collide with it. SDKs construct the channel explicitly.
        .build_transport(false)
        .compile_protos(&["proto/oll/plugin.proto"], &["proto"])?;
    Ok(())
}
