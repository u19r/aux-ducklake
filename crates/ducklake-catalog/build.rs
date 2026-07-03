use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var_os("CARGO_FEATURE_FOUNDATIONDB").is_none() {
        return;
    }
    for path in ["/usr/local/lib", "/opt/homebrew/lib"] {
        let lib = Path::new(path).join("libfdb_c.dylib");
        if lib.exists() {
            println!("cargo:rustc-link-search=native={path}");
            println!("cargo:rustc-link-arg=-Wl,-rpath,{path}");
        }
    }
}
