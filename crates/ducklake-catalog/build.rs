use std::{env, fs, path::Path};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let pins =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/ducklake-upstream-pins.sh");
    println!("cargo:rerun-if-changed={}", pins.display());
    let source = fs::read_to_string(pins).expect("read canonical DuckLake pins");
    let commit = source
        .lines()
        .find_map(|line| line.strip_prefix("export DUCKLAKE_COMMIT="))
        .expect("canonical DuckLake commit")
        .trim_matches('"');
    assert!(commit.len() == 40 && commit.bytes().all(|byte| byte.is_ascii_hexdigit()));
    println!("cargo:rustc-env=AUX_DUCKLAKE_SUPPORTED_COMMIT={commit}");
    if env::var_os("CARGO_FEATURE_FOUNDATIONDB").is_none() {
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
