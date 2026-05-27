use std::env;
use std::process;

const VERSION_SUFFIX_ENV: &str = "PERSPECTA_VERSION_SUFFIX";
const DISPLAY_VERSION_ENV: &str = "PERSPECTA_DISPLAY_VERSION";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed={VERSION_SUFFIX_ENV}");

    let package_version =
        env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown-version".to_string());
    let version_suffix = env::var(VERSION_SUFFIX_ENV).unwrap_or_default();

    if !version_suffix.is_empty() && !is_timestamp_suffix(&version_suffix) {
        eprintln!("{VERSION_SUFFIX_ENV} must be empty or match -YYYYMMDDHHMMSS");
        process::exit(1);
    }

    println!("cargo:rustc-env={DISPLAY_VERSION_ENV}={package_version}{version_suffix}");
}

fn is_timestamp_suffix(value: &str) -> bool {
    value.len() == 15
        && value.starts_with('-')
        && value[1..]
            .chars()
            .all(|character| character.is_ascii_digit())
}
