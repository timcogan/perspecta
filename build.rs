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

    if !version_suffix.is_empty()
        && !is_timestamp_suffix(&version_suffix)
        && !is_web_preview_suffix(&version_suffix)
    {
        eprintln!(
            "{VERSION_SUFFIX_ENV} must be empty, match -YYYYMMDDHHMMSS, or match +web.<7-hex>[.dirty]"
        );
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

fn is_web_preview_suffix(value: &str) -> bool {
    let Some(value) = value.strip_prefix("+web.") else {
        return false;
    };
    let commit = value.strip_suffix(".dirty").unwrap_or(value);
    commit.len() == 7
        && commit
            .chars()
            .all(|character| matches!(character, '0'..='9' | 'a'..='f'))
}
