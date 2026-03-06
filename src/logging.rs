use std::io::{self, Write};
use std::sync::OnceLock;

use log::{LevelFilter, Log, Metadata, Record, SetLoggerError};

struct StderrLogger;

static LOGGER: StderrLogger = StderrLogger;
static INIT: OnceLock<Result<(), SetLoggerError>> = OnceLock::new();

impl Log for StderrLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level().to_level_filter() <= log::max_level()
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let mut stderr = io::stderr().lock();
        let _ = writeln!(
            stderr,
            "[{level:<5}] {target}: {message}",
            level = record.level(),
            target = record.target(),
            message = record.args()
        );
    }

    fn flush(&self) {}
}

pub fn init() -> Result<(), SetLoggerError> {
    try_init()
}

pub fn try_init() -> Result<(), SetLoggerError> {
    match INIT.get_or_init(|| {
        let level = level_from_env();
        log::set_logger(&LOGGER)?;
        log::set_max_level(level);
        Ok(())
    }) {
        Ok(()) => Ok(()),
        // SetLoggerError is not Clone, so regenerate an owned error value.
        Err(_) => match log::set_logger(&LOGGER) {
            Ok(()) => unreachable!(
                "logging::try_init: LOGGER registration succeeded after cached failure"
            ),
            Err(err) => Err(err),
        },
    }
}

fn level_from_env() -> LevelFilter {
    let raw = std::env::var("RUST_LOG").ok();
    level_from_spec(raw.as_deref())
}

fn level_from_spec(spec: Option<&str>) -> LevelFilter {
    spec.and_then(parse_level_filter)
        .unwrap_or(LevelFilter::Info)
}

fn parse_level_filter(spec: &str) -> Option<LevelFilter> {
    let mut global_level = None;

    for part in spec
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        // Skip per-target directives (e.g. "perspecta=debug"): this logger
        // currently supports only one global LevelFilter.
        if part.contains('=') {
            continue;
        }

        if let Some(level) = parse_level(part) {
            global_level = Some(level);
            break;
        }
    }

    global_level
}

fn parse_level(token: &str) -> Option<LevelFilter> {
    match token.to_ascii_lowercase().as_str() {
        "off" => Some(LevelFilter::Off),
        "error" => Some(LevelFilter::Error),
        "warn" | "warning" => Some(LevelFilter::Warn),
        "info" => Some(LevelFilter::Info),
        "debug" => Some(LevelFilter::Debug),
        "trace" => Some(LevelFilter::Trace),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_level_supports_expected_tokens() {
        assert_eq!(parse_level("off"), Some(LevelFilter::Off));
        assert_eq!(parse_level("error"), Some(LevelFilter::Error));
        assert_eq!(parse_level("warn"), Some(LevelFilter::Warn));
        assert_eq!(parse_level("warning"), Some(LevelFilter::Warn));
        assert_eq!(parse_level("info"), Some(LevelFilter::Info));
        assert_eq!(parse_level("debug"), Some(LevelFilter::Debug));
        assert_eq!(parse_level("trace"), Some(LevelFilter::Trace));
    }

    #[test]
    fn parse_level_filter_supports_directives_and_fallback_entries() {
        assert_eq!(parse_level_filter("warn"), Some(LevelFilter::Warn));
        assert_eq!(parse_level_filter("perspecta=debug"), None);
        assert_eq!(
            parse_level_filter("perspecta=debug,info"),
            Some(LevelFilter::Info)
        );
        assert_eq!(parse_level_filter("perspecta=invalid,other=trace"), None);
        assert_eq!(
            parse_level_filter("invalid,perspecta=debug,info"),
            Some(LevelFilter::Info)
        );
        assert_eq!(parse_level_filter("invalid,info"), Some(LevelFilter::Info));
    }

    #[test]
    fn level_from_spec_defaults_to_info_for_missing_or_invalid_values() {
        assert_eq!(level_from_spec(None), LevelFilter::Info);
        assert_eq!(level_from_spec(Some("")), LevelFilter::Info);
        assert_eq!(level_from_spec(Some("invalid")), LevelFilter::Info);
    }
}
