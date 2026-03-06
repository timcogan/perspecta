use std::io::{self, Write};
use std::sync::OnceLock;

use log::{LevelFilter, Log, Metadata, Record};

struct StderrLogger;

static LOGGER: StderrLogger = StderrLogger;

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

pub fn init() {
    static INIT: OnceLock<()> = OnceLock::new();

    INIT.get_or_init(|| {
        let level = level_from_env();
        if log::set_logger(&LOGGER).is_ok() {
            log::set_max_level(level);
        }
    });
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
    spec.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .find_map(|part| {
            let level_token = part.rsplit('=').next().unwrap_or(part).trim();
            parse_level(level_token)
        })
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
        assert_eq!(
            parse_level_filter("perspecta=debug"),
            Some(LevelFilter::Debug)
        );
        assert_eq!(parse_level_filter("invalid,info"), Some(LevelFilter::Info));
        assert_eq!(
            parse_level_filter("perspecta=invalid,other=trace"),
            Some(LevelFilter::Trace)
        );
    }

    #[test]
    fn level_from_spec_defaults_to_info_for_missing_or_invalid_values() {
        assert_eq!(level_from_spec(None), LevelFilter::Info);
        assert_eq!(level_from_spec(Some("")), LevelFilter::Info);
        assert_eq!(level_from_spec(Some("invalid")), LevelFilter::Info);
    }
}
