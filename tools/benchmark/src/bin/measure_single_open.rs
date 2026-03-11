use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::process::ExitCode;

const START_EVENT: &str = "single-open started";
const DICOM_DONE_EVENT: &str = "single-open dicom-load completed";
const COMPLETE_EVENT: &str = "single-open completed";

#[derive(Clone, Copy, Debug, PartialEq)]
struct SingleOpenRun {
    started_at: f64,
    dicom_done_at: Option<f64>,
    completed_at: Option<f64>,
}

impl SingleOpenRun {
    fn total_ms(self) -> Option<f64> {
        self.completed_at
            .map(|completed| (completed - self.started_at) * 1000.0)
    }

    fn dicom_load_ms(self) -> Option<f64> {
        self.dicom_done_at
            .map(|dicom_done| (dicom_done - self.started_at) * 1000.0)
    }

    fn render_ui_ms(self) -> Option<f64> {
        self.dicom_done_at
            .zip(self.completed_at)
            .map(|(dicom_done, completed)| (completed - dicom_done) * 1000.0)
    }
}

fn parse_log_line(line: &str) -> Option<(f64, &str)> {
    let line = line.trim();
    let rest = line.strip_prefix('[')?;
    let (timestamp, rest) = rest.split_once(']')?;
    let timestamp = timestamp.parse::<f64>().ok()?;
    let (_, event) = rest.split_once(" perf: ")?;
    Some((timestamp, event))
}

fn parse_runs<I>(lines: I) -> (Vec<SingleOpenRun>, Vec<String>)
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut runs = Vec::new();
    let mut warnings = Vec::new();
    let mut active = None::<SingleOpenRun>;

    for line in lines {
        let Some((timestamp, event)) = parse_log_line(line.as_ref()) else {
            continue;
        };

        if event == START_EVENT {
            if active.is_some() {
                warnings.push(
                    "Found a new 'single-open started' before the previous run completed; discarding the incomplete run."
                        .to_string(),
                );
            }
            active = Some(SingleOpenRun {
                started_at: timestamp,
                dicom_done_at: None,
                completed_at: None,
            });
            continue;
        }

        let Some(mut run) = active else {
            continue;
        };

        if event == DICOM_DONE_EVENT {
            run.dicom_done_at = Some(timestamp);
            active = Some(run);
            continue;
        }

        if event == COMPLETE_EVENT {
            run.completed_at = Some(timestamp);
            if run.dicom_done_at.is_some() {
                runs.push(run);
            } else {
                warnings.push(
                    "Found 'single-open completed' without a prior 'single-open dicom-load completed'; discarding the run."
                        .to_string(),
                );
            }
            active = None;
        }
    }

    if active.is_some() {
        warnings.push("Log ended before the active single-open run completed.".to_string());
    }

    (runs, warnings)
}

fn parse_runs_from_reader<R: BufRead>(reader: R) -> io::Result<(Vec<SingleOpenRun>, Vec<String>)> {
    let lines = reader.lines().collect::<io::Result<Vec<_>>>()?;
    Ok(parse_runs(lines))
}

fn print_usage() {
    eprintln!("Usage: cargo run -p benchmark-tools --bin measure_single_open -- [logfile]");
    eprintln!("Pass '-' or omit logfile to read from stdin.");
}

fn print_summary(runs: &[SingleOpenRun]) {
    println!("run,total_ms,dicom_load_ms,render_ui_ms");
    for (index, run) in runs.iter().enumerate() {
        let Some(total_ms) = run.total_ms() else {
            continue;
        };
        let Some(dicom_load_ms) = run.dicom_load_ms() else {
            continue;
        };
        let Some(render_ui_ms) = run.render_ui_ms() else {
            continue;
        };
        println!(
            "{},{total_ms:.3},{dicom_load_ms:.3},{render_ui_ms:.3}",
            index + 1
        );
    }
}

fn parse_file(path: &Path) -> io::Result<(Vec<SingleOpenRun>, Vec<String>)> {
    let file = File::open(path)?;
    parse_runs_from_reader(BufReader::new(file))
}

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(logfile) = args.next() else {
        match parse_runs_from_reader(io::stdin().lock()) {
            Ok((runs, warnings)) => return finish(runs, warnings),
            Err(err) => {
                eprintln!("error: could not read stdin: {err}");
                return ExitCode::FAILURE;
            }
        }
    };

    if matches!(logfile.as_str(), "-h" | "--help") {
        print_usage();
        return ExitCode::SUCCESS;
    }

    if args.next().is_some() {
        print_usage();
        return ExitCode::FAILURE;
    }

    let result = if logfile == "-" {
        parse_runs_from_reader(io::stdin().lock())
    } else {
        parse_file(Path::new(&logfile))
    };

    match result {
        Ok((runs, warnings)) => finish(runs, warnings),
        Err(err) => {
            eprintln!("error: could not read {logfile}: {err}");
            ExitCode::FAILURE
        }
    }
}

fn finish(runs: Vec<SingleOpenRun>, warnings: Vec<String>) -> ExitCode {
    if runs.is_empty() {
        for warning in warnings {
            eprintln!("warning: {warning}");
        }
        eprintln!("error: no complete single-open runs were found in the input log.");
        return ExitCode::FAILURE;
    }

    print_summary(&runs);
    for warning in warnings {
        eprintln!("warning: {warning}");
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_log_line_extracts_timestamp_and_event() {
        let line = "[1762361234.101] [INFO ] perf: single-open started";
        let parsed = parse_log_line(line);
        assert_eq!(parsed, Some((1762361234.101, START_EVENT)));
    }

    #[test]
    fn parse_runs_collects_complete_single_open() {
        let lines = [
            "[1762361234.101] [INFO ] perf: single-open started",
            "[1762361234.392] [INFO ] perf: single-open dicom-load completed",
            "[1762361234.407] [INFO ] perf: single-open completed",
        ];

        let (runs, warnings) = parse_runs(lines);
        assert!(warnings.is_empty());
        assert_eq!(runs.len(), 1);
        assert_timing(runs[0].total_ms(), 306.0);
        assert_timing(runs[0].dicom_load_ms(), 291.0);
        assert_timing(runs[0].render_ui_ms(), 15.0);
    }

    #[test]
    fn parse_runs_warns_on_incomplete_sequence() {
        let lines = [
            "[1762361234.101] [INFO ] perf: single-open started",
            "[1762361234.407] [INFO ] perf: single-open completed",
        ];

        let (runs, warnings) = parse_runs(lines);
        assert!(runs.is_empty());
        assert_eq!(warnings.len(), 1);
    }

    fn assert_timing(actual: Option<f64>, expected: f64) {
        let actual = actual.expect("timing should be present");
        let delta = (actual - expected).abs();
        assert!(
            delta < 0.001,
            "expected timing near {expected}, got {actual} (delta={delta})"
        );
    }
}
