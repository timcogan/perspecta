use std::collections::VecDeque;
use std::env;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::io::{BufRead, BufReader, Read};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use benchmark_tools::benchmark_support::{write_synthetic_dicom, TempBenchmarkDir};

const LOG_CONTEXT_LINES: usize = 24;
const START_EVENT: &str = "single-open started";
const DICOM_DONE_EVENT: &str = "single-open dicom-load completed";
const COMPLETE_EVENT: &str = "single-open completed";

#[derive(Clone, Debug, PartialEq, Eq)]
struct BenchmarkConfig {
    app_path: Option<PathBuf>,
    runs: usize,
    warmup: usize,
    rows: usize,
    cols: usize,
    timeout_secs: u64,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            app_path: None,
            runs: 10,
            warmup: 1,
            rows: 1024,
            cols: 1024,
            timeout_secs: 15,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct FullAppRun {
    total_ms: f64,
    startup_ms: f64,
    dicom_load_ms: f64,
    render_ui_ms: f64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LaunchMode {
    Direct,
    Xvfb(PathBuf),
}

#[derive(Clone, Copy)]
enum ColumnAlign {
    Left,
    Right,
}

fn print_usage() {
    eprintln!(
        "Usage: cargo run -p benchmark-tools --bin benchmark_full_single_open -- [--app PATH] [--runs N] [--warmup N] [--rows N] [--cols N] [--timeout-secs N]"
    );
    eprintln!(
        "If no display is available, the benchmark will try to use `xvfb-run -a` automatically."
    );
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let Some(value) = value else {
        bail!("missing value for {flag}");
    };
    let parsed = value
        .parse::<usize>()
        .with_context(|| format!("invalid integer for {flag}: {value}"))?;
    if parsed == 0 {
        bail!("{flag} must be greater than zero");
    }
    Ok(parsed)
}

fn parse_nonnegative_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let Some(value) = value else {
        bail!("missing value for {flag}");
    };
    value
        .parse::<usize>()
        .with_context(|| format!("invalid integer for {flag}: {value}"))
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let Some(value) = value else {
        bail!("missing value for {flag}");
    };
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("invalid integer for {flag}: {value}"))?;
    if parsed == 0 {
        bail!("{flag} must be greater than zero");
    }
    Ok(parsed)
}

fn parse_config() -> Result<BenchmarkConfig> {
    let mut config = BenchmarkConfig::default();
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            "--app" => {
                let Some(path) = args.next() else {
                    bail!("missing value for --app");
                };
                config.app_path = Some(PathBuf::from(path));
            }
            "--runs" => config.runs = parse_usize_arg("--runs", args.next())?,
            "--warmup" => config.warmup = parse_nonnegative_usize_arg("--warmup", args.next())?,
            "--rows" => config.rows = parse_usize_arg("--rows", args.next())?,
            "--cols" => config.cols = parse_usize_arg("--cols", args.next())?,
            "--timeout-secs" => config.timeout_secs = parse_u64_arg("--timeout-secs", args.next())?,
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(config)
}

fn current_epoch_ms() -> f64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_secs() as f64 * 1000.0 + now.subsec_millis() as f64
}

fn parse_log_line(line: &str) -> Option<(f64, &str)> {
    let line = line.trim();
    let rest = line.strip_prefix('[')?;
    let (timestamp, rest) = rest.split_once(']')?;
    let timestamp = timestamp.parse::<f64>().ok()?;
    let (_, event) = rest.split_once(" perf: ")?;
    Some((timestamp, event))
}

fn resolve_app_path(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }

    let current_exe = env::current_exe().context("could not resolve current executable path")?;
    let Some(parent) = current_exe.parent() else {
        bail!(
            "could not resolve parent directory for {}",
            current_exe.display()
        );
    };
    let app_path = parent.join(format!("perspecta{}", env::consts::EXE_SUFFIX));
    if !app_path.is_file() {
        bail!(
            "could not find the perspecta app binary at {}. Build it first with `cargo build --bin perspecta` or pass --app PATH.",
            app_path.display()
        );
    }
    Ok(app_path)
}

fn has_display() -> bool {
    has_display_vars(
        env::var_os("DISPLAY"),
        env::var_os("WAYLAND_DISPLAY"),
        env::var_os("WAYLAND_SOCKET"),
    )
}

fn has_display_vars(
    display: Option<OsString>,
    wayland_display: Option<OsString>,
    wayland_socket: Option<OsString>,
) -> bool {
    [display, wayland_display, wayland_socket]
        .into_iter()
        .flatten()
        .any(|value| !value.is_empty())
}

fn find_executable_in_path(name: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    for entry in env::split_paths(&path) {
        let candidate = entry.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn resolve_launch_mode() -> Result<LaunchMode> {
    if !cfg!(target_os = "linux") {
        return Ok(LaunchMode::Direct);
    }

    resolve_linux_launch_mode(has_display(), find_executable_in_path("xvfb-run"))
}

fn resolve_linux_launch_mode(has_display: bool, xvfb_path: Option<PathBuf>) -> Result<LaunchMode> {
    if has_display {
        return Ok(LaunchMode::Direct);
    }

    if let Some(path) = xvfb_path {
        return Ok(LaunchMode::Xvfb(path));
    }

    bail!("no DISPLAY or Wayland socket is available, and `xvfb-run` was not found in PATH")
}

fn build_app_command(launch_mode: &LaunchMode, app_path: &Path, dicom_path: &Path) -> Command {
    match launch_mode {
        LaunchMode::Direct => {
            let mut command = Command::new(app_path);
            command.arg(dicom_path);
            command
        }
        LaunchMode::Xvfb(xvfb_path) => {
            let mut command = Command::new(xvfb_path);
            command
                .arg("-e")
                .arg("/dev/stderr")
                .arg("-a")
                .arg(app_path)
                .arg(dicom_path);
            configure_xvfb_process_group(&mut command);
            command
        }
    }
}

#[cfg(unix)]
fn configure_xvfb_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_xvfb_process_group(_command: &mut Command) {}

fn launch_mode_label(launch_mode: &LaunchMode) -> &'static str {
    match launch_mode {
        LaunchMode::Direct => "direct",
        LaunchMode::Xvfb(_) => "xvfb",
    }
}

fn render_table(headers: &[&str], aligns: &[ColumnAlign], rows: &[Vec<String>]) -> String {
    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.len());
        }
    }

    let mut output = String::new();
    push_table_rule(&mut output, &widths);
    push_table_row(
        &mut output,
        &headers
            .iter()
            .map(|header| (*header).to_string())
            .collect::<Vec<_>>(),
        &widths,
        aligns,
    );
    push_table_rule(&mut output, &widths);
    for row in rows {
        push_table_row(&mut output, row, &widths, aligns);
    }
    push_table_rule(&mut output, &widths);
    output
}

fn push_table_rule(output: &mut String, widths: &[usize]) {
    output.push('+');
    for width in widths {
        let _ = write!(output, "{:-<width$}+", "", width = width + 2);
    }
    output.push('\n');
}

fn push_table_row(output: &mut String, cells: &[String], widths: &[usize], aligns: &[ColumnAlign]) {
    output.push('|');
    for ((cell, width), align) in cells.iter().zip(widths.iter()).zip(aligns.iter()) {
        match align {
            ColumnAlign::Left => {
                let _ = write!(output, " {cell:<width$} |", width = *width);
            }
            ColumnAlign::Right => {
                let _ = write!(output, " {cell:>width$} |", width = *width);
            }
        }
    }
    output.push('\n');
}

fn spawn_output_reader<R>(reader: R, tx: mpsc::Sender<Result<String, String>>)
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let reader = BufReader::new(reader);
        for line in reader.lines() {
            match line {
                Ok(line) => {
                    if tx.send(Ok(line)).is_err() {
                        break;
                    }
                }
                Err(err) => {
                    let _ = tx.send(Err(format!("failed to read app output: {err}")));
                    break;
                }
            }
        }
    });
}

fn push_log_line(last_lines: &mut VecDeque<String>, line: String) {
    if last_lines.len() == LOG_CONTEXT_LINES {
        last_lines.pop_front();
    }
    last_lines.push_back(line);
}

fn format_log_context(last_lines: &VecDeque<String>) -> String {
    if last_lines.is_empty() {
        return "no stderr output captured".to_string();
    }
    last_lines.iter().cloned().collect::<Vec<_>>().join("\n")
}

fn kill_child(child: &mut std::process::Child, launch_mode: &LaunchMode) {
    match child.try_wait() {
        Ok(Some(_)) => {}
        Ok(None) => {
            terminate_child(child, launch_mode);
            let _ = child.wait();
        }
        Err(_) => {
            terminate_child(child, launch_mode);
            let _ = child.wait();
        }
    }
}

fn terminate_child(child: &mut std::process::Child, launch_mode: &LaunchMode) {
    #[cfg(unix)]
    {
        if matches!(launch_mode, LaunchMode::Xvfb(_)) {
            if let Ok(pid) = i32::try_from(child.id()) {
                // Safety: `xvfb-run` is spawned in its own process group, so signaling `-pid`
                // targets that group and terminates the wrapper and its children together.
                if unsafe { libc::kill(-pid, libc::SIGKILL) } == 0 {
                    return;
                }
            }
        }
    }

    let _ = child.kill();
}

fn compute_run(
    spawn_ms: f64,
    started_ms: f64,
    dicom_done_ms: f64,
    completed_ms: f64,
) -> FullAppRun {
    FullAppRun {
        total_ms: completed_ms - spawn_ms,
        startup_ms: started_ms - spawn_ms,
        dicom_load_ms: dicom_done_ms - started_ms,
        render_ui_ms: completed_ms - dicom_done_ms,
    }
}

fn run_once(
    launch_mode: &LaunchMode,
    app_path: &Path,
    dicom_path: &Path,
    timeout: Duration,
) -> Result<FullAppRun> {
    let spawn_ms = current_epoch_ms();
    let mut command = build_app_command(launch_mode, app_path, dicom_path);
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("RUST_LOG", "info")
        .spawn()
        .with_context(|| format!("could not launch {}", app_path.display()))?;
    let stdout = child
        .stdout
        .take()
        .context("failed to capture child stdout")?;
    let stderr = child
        .stderr
        .take()
        .context("failed to capture child stderr")?;

    let (tx, rx) = mpsc::channel::<Result<String, String>>();
    spawn_output_reader(stdout, tx.clone());
    spawn_output_reader(stderr, tx);

    let deadline = Instant::now() + timeout;
    let mut last_lines = VecDeque::new();
    let mut started_ms = None::<f64>;
    let mut dicom_done_ms = None::<f64>;
    let mut completed_ms = None::<f64>;

    loop {
        if Instant::now() >= deadline {
            kill_child(&mut child, launch_mode);
            bail!(
                "timed out waiting for full-app benchmark completion after {:.1}s\nlast stderr lines:\n{}",
                timeout.as_secs_f64(),
                format_log_context(&last_lines)
            );
        }

        let wait_time = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(100));

        match rx.recv_timeout(wait_time) {
            Ok(Ok(line)) => {
                push_log_line(&mut last_lines, line.clone());
                if let Some((timestamp, event)) = parse_log_line(&line) {
                    match event {
                        START_EVENT => started_ms = Some(timestamp * 1000.0),
                        DICOM_DONE_EVENT => dicom_done_ms = Some(timestamp * 1000.0),
                        COMPLETE_EVENT => {
                            completed_ms = Some(timestamp * 1000.0);
                            kill_child(&mut child, launch_mode);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Ok(Err(err)) => {
                kill_child(&mut child, launch_mode);
                bail!("{err}");
            }
            Err(RecvTimeoutError::Timeout) => {
                if let Some(status) = child.try_wait().context("failed to poll child process")? {
                    while let Ok(message) = rx.try_recv() {
                        match message {
                            Ok(line) => {
                                push_log_line(&mut last_lines, line.clone());
                                if let Some((timestamp, event)) = parse_log_line(&line) {
                                    match event {
                                        START_EVENT => started_ms = Some(timestamp * 1000.0),
                                        DICOM_DONE_EVENT => {
                                            dicom_done_ms = Some(timestamp * 1000.0)
                                        }
                                        COMPLETE_EVENT => completed_ms = Some(timestamp * 1000.0),
                                        _ => {}
                                    }
                                }
                            }
                            Err(err) => bail!("{err}"),
                        }
                    }

                    if completed_ms.is_some() {
                        break;
                    }

                    bail!(
                        "perspecta exited with status {status} before benchmark completion\nlast stderr lines:\n{}",
                        format_log_context(&last_lines)
                    );
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                if let Some(status) = child.try_wait().context("failed to poll child process")? {
                    if completed_ms.is_some() {
                        break;
                    }
                    bail!(
                        "stderr reader disconnected after app exit with status {status}\nlast stderr lines:\n{}",
                        format_log_context(&last_lines)
                    );
                }
                kill_child(&mut child, launch_mode);
                bail!(
                    "stderr reader disconnected before benchmark completion\nlast stderr lines:\n{}",
                    format_log_context(&last_lines)
                );
            }
        }
    }

    let started_ms = started_ms.context("app never emitted 'single-open started'")?;
    let dicom_done_ms =
        dicom_done_ms.context("app never emitted 'single-open dicom-load completed'")?;
    let completed_ms = completed_ms.context("app never emitted 'single-open completed'")?;
    Ok(compute_run(
        spawn_ms,
        started_ms,
        dicom_done_ms,
        completed_ms,
    ))
}

fn metric_stats(values: &[f64]) -> Option<(f64, f64, f64)> {
    let first = *values.first()?;
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let median = if sorted.len() % 2 == 1 {
        sorted[sorted.len() / 2]
    } else {
        let upper = sorted.len() / 2;
        (sorted[upper - 1] + sorted[upper]) / 2.0
    };
    let min = first.min(*sorted.first().unwrap_or(&first));
    let max = first.max(*sorted.last().unwrap_or(&first));
    Some((median, min, max))
}

fn run_table_rows(runs: &[FullAppRun]) -> Vec<Vec<String>> {
    runs.iter()
        .enumerate()
        .map(|(index, run)| {
            vec![
                (index + 1).to_string(),
                format!("{:.3}", run.total_ms),
                format!("{:.3}", run.startup_ms),
                format!("{:.3}", run.dicom_load_ms),
                format!("{:.3}", run.render_ui_ms),
            ]
        })
        .collect()
}

fn summary_table_rows(runs: &[FullAppRun]) -> Vec<Vec<String>> {
    [
        (
            "total",
            runs.iter().map(|run| run.total_ms).collect::<Vec<_>>(),
        ),
        (
            "startup",
            runs.iter().map(|run| run.startup_ms).collect::<Vec<_>>(),
        ),
        (
            "dicom_load",
            runs.iter().map(|run| run.dicom_load_ms).collect::<Vec<_>>(),
        ),
        (
            "render_ui",
            runs.iter().map(|run| run.render_ui_ms).collect::<Vec<_>>(),
        ),
    ]
    .into_iter()
    .filter_map(|(label, values)| {
        metric_stats(&values).map(|(median, min, max)| {
            vec![
                label.to_string(),
                format!("{median:.3}"),
                format!("{min:.3}"),
                format!("{max:.3}"),
            ]
        })
    })
    .collect()
}

fn print_report(config: &BenchmarkConfig, launch_mode: &LaunchMode, runs: &[FullAppRun]) {
    println!("Full Single-Image Benchmark");
    println!("mode: {}", launch_mode_label(launch_mode));
    println!("synthetic image: {}x{}", config.rows, config.cols);
    println!("measured runs: {}", runs.len());
    println!("warmup runs: {}", config.warmup);
    println!();

    println!("Runs");
    let run_table = render_table(
        &["Run", "Total", "Startup", "DICOM Load", "Render/UI"],
        &[
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
        ],
        &run_table_rows(runs),
    );
    print!("{run_table}");
    println!();

    println!("Summary (ms)");
    let summary_table = render_table(
        &["Metric", "Median", "Min", "Max"],
        &[
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
        ],
        &summary_table_rows(runs),
    );
    print!("{summary_table}");
}

fn run() -> Result<()> {
    let config = parse_config()?;
    let app_path = resolve_app_path(config.app_path.clone())?;
    let launch_mode = resolve_launch_mode()?;
    let temp_dir = TempBenchmarkDir::new("benchmark-full-single-open")?;
    let dicom_path = temp_dir.path().join("synthetic-full-single-open.dcm");
    write_synthetic_dicom(&dicom_path, config.rows, config.cols)?;
    let timeout = Duration::from_secs(config.timeout_secs);

    for _ in 0..config.warmup {
        let _ = run_once(&launch_mode, &app_path, &dicom_path, timeout)?;
    }

    let mut runs = Vec::with_capacity(config.runs);
    for _ in 0..config.runs {
        let run = run_once(&launch_mode, &app_path, &dicom_path, timeout)?;
        runs.push(run);
    }
    print_report(&config, &launch_mode, &runs);
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::fs;
    #[cfg(unix)]
    use std::time::Duration;

    #[test]
    fn compute_run_splits_full_app_time() {
        let run = compute_run(1000.0, 1120.0, 1410.0, 1435.0);
        assert_eq!(
            run,
            FullAppRun {
                total_ms: 435.0,
                startup_ms: 120.0,
                dicom_load_ms: 290.0,
                render_ui_ms: 25.0,
            }
        );
    }

    #[test]
    fn metric_stats_returns_median_min_and_max() {
        let stats = metric_stats(&[9.0, 3.0, 5.0, 7.0]).expect("stats should exist");
        assert_eq!(stats, (6.0, 3.0, 9.0));
    }

    #[test]
    fn render_table_aligns_headers_and_rows() {
        let table = render_table(
            &["Name", "Value"],
            &[ColumnAlign::Left, ColumnAlign::Right],
            &[vec!["total".to_string(), "12.300".to_string()]],
        );

        assert!(table.contains("| Name  |  Value |"));
        assert!(table.contains("| total | 12.300 |"));
    }

    #[test]
    fn parse_log_line_extracts_timestamp_and_event() {
        let line = "[1762361234.101] [INFO ] perf: single-open started";
        let parsed = parse_log_line(line);
        assert_eq!(parsed, Some((1762361234.101, START_EVENT)));
    }

    #[test]
    fn has_display_vars_detects_any_display_source() {
        assert!(!has_display_vars(None, None, None));
        assert!(has_display_vars(Some(OsString::from(":1")), None, None));
        assert!(has_display_vars(
            None,
            Some(OsString::from("wayland-0")),
            None
        ));
        assert!(has_display_vars(None, None, Some(OsString::from("socket"))));
    }

    #[test]
    fn resolve_linux_launch_mode_prefers_direct_with_display() {
        let mode = resolve_linux_launch_mode(true, Some(PathBuf::from("/usr/bin/xvfb-run")))
            .expect("display should use direct mode");

        assert_eq!(mode, LaunchMode::Direct);
    }

    #[test]
    fn resolve_linux_launch_mode_uses_xvfb_without_display() {
        let mode = resolve_linux_launch_mode(false, Some(PathBuf::from("/usr/bin/xvfb-run")))
            .expect("xvfb should be used when display is absent");

        assert_eq!(mode, LaunchMode::Xvfb(PathBuf::from("/usr/bin/xvfb-run")));
    }

    #[test]
    fn resolve_linux_launch_mode_errors_without_display_or_xvfb() {
        let err =
            resolve_linux_launch_mode(false, None).expect_err("missing display and xvfb must fail");

        assert!(
            format!("{err:#}").contains("xvfb-run"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn build_app_command_uses_xvfb_when_requested() {
        let app_path = Path::new("/tmp/perspecta");
        let dicom_path = Path::new("/tmp/test.dcm");
        let launch_mode = LaunchMode::Xvfb(PathBuf::from("/usr/bin/xvfb-run"));
        let command = build_app_command(&launch_mode, app_path, dicom_path);
        let program = command.get_program().to_string_lossy().into_owned();
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(program, "/usr/bin/xvfb-run");
        assert_eq!(
            args,
            vec!["-e", "/dev/stderr", "-a", "/tmp/perspecta", "/tmp/test.dcm"]
        );
    }

    #[test]
    fn resolve_launch_mode_prefers_direct_on_non_linux() {
        if cfg!(target_os = "linux") {
            return;
        }

        let mode = resolve_launch_mode().expect("non-Linux should use direct mode");
        assert_eq!(mode, LaunchMode::Direct);
    }

    #[cfg(unix)]
    #[test]
    fn kill_child_terminates_xvfb_process_group() {
        let temp_dir = TempBenchmarkDir::new("kill-child-group").expect("temp dir should exist");
        let pid_path = temp_dir.path().join("sleep.pid");
        let command_text = format!("sleep 1000 & echo $! > '{}'; wait", pid_path.display());

        let mut command = Command::new("sh");
        command.arg("-c").arg(command_text).process_group(0);
        let mut child = command.spawn().expect("shell should spawn");

        let deadline = Instant::now() + Duration::from_secs(2);
        let sleep_pid = loop {
            if let Ok(pid_text) = fs::read_to_string(&pid_path) {
                break pid_text
                    .trim()
                    .parse::<i32>()
                    .expect("background pid should parse");
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for child pid file"
            );
            thread::sleep(Duration::from_millis(10));
        };

        kill_child(
            &mut child,
            &LaunchMode::Xvfb(PathBuf::from("/usr/bin/xvfb-run")),
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        while unix_process_exists(sleep_pid) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !unix_process_exists(sleep_pid),
            "background process {sleep_pid} should be gone after group kill"
        );
    }

    #[cfg(unix)]
    fn unix_process_exists(pid: i32) -> bool {
        // Safety: `kill(pid, 0)` does not send a signal; it only asks the kernel whether the
        // process exists and is visible to this user.
        let result = unsafe { libc::kill(pid, 0) };
        if result == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }
}
