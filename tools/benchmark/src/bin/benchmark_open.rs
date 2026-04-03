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
const OPEN_STARTED_EVENT: &str = "open started";
const OPEN_DICOM_LOADED_EVENT: &str = "open dicom-loaded";
const OPEN_COMPLETED_EVENT: &str = "open completed";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BenchmarkMode {
    Single,
    EightUp,
}

impl BenchmarkMode {
    fn from_images(images: usize) -> Result<Self> {
        match images {
            1 => Ok(Self::Single),
            8 => Ok(Self::EightUp),
            other => bail!("--images must be 1 or 8 (got {other})"),
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Single => "Full Open Benchmark (Single)",
            Self::EightUp => "Full Open Benchmark (8-Up)",
        }
    }

    fn scenario_label(self) -> &'static str {
        match self {
            Self::Single => "single-image",
            Self::EightUp => "8-up",
        }
    }

    fn start_event(self) -> &'static str {
        OPEN_STARTED_EVENT
    }

    fn dicom_loaded_event(self) -> &'static str {
        OPEN_DICOM_LOADED_EVENT
    }

    fn completed_event(self) -> &'static str {
        OPEN_COMPLETED_EVENT
    }

    fn expected_loaded_events(self) -> usize {
        match self {
            Self::Single => 1,
            Self::EightUp => 8,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BenchmarkConfig {
    app_path: Option<PathBuf>,
    runs: usize,
    warmup: usize,
    rows: usize,
    cols: usize,
    timeout_secs: u64,
    images: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            app_path: None,
            runs: 5,
            warmup: 1,
            rows: 1024,
            cols: 1024,
            timeout_secs: 30,
            images: 1,
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

#[derive(Default)]
struct RunProgress {
    started_ms: Option<f64>,
    loaded_events: usize,
    dicom_done_ms: Option<f64>,
    completed_ms: Option<f64>,
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

struct MammoSyntheticSpec {
    file_name: &'static str,
    study_date: &'static str,
    laterality: &'static str,
    view_position: &'static str,
    instance_number: i32,
}

const EIGHT_UP_SPECS: [MammoSyntheticSpec; 8] = [
    MammoSyntheticSpec {
        file_name: "current-rcc.dcm",
        study_date: "20260101",
        laterality: "R",
        view_position: "CC",
        instance_number: 1,
    },
    MammoSyntheticSpec {
        file_name: "current-lcc.dcm",
        study_date: "20260101",
        laterality: "L",
        view_position: "CC",
        instance_number: 2,
    },
    MammoSyntheticSpec {
        file_name: "current-rmlo.dcm",
        study_date: "20260101",
        laterality: "R",
        view_position: "MLO",
        instance_number: 3,
    },
    MammoSyntheticSpec {
        file_name: "current-lmlo.dcm",
        study_date: "20260101",
        laterality: "L",
        view_position: "MLO",
        instance_number: 4,
    },
    MammoSyntheticSpec {
        file_name: "prior-rcc.dcm",
        study_date: "20240101",
        laterality: "R",
        view_position: "CC",
        instance_number: 5,
    },
    MammoSyntheticSpec {
        file_name: "prior-lcc.dcm",
        study_date: "20240101",
        laterality: "L",
        view_position: "CC",
        instance_number: 6,
    },
    MammoSyntheticSpec {
        file_name: "prior-rmlo.dcm",
        study_date: "20240101",
        laterality: "R",
        view_position: "MLO",
        instance_number: 7,
    },
    MammoSyntheticSpec {
        file_name: "prior-lmlo.dcm",
        study_date: "20240101",
        laterality: "L",
        view_position: "MLO",
        instance_number: 8,
    },
];

fn print_usage() {
    eprintln!(
        "Usage: cargo run -p benchmark-tools --bin benchmark_open -- [--app PATH] [--runs N] [--warmup N] [--rows N] [--cols N] [--timeout-secs N] [--images 1|8]"
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
            "--images" => config.images = parse_usize_arg("--images", args.next())?,
            other => bail!("unknown argument: {other}"),
        }
    }

    BenchmarkMode::from_images(config.images)?;
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

fn build_app_command(
    launch_mode: &LaunchMode,
    app_path: &Path,
    dicom_paths: &[PathBuf],
) -> Command {
    match launch_mode {
        LaunchMode::Direct => {
            let mut command = Command::new(app_path);
            command.args(dicom_paths);
            command
        }
        LaunchMode::Xvfb(xvfb_path) => {
            let mut command = Command::new(xvfb_path);
            command.arg("-e").arg("/dev/stderr").arg("-a").arg(app_path);
            command.args(dicom_paths);
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

fn observe_perf_event(
    progress: &mut RunProgress,
    mode: BenchmarkMode,
    timestamp: f64,
    event: &str,
) {
    let timestamp_ms = timestamp * 1000.0;

    if event == mode.start_event() {
        progress.started_ms = Some(timestamp_ms);
        return;
    }

    if event == mode.dicom_loaded_event() {
        progress.loaded_events = progress.loaded_events.saturating_add(1);
        if progress.loaded_events == mode.expected_loaded_events()
            && progress.dicom_done_ms.is_none()
        {
            progress.dicom_done_ms = Some(timestamp_ms);
        }
        return;
    }

    if event == mode.completed_event() {
        progress.completed_ms = Some(timestamp_ms);
    }
}

fn run_once(
    mode: BenchmarkMode,
    launch_mode: &LaunchMode,
    app_path: &Path,
    dicom_paths: &[PathBuf],
    timeout: Duration,
) -> Result<FullAppRun> {
    let spawn_ms = current_epoch_ms();
    let mut command = build_app_command(launch_mode, app_path, dicom_paths);
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
    let mut progress = RunProgress::default();

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
                    observe_perf_event(&mut progress, mode, timestamp, event);
                    if progress.completed_ms.is_some() {
                        kill_child(&mut child, launch_mode);
                        break;
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
                                    observe_perf_event(&mut progress, mode, timestamp, event);
                                }
                            }
                            Err(err) => bail!("{err}"),
                        }
                    }

                    if progress.completed_ms.is_some() {
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
                    if progress.completed_ms.is_some() {
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

    let started_ms = progress
        .started_ms
        .with_context(|| format!("app never emitted '{}'", mode.start_event()))?;
    let dicom_done_ms = progress.dicom_done_ms.with_context(|| {
        format!(
            "app never emitted {} occurrence(s) of '{}'",
            mode.expected_loaded_events(),
            mode.dicom_loaded_event()
        )
    })?;
    let completed_ms = progress
        .completed_ms
        .with_context(|| format!("app never emitted '{}'", mode.completed_event()))?;

    Ok(compute_run(
        spawn_ms,
        started_ms,
        dicom_done_ms,
        completed_ms,
    ))
}

fn metric_stats(values: &[f64]) -> Option<(f64, f64, f64)> {
    values.first()?;
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let median = if sorted.len() % 2 == 1 {
        sorted[sorted.len() / 2]
    } else {
        let upper = sorted.len() / 2;
        (sorted[upper - 1] + sorted[upper]) / 2.0
    };
    let min = *sorted.first().unwrap();
    let max = *sorted.last().unwrap();
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

fn print_report(
    config: &BenchmarkConfig,
    mode: BenchmarkMode,
    launch_mode: &LaunchMode,
    runs: &[FullAppRun],
) {
    println!("{}", mode.title());
    println!("scenario: {}", mode.scenario_label());
    println!("mode: {}", launch_mode_label(launch_mode));
    println!("synthetic viewport image: {}x{}", config.rows, config.cols);
    println!("synthetic image count: {}", config.images);
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

fn write_synthetic_inputs(
    temp_dir: &TempBenchmarkDir,
    config: &BenchmarkConfig,
    mode: BenchmarkMode,
) -> Result<Vec<PathBuf>> {
    match mode {
        BenchmarkMode::Single => {
            let path = temp_dir.path().join("synthetic-full-open.dcm");
            write_synthetic_dicom(&path, config.rows, config.cols, "20260101", "R", "CC", 1)?;
            Ok(vec![path])
        }
        BenchmarkMode::EightUp => {
            let mut paths = Vec::with_capacity(EIGHT_UP_SPECS.len());
            for spec in EIGHT_UP_SPECS {
                let path = temp_dir.path().join(spec.file_name);
                write_synthetic_dicom(
                    &path,
                    config.rows,
                    config.cols,
                    spec.study_date,
                    spec.laterality,
                    spec.view_position,
                    spec.instance_number,
                )?;
                paths.push(path);
            }
            Ok(paths)
        }
    }
}

fn run() -> Result<()> {
    let config = parse_config()?;
    let mode = BenchmarkMode::from_images(config.images)?;
    let app_path = resolve_app_path(config.app_path.clone())?;
    let launch_mode = resolve_launch_mode()?;
    let temp_dir = TempBenchmarkDir::new("benchmark-full-open")?;
    let dicom_paths = write_synthetic_inputs(&temp_dir, &config, mode)?;
    let timeout = Duration::from_secs(config.timeout_secs);

    for _ in 0..config.warmup {
        let _ = run_once(mode, &launch_mode, &app_path, &dicom_paths, timeout)?;
    }

    let mut runs = Vec::with_capacity(config.runs);
    for _ in 0..config.runs {
        let run = run_once(mode, &launch_mode, &app_path, &dicom_paths, timeout)?;
        runs.push(run);
    }
    print_report(&config, mode, &launch_mode, &runs);
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
    fn benchmark_mode_titles_are_distinct() {
        assert_eq!(
            BenchmarkMode::Single.title(),
            "Full Open Benchmark (Single)"
        );
        assert_eq!(BenchmarkMode::EightUp.title(), "Full Open Benchmark (8-Up)");
    }

    #[test]
    fn observe_perf_event_marks_dicom_done_after_expected_image_count() {
        let mut progress = RunProgress::default();

        observe_perf_event(
            &mut progress,
            BenchmarkMode::EightUp,
            1.0,
            OPEN_STARTED_EVENT,
        );
        for timestamp in 2..=8 {
            observe_perf_event(
                &mut progress,
                BenchmarkMode::EightUp,
                timestamp as f64,
                OPEN_DICOM_LOADED_EVENT,
            );
        }
        assert_eq!(progress.started_ms, Some(1000.0));
        assert_eq!(progress.loaded_events, 7);
        assert_eq!(progress.dicom_done_ms, None);

        observe_perf_event(
            &mut progress,
            BenchmarkMode::EightUp,
            9.0,
            OPEN_DICOM_LOADED_EVENT,
        );

        assert_eq!(progress.loaded_events, 8);
        assert_eq!(progress.dicom_done_ms, Some(9000.0));
    }

    #[test]
    fn parse_log_line_extracts_timestamp_and_event() {
        let line = "[1762361234.101] [INFO ] perf: open started";
        let parsed = parse_log_line(line);
        assert_eq!(parsed, Some((1762361234.101, OPEN_STARTED_EVENT)));
    }

    #[test]
    fn benchmark_mode_rejects_unsupported_image_counts() {
        let err = BenchmarkMode::from_images(4).expect_err("unsupported image count should fail");
        assert!(format!("{err:#}").contains("1 or 8"));
    }

    #[test]
    fn build_app_command_passes_all_paths() {
        let app_path = Path::new("/tmp/perspecta");
        let dicom_paths = vec![
            PathBuf::from("/tmp/a.dcm"),
            PathBuf::from("/tmp/b.dcm"),
            PathBuf::from("/tmp/c.dcm"),
        ];
        let launch_mode = LaunchMode::Direct;
        let command = build_app_command(&launch_mode, app_path, &dicom_paths);
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(args, vec!["/tmp/a.dcm", "/tmp/b.dcm", "/tmp/c.dcm"]);
    }
}
