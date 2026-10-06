use std::ffi::{OsStr, OsString};
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use serde::Serialize;

use crate::dicom::inspect::{inspect_file, InspectError, MIB_BYTES};

const DEFAULT_MAX_FILE_MIB: u64 = 4096;

const HELP: &str = "Perspecta DICOM Viewer

Usage:
  perspecta inspect <file>
  perspecta inspect [--max-file-mib <MiB>] [--] <file>
  perspecta inspect --help
  perspecta [--open] <file>...
  perspecta <perspecta:// URL>

Inspect one local DICOM file without a window or pixel decoding.
The default file size limit is 4096 MiB (4 GiB).
Use --max-file-mib before the file name to set a positive whole number of MiB.
The complete file is read into memory. This is not full DICOM validation.
Parsing and repairs can require memory beyond the file size.
Success writes schema_version 1 JSON to stdout. Errors write JSON to stderr.
Exit codes: 0 success, 1 file/data/output error, 2 invalid arguments.
Use -- before a file name that starts with a hyphen.
To open a file named inspect in the viewer, use --open inspect or ./inspect.
";

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Viewer,
    Inspect { path: PathBuf, limit_bytes: u64 },
    Help,
}

#[derive(Debug)]
enum CliError {
    Arguments,
    FileLimit,
    Inspect(InspectError),
    Output,
}

impl CliError {
    fn code(&self) -> &'static str {
        match self {
            Self::Arguments | Self::FileLimit => "invalid_arguments",
            Self::Inspect(InspectError::Read) => "read_error",
            Self::Inspect(InspectError::FileTooLarge { .. }) => "file_too_large",
            Self::Inspect(InspectError::InvalidMetadata(_)) => "invalid_metadata",
            Self::Output => "output_error",
        }
    }

    fn message(&self) -> String {
        match self {
            Self::Arguments => {
                "Expected one local file. Use 'perspecta inspect --help' for usage.".to_owned()
            }
            Self::FileLimit => format!(
                "--max-file-mib requires an integer from 1 to {} (MiB). Example: --max-file-mib 8192.",
                u64::MAX / MIB_BYTES
            ),
            Self::Inspect(error) => error.to_string(),
            Self::Output => {
                "Could not write command output. Check the output destination.".to_owned()
            }
        }
    }

    fn exit_code(&self) -> ExitCode {
        if matches!(self, Self::Arguments | Self::FileLimit) {
            ExitCode::from(2)
        } else {
            ExitCode::FAILURE
        }
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    schema_version: u8,
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: String,
}

fn parse_args(args: &[OsString]) -> Result<Command, CliError> {
    let Some(first) = args.first() else {
        return Ok(Command::Viewer);
    };
    if first == "--help" || first == "-h" {
        return if args.len() == 1 {
            Ok(Command::Help)
        } else {
            Err(CliError::Arguments)
        };
    }
    if first != "inspect" {
        return Ok(Command::Viewer);
    }
    let (args, limit_bytes) = match &args[1..] {
        [flag, value, rest @ ..] if flag == "--max-file-mib" => (rest, parse_file_limit(value)?),
        [flag] if flag == "--max-file-mib" => return Err(CliError::FileLimit),
        rest => (rest, DEFAULT_MAX_FILE_MIB * MIB_BYTES),
    };
    match args {
        [arg] if arg == "--help" || arg == "-h" => Ok(Command::Help),
        [separator, path] if separator == "--" && !path.is_empty() => Ok(Command::Inspect {
            path: path.into(),
            limit_bytes,
        }),
        [path] if !path.is_empty() && !path.as_encoded_bytes().starts_with(b"-") => {
            Ok(Command::Inspect {
                path: path.into(),
                limit_bytes,
            })
        }
        _ => Err(CliError::Arguments),
    }
}

fn parse_file_limit(value: &OsStr) -> Result<u64, CliError> {
    value
        .to_str()
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .and_then(|value| value.checked_mul(MIB_BYTES))
        .ok_or(CliError::FileLimit)
}

/// Return None when the existing viewer launch path should handle the arguments.
pub(crate) fn run(
    args: &[OsString],
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> Option<ExitCode> {
    let result = match parse_args(args) {
        Ok(Command::Viewer) => return None,
        Ok(Command::Help) => stdout
            .write_all(HELP.as_bytes())
            .and_then(|()| stdout.flush())
            .map_err(|_| CliError::Output),
        Ok(Command::Inspect { path, limit_bytes }) => inspect_file(&path, limit_bytes)
            .map_err(CliError::Inspect)
            .and_then(|inspection| write_json(stdout, &inspection).map_err(|_| CliError::Output)),
        Err(error) => Err(error),
    };
    Some(match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let response = ErrorResponse {
                schema_version: 1,
                error: ErrorDetail {
                    code: error.code(),
                    message: error.message(),
                },
            };
            // A failed error stream cannot carry another diagnostic.
            let _ = write_json(stderr, &response);
            error.exit_code()
        }
    })
}

fn write_json(output: &mut impl Write, value: &impl Serialize) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    bytes.push(b'\n');
    output.write_all(&bytes)?;
    output.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn existing_launches_bypass_cli_dispatch() {
        for args in [
            vec![],
            vec!["example.dcm"],
            vec!["one.dcm", "two.dcm"],
            vec!["--open", "inspect"],
            vec!["./inspect"],
            vec!["perspecta://open?path=example.dcm"],
            vec!["perspecta://open?group=one.dcm|two.dcm&open_group=0"],
            vec!["perspecta://open?dicomweb=https%3A%2F%2Fexample.invalid&study=1.2.3"],
        ] {
            let args: Vec<_> = args.into_iter().map(OsString::from).collect();
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            assert_eq!(run(&args, &mut stdout, &mut stderr), None);
            assert!(stdout.is_empty());
            assert!(stderr.is_empty());
        }
    }

    struct BrokenOutput;

    impl Write for BrokenOutput {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }

    #[test]
    fn failed_output_returns_an_error_instead_of_success() {
        let mut stderr = Vec::new();
        let code = run(&["--help".into()], &mut BrokenOutput, &mut stderr);
        assert_eq!(code, Some(ExitCode::FAILURE));
        let error: serde_json::Value =
            serde_json::from_slice(&stderr).expect("error output must be JSON");
        assert_eq!(error["error"]["code"], "output_error");
    }
}
