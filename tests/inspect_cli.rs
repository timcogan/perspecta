#![cfg(not(target_arch = "wasm32"))]

use std::path::Path;
use std::process::{Command, Output};

use dicom_core::{dicom_value, DataElement, PrimitiveValue, Tag, VR};
use dicom_object::{DefaultDicomObject, FileMetaTableBuilder, InMemDicomObject};
use dicom_pixeldata::PixelDecoder;
use serde_json::{json, Value};

const IMAGE_CLASS: &str = "1.2.840.10008.5.1.4.1.1.7";
const TRANSFER_SYNTAX: &str = "1.2.840.10008.1.2.1";

fn object(sop_class: &str, modality: &str) -> DefaultDicomObject {
    InMemDicomObject::from_element_iter([
        DataElement::new(Tag(0x0008, 0x0016), VR::UI, sop_class),
        DataElement::new(Tag(0x0008, 0x0018), VR::UI, "2.25.42"),
        DataElement::new(Tag(0x0008, 0x0060), VR::CS, modality),
        DataElement::new(Tag(0x0010, 0x0010), VR::PN, "SYNTHETIC^EXCLUDED"),
        DataElement::new(Tag(0x0010, 0x0020), VR::LO, "SYNTHETIC-EXCLUDED-ID"),
        DataElement::new(Tag(0x0008, 0x1030), VR::LO, "SYNTHETIC-EXCLUDED-TEXT"),
    ])
    .with_meta(
        FileMetaTableBuilder::new()
            .transfer_syntax(TRANSFER_SYNTAX)
            .media_storage_sop_class_uid(sop_class)
            .media_storage_sop_instance_uid("2.25.42"),
    )
    .expect("synthetic object must have file meta")
}

fn image(frames: &str) -> DefaultDicomObject {
    let mut object = object(IMAGE_CLASS, "OT");
    for element in [
        DataElement::new(Tag(0x0028, 0x0002), VR::US, dicom_value!(U16, [1])),
        DataElement::new(Tag(0x0028, 0x0004), VR::CS, "MONOCHROME2"),
        DataElement::new(Tag(0x0028, 0x0008), VR::IS, frames),
        DataElement::new(Tag(0x0028, 0x0010), VR::US, dicom_value!(U16, [2])),
        DataElement::new(Tag(0x0028, 0x0011), VR::US, dicom_value!(U16, [3])),
        DataElement::new(Tag(0x0028, 0x0100), VR::US, dicom_value!(U16, [8])),
        DataElement::new(Tag(0x0028, 0x0101), VR::US, dicom_value!(U16, [8])),
        DataElement::new(Tag(0x0028, 0x0102), VR::US, dicom_value!(U16, [7])),
        DataElement::new(Tag(0x0028, 0x0103), VR::US, dicom_value!(U16, [0])),
        DataElement::new(
            Tag(0x7FE0, 0x0010),
            VR::OB,
            PrimitiveValue::U8(vec![0; 18].into()),
        ),
    ] {
        object.put(element);
    }
    object
}

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_perspecta"));
    command
        .env_remove("DISPLAY")
        .env_remove("WAYLAND_DISPLAY")
        .env_remove("WAYLAND_SOCKET")
        .env("RUST_LOG", "trace");
    command
}

fn inspect(path: &Path) -> Output {
    command()
        .arg("inspect")
        .arg(path)
        .output()
        .expect("inspect executable must run")
}

fn inspect_with_limit(path: &Path, limit: &str) -> Output {
    command()
        .args(["inspect", "--max-file-mib", limit])
        .arg(path)
        .output()
        .expect("inspect executable must run")
}

fn summary(output: Output) -> Value {
    assert_eq!(
        output.status.code(),
        Some(0),
        "inspect failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "success must not emit diagnostics"
    );
    assert!(output.stdout.ends_with(b"\n"));
    serde_json::from_slice(&output.stdout).expect("stdout must contain one JSON object")
}

fn error(output: Output, exit_code: i32, code: &str) -> Value {
    assert_eq!(output.status.code(), Some(exit_code));
    assert!(output.stdout.is_empty(), "errors must leave stdout empty");
    let error: Value =
        serde_json::from_slice(&output.stderr).expect("stderr must contain one JSON error object");
    assert_eq!(error["schema_version"], 1);
    assert_eq!(error["error"]["code"], code);
    assert!(error["error"]["message"].is_string());
    error
}

#[test]
fn images_return_only_the_versioned_technical_summary_without_a_display() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic image.dcm");
    for frames in ["1", "3"] {
        image(frames)
            .write_to_file(&path)
            .expect("synthetic image must be writable");
        assert_eq!(
            summary(inspect(&path)),
            json!({
                "schema_version": 1,
                "kind": "image",
                "modality": "OT",
                "sop_class_uid": IMAGE_CLASS,
                "transfer_syntax_uid": TRANSFER_SYNTAX,
                "rows": 2,
                "columns": 3,
                "number_of_frames": frames.parse::<u32>().expect("test frame count is numeric")
            })
        );
    }
}

#[test]
fn classifies_reports_overlays_maps_and_other_objects() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic.dcm");
    for (sop, modality, kind) in [
        ("1.2.840.10008.5.1.4.1.1.88.11", "SR", "structured_report"),
        ("1.2.840.10008.5.1.4.1.1.11.1", "PR", "gsps"),
        ("1.2.840.10008.5.1.4.1.1.30", "OT", "parametric_map"),
        ("2.25.99", "OT", "other"),
    ] {
        let mut object = object(sop, modality);
        object.put(DataElement::new(
            Tag(0x0028, 0x0010),
            VR::US,
            dicom_value!(U16, [2]),
        ));
        object.put(DataElement::new(
            Tag(0x0028, 0x0011),
            VR::US,
            dicom_value!(U16, [3]),
        ));
        object
            .write_to_file(&path)
            .expect("synthetic object must be writable");
        let actual = summary(inspect(&path));
        assert_eq!(actual["kind"], kind);
        assert_eq!(actual["sop_class_uid"], sop);
        if kind == "parametric_map" {
            assert_eq!(actual["rows"], 2);
            assert_eq!(actual["columns"], 3);
        } else {
            assert!(actual["rows"].is_null());
            assert!(actual["columns"].is_null());
        }
        assert!(actual["number_of_frames"].is_null());
    }
}

#[test]
fn absent_fields_are_null_without_inferred_frame_counts() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic.dcm");
    let mut object = image("1");
    for tag in [
        Tag(0x0008, 0x0060),
        Tag(0x0008, 0x0016),
        Tag(0x0028, 0x0008),
        Tag(0x0028, 0x0010),
        Tag(0x0028, 0x0011),
    ] {
        object.remove_element(tag);
    }
    object
        .write_to_file(&path)
        .expect("synthetic object must be writable");
    let actual = summary(inspect(&path));
    assert_eq!(actual["kind"], "image");
    for field in [
        "modality",
        "sop_class_uid",
        "rows",
        "columns",
        "number_of_frames",
    ] {
        assert!(actual[field].is_null(), "{field} must be null");
    }
}

#[test]
fn malformed_numbers_fail_without_echoing_their_values() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic.dcm");
    for frames in [
        "",
        "0",
        "-1",
        "1.5",
        "1\\2",
        "4294967296",
        "SYNTHETIC-INVALID",
    ] {
        image(frames)
            .write_to_file(&path)
            .expect("synthetic object must be writable");
        let actual = error(inspect(&path), 1, "invalid_metadata");
        assert_eq!(
            actual["error"]["message"],
            "Invalid NumberOfFrames value in the DICOM dataset."
        );
    }
    for (tag, name) in [
        (Tag(0x0028, 0x0010), "Rows"),
        (Tag(0x0028, 0x0011), "Columns"),
    ] {
        for values in [vec![0], vec![2, 3]] {
            let mut object = image("1");
            object.put(DataElement::new(
                tag,
                VR::US,
                PrimitiveValue::U16(values.into()),
            ));
            object
                .write_to_file(&path)
                .expect("synthetic object must be writable");
            let actual = error(inspect(&path), 1, "invalid_metadata");
            assert_eq!(
                actual["error"]["message"],
                format!("Invalid {name} value in the DICOM dataset.")
            );
        }
    }
}

#[test]
fn inspect_does_not_require_decodable_pixels() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic.dcm");
    let mut object = image("1");
    object.remove_element(Tag(0x0028, 0x0100));
    object
        .write_to_file(&path)
        .expect("synthetic object must be writable");
    let reopened = dicom_object::open_file(&path).expect("metadata must remain readable");
    assert!(
        reopened.decode_pixel_data().is_err(),
        "fixture must fail pixel decoding"
    );
    assert_eq!(summary(inspect(&path))["kind"], "image");
}

#[test]
fn shared_reader_repairs_missing_meta_group_length_without_logs_or_file_changes() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic.dcm");
    let mut bytes = Vec::new();
    image("1")
        .write_all(&mut bytes)
        .expect("synthetic image must serialize");
    assert_eq!(&bytes[132..136], &[2, 0, 0, 0]);
    bytes.drain(132..144);
    std::fs::write(&path, &bytes).expect("synthetic image must be writable");
    assert_eq!(summary(inspect(&path))["kind"], "image");
    assert_eq!(
        std::fs::read(&path).expect("fixture must still exist"),
        bytes
    );
}

#[test]
fn read_errors_do_not_expose_the_input_path_or_file_contents() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("SYNTHETIC-EXCLUDED-FILENAME.dcm");
    for write_invalid_file in [false, true] {
        if write_invalid_file {
            std::fs::write(&path, b"SYNTHETIC-EXCLUDED-CONTENT")
                .expect("invalid fixture must be writable");
        }
        let actual = error(inspect(&path), 1, "read_error");
        assert!(!actual.to_string().contains("SYNTHETIC"));
        assert!(!actual
            .to_string()
            .contains(&directory.path().to_string_lossy().to_string()));
    }
    error(inspect(directory.path()), 1, "read_error");
}

#[test]
fn oversized_files_fail_before_parsing_without_exposing_the_input_path() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("SYNTHETIC-EXCLUDED-FILENAME.dcm");
    // Extend an empty file without a large buffer or a stored fixture.
    let file = std::fs::File::create(&path).expect("synthetic file must be writable");
    let size = 4096 * 1024 * 1024 + 1;
    file.set_len(size).expect("synthetic file size must be set");
    drop(file);

    assert_eq!(
        error(inspect(&path), 1, "file_too_large"),
        json!({
            "schema_version": 1,
            "error": {
                "code": "file_too_large",
                "message": "DICOM file size is 4294967297 bytes (4097 MiB rounded up). \
                            This exceeds the configured limit of 4096 MiB (4294967296 bytes). \
                            Retry with --max-file-mib 4097 or higher. \
                            Inspection loads the complete file into memory. Parsing and repairs can require additional memory."
            }
        })
    );
    assert_eq!(
        std::fs::metadata(&path)
            .expect("synthetic file must still exist")
            .len(),
        size
    );
}

#[test]
fn custom_file_limit_accepts_its_boundary_and_can_be_raised() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("SYNTHETIC-EXCLUDED-FILENAME.dcm");
    let mut object = image("1");
    let padding_tag = Tag(0xFFFC, 0xFFFC);
    object.put(DataElement::new(padding_tag, VR::OB, PrimitiveValue::Empty));
    let mut bytes = Vec::new();
    object
        .write_all(&mut bytes)
        .expect("synthetic image must serialize");
    let boundary = 1024 * 1024;
    let padding_length = boundary - bytes.len();

    for extra_bytes in [0, 2] {
        object.put(DataElement::new(
            padding_tag,
            VR::OB,
            PrimitiveValue::U8(vec![0; padding_length + extra_bytes].into()),
        ));
        object
            .write_to_file(&path)
            .expect("synthetic image must be writable");
        assert_eq!(
            std::fs::metadata(&path).expect("fixture must exist").len(),
            (boundary + extra_bytes) as u64
        );

        if extra_bytes == 0 {
            assert_eq!(summary(inspect_with_limit(&path, "1"))["kind"], "image");
        } else {
            let actual = error(inspect_with_limit(&path, "1"), 1, "file_too_large");
            let message = actual["error"]["message"]
                .as_str()
                .expect("error must have a message");
            assert!(message.contains("1048578 bytes (2 MiB rounded up)"));
            assert!(message.contains("limit of 1 MiB (1048576 bytes)"));
            assert!(message.contains("--max-file-mib 2 or higher"));
            assert!(!message.contains("SYNTHETIC"));
            assert!(!message.contains(&path.to_string_lossy().to_string()));
        }
        assert_eq!(summary(inspect_with_limit(&path, "2"))["kind"], "image");
    }
    assert_eq!(
        summary(inspect_with_limit(&path, "17592186044415"))["kind"],
        "image"
    );
}

#[test]
fn invalid_file_limits_fail_with_usage_without_echoing_the_value() {
    for value in [
        "",
        "0",
        "-1",
        "+1",
        "1.5",
        "1MiB",
        " 1",
        "1 ",
        "SYNTHETIC-EXCLUDED",
        "18446744073709551616",
        "17592186044416",
    ] {
        let actual = error(
            inspect_with_limit(Path::new("synthetic.dcm"), value),
            2,
            "invalid_arguments",
        );
        assert_eq!(actual["error"]["message"], "--max-file-mib requires an integer from 1 to 17592186044415 (MiB). Example: --max-file-mib 8192.");
    }
    let actual = error(
        command()
            .args(["inspect", "--max-file-mib"])
            .output()
            .expect("CLI must run"),
        2,
        "invalid_arguments",
    );
    assert!(actual["error"]["message"]
        .as_str()
        .expect("error must have a message")
        .contains("--max-file-mib"));
}

#[test]
fn invalid_arguments_and_help_do_not_start_the_viewer() {
    for args in [
        vec!["inspect"],
        vec!["inspect", ""],
        vec!["inspect", "one.dcm", "two.dcm"],
        vec!["inspect", "--unknown"],
        vec!["inspect", "--"],
        vec!["inspect", "--", ""],
        vec!["inspect", "--help", "extra"],
        vec!["inspect", "--max-file-mib", "1"],
        vec!["inspect", "--max-file-mib", "1", "one.dcm", "two.dcm"],
        vec![
            "inspect",
            "--max-file-mib",
            "1",
            "--max-file-mib",
            "2",
            "one.dcm",
        ],
    ] {
        error(
            command().args(args).output().expect("CLI must run"),
            2,
            "invalid_arguments",
        );
    }
    for args in [
        vec!["inspect", "--help"],
        vec!["--help"],
        vec!["inspect", "-h"],
        vec!["inspect", "--max-file-mib", "1", "--help"],
    ] {
        let output = command().args(args).output().expect("CLI help must run");
        assert_eq!(output.status.code(), Some(0));
        assert!(output.stderr.is_empty());
        assert!(String::from_utf8_lossy(&output.stdout).contains("perspecta inspect <file>"));
        assert!(String::from_utf8_lossy(&output.stdout).contains("4096 MiB (4 GiB)"));
        assert!(String::from_utf8_lossy(&output.stdout).contains("--max-file-mib"));
    }
}

#[test]
fn separator_allows_a_file_named_like_a_flag() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("--help");
    image("1")
        .write_to_file(&path)
        .expect("synthetic object must be writable");
    for args in [
        vec!["inspect", "--", "--help"],
        vec!["inspect", "--max-file-mib", "1", "--", "--help"],
    ] {
        let output = command()
            .current_dir(directory.path())
            .args(args)
            .output()
            .expect("CLI must run");
        assert_eq!(summary(output)["kind"], "image");
    }
}

#[cfg(unix)]
#[test]
fn native_paths_need_not_be_utf8() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory
        .path()
        .join(OsStr::from_bytes(b"synthetic-\xff.dcm"));
    image("1")
        .write_to_file(&path)
        .expect("synthetic object must be writable");
    assert_eq!(summary(inspect(&path))["kind"], "image");
    assert_eq!(summary(inspect_with_limit(&path, "1"))["kind"], "image");
    error(
        command()
            .args(["inspect", "--max-file-mib"])
            .arg(OsStr::from_bytes(b"\xff"))
            .arg(&path)
            .output()
            .expect("CLI must run"),
        2,
        "invalid_arguments",
    );
}
