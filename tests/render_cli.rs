#![cfg(not(target_arch = "wasm32"))]

use std::io::Cursor;
use std::path::Path;
use std::process::{Command, Output};

use dicom_core::{dicom_value, DataElement, PrimitiveValue, Tag, VR};
use dicom_object::{DefaultDicomObject, FileMetaTableBuilder, InMemDicomObject};
use serde_json::Value;

fn image(photometric: &str, frames: &str, pixels: PrimitiveValue) -> DefaultDicomObject {
    let samples = if photometric == "RGB" { 3 } else { 1 };
    InMemDicomObject::from_element_iter([
        DataElement::new(Tag(0x0008, 0x0016), VR::UI, "1.2.840.10008.5.1.4.1.1.7"),
        DataElement::new(Tag(0x0008, 0x0018), VR::UI, "2.25.42"),
        DataElement::new(Tag(0x0010, 0x0010), VR::PN, "SYNTHETIC^EXCLUDED"),
        DataElement::new(Tag(0x0010, 0x0020), VR::LO, "SYNTHETIC-EXCLUDED-ID"),
        DataElement::new(Tag(0x0028, 0x0002), VR::US, dicom_value!(U16, [samples])),
        DataElement::new(Tag(0x0028, 0x0004), VR::CS, photometric),
        DataElement::new(Tag(0x0028, 0x0006), VR::US, dicom_value!(U16, [0])),
        DataElement::new(Tag(0x0028, 0x0008), VR::IS, frames),
        DataElement::new(Tag(0x0028, 0x0010), VR::US, dicom_value!(U16, [2])),
        DataElement::new(Tag(0x0028, 0x0011), VR::US, dicom_value!(U16, [2])),
        DataElement::new(Tag(0x0028, 0x0100), VR::US, dicom_value!(U16, [8])),
        DataElement::new(Tag(0x0028, 0x0101), VR::US, dicom_value!(U16, [8])),
        DataElement::new(Tag(0x0028, 0x0102), VR::US, dicom_value!(U16, [7])),
        DataElement::new(Tag(0x0028, 0x0103), VR::US, dicom_value!(U16, [0])),
        DataElement::new(Tag(0x0028, 0x1050), VR::DS, "127.5"),
        DataElement::new(Tag(0x0028, 0x1051), VR::DS, "255"),
        DataElement::new(Tag(0x7FE0, 0x0010), VR::OB, pixels),
    ])
    .with_meta(
        FileMetaTableBuilder::new()
            .transfer_syntax("1.2.840.10008.1.2.1")
            .media_storage_sop_class_uid("1.2.840.10008.5.1.4.1.1.7")
            .media_storage_sop_instance_uid("2.25.42"),
    )
    .expect("synthetic object must have file meta")
}

fn mono() -> DefaultDicomObject {
    image(
        "MONOCHROME2",
        "2",
        dicom_value!(U8, [0, 64, 128, 255, 255, 128, 64, 0]),
    )
}

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_perspecta"));
    command
        .arg("render")
        .env_remove("DISPLAY")
        .env_remove("WAYLAND_DISPLAY")
        .env_remove("WAYLAND_SOCKET")
        .env("RUST_LOG", "trace");
    command
}

fn render(path: &Path, options: &[&str]) -> Output {
    command()
        .args(options)
        .arg("--")
        .arg(path)
        .output()
        .expect("CLI must run")
}

fn rgba(output: Output) -> Vec<[u8; 4]> {
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "success must not emit diagnostics"
    );
    assert_eq!(&output.stdout[..8], b"\x89PNG\r\n\x1a\n");
    // Permit only required image chunks, so metadata cannot leak through ancillary chunks.
    let mut chunks = &output.stdout[8..];
    while !chunks.is_empty() {
        let length =
            u32::from_be_bytes(chunks[..4].try_into().expect("chunk length must exist")) as usize;
        assert!(matches!(&chunks[4..8], b"IHDR" | b"IDAT" | b"IEND"));
        chunks = &chunks[length + 12..];
    }
    let mut reader = png::Decoder::new(Cursor::new(output.stdout))
        .read_info()
        .expect("PNG must decode");
    let mut bytes = vec![0; reader.output_buffer_size()];
    let info = reader
        .next_frame(&mut bytes)
        .expect("PNG must contain a frame");
    assert_eq!((info.width, info.height), (2, 2));
    assert_eq!(info.color_type, png::ColorType::Rgba);
    assert_eq!(info.bit_depth, png::BitDepth::Eight);
    bytes[..info.buffer_size()].as_chunks::<4>().0.to_vec()
}

fn gray(values: &[u8]) -> Vec<[u8; 4]> {
    values
        .iter()
        .map(|&value| [value, value, value, 255])
        .collect()
}

fn error(output: Output, exit_code: i32, code: &str) -> Value {
    assert_eq!(output.status.code(), Some(exit_code));
    assert!(output.stdout.is_empty());
    let text = String::from_utf8_lossy(&output.stderr);
    assert!(!text.contains("SYNTHETIC"));
    let error: Value =
        serde_json::from_slice(&output.stderr).expect("stderr must contain one JSON object");
    assert_eq!(error["schema_version"], 1);
    assert_eq!(error["error"]["code"], code);
    error
}

#[test]
fn exports_the_requested_stored_frame_without_metadata_logs_or_source_changes() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("SYNTHETIC image.dcm");
    mono()
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    let original = std::fs::read(&path).expect("source must be readable");
    assert_eq!(rgba(render(&path, &[])), gray(&[0, 64, 128, 255]));
    assert_eq!(
        rgba(render(
            &path,
            &["--output", "png", "--frame", "2", "--max-file-mib", "1"]
        )),
        gray(&[255, 128, 64, 0])
    );
    assert_eq!(
        rgba(render(
            &path,
            &["--max-file-mib", "1", "--frame", "1", "--output", "png"]
        )),
        gray(&[0, 64, 128, 255])
    );
    assert_eq!(
        std::fs::read(path).expect("source must remain readable"),
        original
    );
}

#[test]
fn does_not_decode_an_unrequested_truncated_frame() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic.dcm");
    let object = image("MONOCHROME2", "2", dicom_value!(U8, [0, 64, 128, 255]));
    object
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    assert_eq!(rgba(render(&path, &[])), gray(&[0, 64, 128, 255]));
    error(render(&path, &["--frame", "2"]), 1, "decode_error");
}

#[test]
fn applies_monochrome_inversion_and_defaults_for_absent_frame_count_and_window() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic.dcm");
    let mut object = image("MONOCHROME1", "1", dicom_value!(U8, [10, 20, 30, 40]));
    for tag in [
        Tag(0x0028, 0x0008),
        Tag(0x0028, 0x1050),
        Tag(0x0028, 0x1051),
    ] {
        object.remove_element(tag);
    }
    object
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    assert_eq!(rgba(render(&path, &[])), gray(&[255, 170, 85, 0]));
    error(render(&path, &["--frame", "2"]), 1, "frame_out_of_range");
}

#[test]
fn applies_signed_sixteen_bit_rescale_and_window() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic.dcm");
    let mut object = mono();
    for (tag, value) in [(0x0100, 16), (0x0101, 16), (0x0102, 15), (0x0103, 1)] {
        object.put(DataElement::new(
            Tag(0x0028, tag),
            VR::US,
            dicom_value!(U16, [value]),
        ));
    }
    for (tag, value) in [
        (0x0008, "1"),
        (0x1050, "50"),
        (0x1051, "400"),
        (0x1052, "-50"),
        (0x1053, "2"),
    ] {
        object.put(DataElement::new(
            Tag(0x0028, tag),
            if tag == 0x0008 { VR::IS } else { VR::DS },
            value,
        ));
    }
    object.put(DataElement::new(
        Tag(0x7FE0, 0x0010),
        VR::OW,
        dicom_value!(I16, [-100, 0, 100, 200]),
    ));
    object
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    assert_eq!(rgba(render(&path, &[])), gray(&[0, 64, 191, 255]));
}

#[test]
fn exports_rgb_at_eight_and_sixteen_bits() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic.dcm");
    let expected = vec![
        [255, 0, 0, 255],
        [0, 255, 0, 255],
        [0, 0, 255, 255],
        [255, 255, 255, 255],
    ];
    let pixels = vec![255, 0, 0, 0, 255, 0, 0, 0, 255, 255, 255, 255];
    let mut object = image("RGB", "1", PrimitiveValue::U8(pixels.into()));
    object
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    assert_eq!(rgba(render(&path, &[])), expected);
    for (tag, value) in [(0x0100, 16), (0x0101, 12), (0x0102, 11)] {
        object.put(DataElement::new(
            Tag(0x0028, tag),
            VR::US,
            dicom_value!(U16, [value]),
        ));
    }
    object.put(DataElement::new(
        Tag(0x7FE0, 0x0010),
        VR::OW,
        dicom_value!(U16, [4095, 0, 0, 0, 4095, 0, 0, 0, 4095, 4095, 4095, 4095]),
    ));
    object
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    assert_eq!(rgba(render(&path, &[])), expected);
}

#[cfg(feature = "jpeg_ls")]
#[test]
fn exports_a_compressed_frame_with_the_existing_codec() {
    use dicom_pixeldata::Transcode;
    use dicom_transfer_syntax_registry::entries::JPEG_LS_LOSSLESS_IMAGE_COMPRESSION;
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic.dcm");
    let mut object = mono();
    object
        .transcode(&JPEG_LS_LOSSLESS_IMAGE_COMPRESSION.erased())
        .expect("synthetic image must transcode");
    object
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    assert_eq!(
        rgba(render(&path, &["--frame", "2"])),
        gray(&[255, 128, 64, 0])
    );
}

#[test]
fn rejects_invalid_arguments_and_prints_help_without_a_display() {
    for args in [
        vec![],
        vec![""],
        vec!["one.dcm", "two.dcm"],
        vec!["--unknown"],
        vec!["--output"],
        vec!["--output", "json", "SYNTHETIC.dcm"],
        vec!["--output", "png", "--output", "png", "SYNTHETIC.dcm"],
        vec!["--frame"],
        vec!["--frame", "0", "SYNTHETIC.dcm"],
        vec!["--frame", "-1", "SYNTHETIC.dcm"],
        vec!["--frame", "1.5", "SYNTHETIC.dcm"],
        vec!["--frame", "4294967296", "SYNTHETIC.dcm"],
        vec!["--frame", "SYNTHETIC", "SYNTHETIC.dcm"],
        vec!["--frame", "1", "--frame", "2", "SYNTHETIC.dcm"],
        vec!["--max-file-mib"],
        vec!["--max-file-mib", "0", "SYNTHETIC.dcm"],
        vec![
            "--max-file-mib",
            "1",
            "--max-file-mib",
            "2",
            "SYNTHETIC.dcm",
        ],
        vec!["--help", "SYNTHETIC.dcm"],
        vec!["--", ""],
    ] {
        error(
            command().args(args).output().expect("CLI must run"),
            2,
            "invalid_arguments",
        );
    }
    for flag in ["--help", "-h"] {
        let output = command().arg(flag).output().expect("CLI help must run");
        assert_eq!(output.status.code(), Some(0));
        assert!(output.stderr.is_empty());
        assert!(String::from_utf8_lossy(&output.stdout).contains("perspecta render"));
    }
}

#[test]
fn errors_are_structured_and_exclude_input_paths_and_metadata() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("SYNTHETIC-EXCLUDED.dcm");
    error(render(&path, &[]), 1, "read_error");
    error(render(directory.path(), &[]), 1, "read_error");
    std::fs::write(&path, b"SYNTHETIC-EXCLUDED").expect("invalid file must be writable");
    error(render(&path, &[]), 1, "read_error");
    mono()
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    let actual = error(render(&path, &["--frame", "3"]), 1, "frame_out_of_range");
    assert!(actual["error"]["message"]
        .as_str()
        .expect("message must be text")
        .contains("--frame from 1 to 2"));
    let mut invalid = mono();
    invalid.put(DataElement::new(
        Tag(0x0028, 0x0008),
        VR::IS,
        "SYNTHETIC-EXCLUDED",
    ));
    invalid
        .write_to_file(&path)
        .expect("invalid image must be writable");
    error(render(&path, &[]), 1, "invalid_metadata");
    let mut truncated = mono();
    truncated.put(DataElement::new(
        Tag(0x7FE0, 0x0010),
        VR::OB,
        dicom_value!(U8, [0, 1]),
    ));
    truncated
        .write_to_file(&path)
        .expect("truncated image must be writable");
    error(render(&path, &[]), 1, "decode_error");
    for bits in [0, 17] {
        let mut invalid = mono();
        invalid.put(DataElement::new(
            Tag(0x0028, 0x0101),
            VR::US,
            dicom_value!(U16, [bits]),
        ));
        invalid
            .write_to_file(&path)
            .expect("invalid image must be writable");
        error(render(&path, &[]), 1, "invalid_metadata");
    }
    let mut unsupported = mono();
    unsupported.put(DataElement::new(
        Tag(0x0028, 0x0004),
        VR::CS,
        "PALETTE COLOR",
    ));
    unsupported
        .write_to_file(&path)
        .expect("unsupported image must be writable");
    error(render(&path, &[]), 1, "unsupported_image");
    for class in [
        "1.2.840.10008.5.1.4.1.1.88.11",
        "1.2.840.10008.5.1.4.1.1.11.1",
        "1.2.840.10008.5.1.4.1.1.30",
    ] {
        let mut unsupported = mono();
        unsupported.put(DataElement::new(Tag(0x0008, 0x0016), VR::UI, class));
        unsupported
            .write_to_file(&path)
            .expect("unsupported object must be writable");
        error(render(&path, &[]), 1, "unsupported_image");
    }
    let file = std::fs::File::create(&path).expect("oversized file must be writable");
    file.set_len(1024 * 1024 + 2)
        .expect("small custom limit must be exceeded");
    let actual = error(render(&path, &["--max-file-mib", "1"]), 1, "file_too_large");
    assert!(actual["error"]["message"]
        .as_str()
        .expect("message must be text")
        .contains("--max-file-mib 2 or higher"));
}

#[test]
fn separator_accepts_a_filename_that_looks_like_an_option() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    mono()
        .write_to_file(directory.path().join("--help"))
        .expect("synthetic image must be writable");
    let output = command()
        .current_dir(directory.path())
        .args(["--", "--help"])
        .output()
        .expect("CLI must run");
    assert_eq!(rgba(output), gray(&[0, 64, 128, 255]));
}

#[cfg(unix)]
#[test]
fn failed_png_output_returns_a_structured_error() {
    use std::os::fd::OwnedFd;
    use std::os::unix::net::UnixStream;
    use std::process::Stdio;
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic.dcm");
    mono()
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    let (reader, writer) = UnixStream::pair().expect("output stream must exist");
    drop(reader);
    let output = command()
        .arg(path)
        .stdout(Stdio::from(OwnedFd::from(writer)))
        .output()
        .expect("CLI must run");
    error(output, 1, "output_error");
}

#[cfg(unix)]
#[test]
fn accepts_native_paths_without_utf8() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory
        .path()
        .join(OsStr::from_bytes(b"synthetic-\xff.dcm"));
    mono()
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    assert_eq!(rgba(render(&path, &[])), gray(&[0, 64, 128, 255]));
}
