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
    rendered_rgba(output, (2, 2))
}

fn rendered_rgba(output: Output, dimensions: (u32, u32)) -> Vec<[u8; 4]> {
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
    decode_png(&output.stdout, dimensions)
}

fn decode_png(png: &[u8], dimensions: (u32, u32)) -> Vec<[u8; 4]> {
    assert_eq!(&png[..8], b"\x89PNG\r\n\x1a\n");
    // Permit only required image chunks, so metadata cannot leak through ancillary chunks.
    let mut chunks = &png[8..];
    while !chunks.is_empty() {
        let length =
            u32::from_be_bytes(chunks[..4].try_into().expect("chunk length must exist")) as usize;
        assert!(matches!(&chunks[4..8], b"IHDR" | b"IDAT" | b"IEND"));
        chunks = &chunks[length + 12..];
    }
    let mut reader = png::Decoder::new(Cursor::new(png))
        .read_info()
        .expect("PNG must decode");
    let mut bytes = vec![
        0;
        reader
            .output_buffer_size()
            .expect("synthetic PNG buffer size must fit in memory")
    ];
    let info = reader
        .next_frame(&mut bytes)
        .expect("PNG must contain a frame");
    assert_eq!((info.width, info.height), dimensions);
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
fn saves_a_complete_png_without_overwriting_a_destination_or_source() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("SYNTHETIC source.dcm");
    let destination = directory.path().join("SYNTHETIC preview.png");
    mono()
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    let original = std::fs::read(&path).expect("source must be readable");
    let output = command()
        .current_dir(directory.path())
        .args([
            "--output-file",
            "SYNTHETIC preview.png",
            "--frame",
            "2",
            "--",
        ])
        .arg(&path)
        .output()
        .expect("CLI must run");
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
    let png = std::fs::read(&destination).expect("PNG must be saved");
    assert_eq!(decode_png(&png, (2, 2)), gray(&[255, 128, 64, 0]));
    assert_eq!(png, render(&path, &["--frame", "2"]).stdout);
    for existing in [&destination, &path] {
        error(
            command()
                .arg("--output-file")
                .arg(existing)
                .arg("--")
                .arg(&path)
                .output()
                .expect("CLI must run"),
            1,
            "output_exists",
        );
    }
    assert_eq!(std::fs::read(&path).expect("source must remain"), original);
    assert_eq!(std::fs::read(destination).expect("PNG must remain"), png);
    assert_eq!(
        rgba(render(&path, &["--output-file", "-"])),
        gray(&[0, 64, 128, 255])
    );
    assert_eq!(
        std::fs::read_dir(directory.path())
            .expect("directory must be readable")
            .count(),
        2,
        "temporary output files must not remain"
    );
}

#[test]
fn failed_export_leaves_no_output_file_and_sanitizes_destination_errors() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("SYNTHETIC source.dcm");
    let destination = directory.path().join("SYNTHETIC preview.png");
    let run = |destination: &Path| {
        command()
            .arg("--output-file")
            .arg(destination)
            .arg("--")
            .arg(&path)
            .output()
            .expect("CLI must run")
    };
    error(run(&destination), 1, "read_error");
    assert!(!destination.exists());
    mono()
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    error(
        run(&directory.path().join("SYNTHETIC missing/preview.png")),
        1,
        "output_error",
    );
    error(run(directory.path()), 1, "output_exists");
    assert_eq!(
        std::fs::read_dir(directory.path())
            .expect("directory must be readable")
            .count(),
        1
    );
}

#[cfg(unix)]
#[test]
fn refuses_a_dangling_output_symlink() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("SYNTHETIC source.dcm");
    let destination = directory.path().join("SYNTHETIC preview.png");
    let target = directory.path().join("SYNTHETIC missing.png");
    mono()
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    std::os::unix::fs::symlink(&target, &destination).expect("symlink must be created");
    error(
        command()
            .arg("--output-file")
            .arg(&destination)
            .arg(&path)
            .output()
            .expect("CLI must run"),
        1,
        "output_exists",
    );
    assert_eq!(
        std::fs::read_link(destination).expect("symlink must remain"),
        target
    );
    assert!(!target.exists());
}

#[test]
fn checks_the_pixel_limit_before_decoding_even_when_resizing() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("SYNTHETIC.dcm");
    mono()
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    assert_eq!(
        rgba(render(&path, &["--max-pixels", "4"])),
        gray(&[0, 64, 128, 255])
    );
    let truncated = image("MONOCHROME2", "1", dicom_value!(U8, [0, 1]));
    truncated
        .write_to_file(&path)
        .expect("truncated image must be writable");
    let response = error(
        render(&path, &["--max-pixels", "3", "--max-dimension", "1"]),
        1,
        "frame_too_large",
    );
    let message = response["error"]["message"]
        .as_str()
        .expect("message must be text");
    assert!(message.contains("4 pixels"));
    assert!(message.contains("limit of 3"));
    assert!(message.contains("--max-pixels 4 or higher"));
    assert!(message.contains("resizing happens after decoding"));
    error(render(&path, &["--max-pixels", "4"]), 1, "decode_error");
}

#[test]
fn resizes_with_aspect_ratio_and_never_upscales() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic.dcm");
    for (rows, columns) in [(2, 4), (4, 2)] {
        let mut object = image(
            "MONOCHROME2",
            "1",
            dicom_value!(U8, [64, 64, 64, 64, 64, 64, 64, 64]),
        );
        for (tag, value) in [(0x0010, rows), (0x0011, columns)] {
            object.put(DataElement::new(
                Tag(0x0028, tag),
                VR::US,
                dicom_value!(U16, [value]),
            ));
        }
        object
            .write_to_file(&path)
            .expect("synthetic image must be writable");
        for (limit, dimensions) in [
            ("8", (u32::from(columns), u32::from(rows))),
            ("2", (u32::from(columns / 2), u32::from(rows / 2))),
            ("1", (1, 1)),
        ] {
            let pixels = rendered_rgba(render(&path, &["--max-dimension", limit]), dimensions);
            assert_eq!(
                pixels,
                gray(&vec![64; (dimensions.0 * dimensions.1) as usize])
            );
        }
    }
}

#[test]
fn explicit_window_overrides_metadata_and_preserves_monochrome_inversion() {
    let directory = tempfile::tempdir().expect("temporary directory must exist");
    let path = directory.path().join("synthetic.dcm");
    for (photometric, expected) in [
        ("MONOCHROME2", gray(&[0, 128, 255, 255])),
        ("MONOCHROME1", gray(&[255, 127, 0, 0])),
    ] {
        image(photometric, "1", dicom_value!(U8, [0, 64, 128, 255]))
            .write_to_file(&path)
            .expect("synthetic image must be writable");
        assert_eq!(
            rgba(render(
                &path,
                &["--window-width", "128", "--window-center", "64"]
            )),
            expected
        );
    }
    mono()
        .write_to_file(&path)
        .expect("synthetic image must be writable");
    assert_eq!(
        rgba(render(
            &path,
            &["--window-center", "-32", "--window-width", "64"]
        )),
        gray(&[255, 255, 255, 255])
    );
    image(
        "RGB",
        "1",
        dicom_value!(U8, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    )
    .write_to_file(&path)
    .expect("synthetic image must be writable");
    error(
        render(&path, &["--window-center", "64", "--window-width", "128"]),
        2,
        "invalid_arguments",
    );
}

#[test]
fn rejects_invalid_export_controls_without_echoing_values() {
    for flag in ["--max-pixels", "--max-dimension"] {
        for value in [
            "0",
            "-1",
            "+1",
            "1.5",
            "18446744073709551616",
            "SYNTHETIC",
            "",
        ] {
            error(
                render(Path::new("SYNTHETIC.dcm"), &[flag, value]),
                2,
                "invalid_arguments",
            );
        }
    }
    for value in ["NaN", "inf", "-inf", "1e40", "SYNTHETIC", ""] {
        for pair in [
            ["--window-center", value, "--window-width", "128"],
            ["--window-width", value, "--window-center", "64"],
        ] {
            error(
                render(Path::new("SYNTHETIC.dcm"), &pair),
                2,
                "invalid_arguments",
            );
        }
    }
    for args in [
        vec!["--max-pixels"],
        vec!["--max-dimension"],
        vec!["--window-center"],
        vec!["--window-width"],
        vec!["--output-file"],
        vec!["--max-dimension", "4294967296", "SYNTHETIC.dcm"],
        vec!["--window-center", "64", "SYNTHETIC.dcm"],
        vec!["--window-width", "128", "SYNTHETIC.dcm"],
        vec![
            "--window-center",
            "64",
            "--window-width",
            "0.5",
            "SYNTHETIC.dcm",
        ],
        vec![
            "--window-center",
            "3e38",
            "--window-width",
            "3e38",
            "SYNTHETIC.dcm",
        ],
        vec!["--output-file", "", "SYNTHETIC.dcm"],
    ] {
        error(
            command().args(args).output().expect("CLI must run"),
            2,
            "invalid_arguments",
        );
    }
    for (flag, value) in [
        ("--max-pixels", "4"),
        ("--max-dimension", "2"),
        ("--window-center", "64"),
        ("--window-width", "128"),
        ("--output-file", "SYNTHETIC.png"),
    ] {
        error(
            render(Path::new("SYNTHETIC.dcm"), &[flag, value, flag, value]),
            2,
            "invalid_arguments",
        );
    }
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
    assert_eq!(
        rgba(render(
            &path,
            &["--window-center", "-50", "--window-width", "400"]
        )),
        gray(&[0, 128, 255, 255])
    );
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
    let destination = directory
        .path()
        .join(OsStr::from_bytes(b"synthetic-\xff.png"));
    let output = command()
        .arg("--output-file")
        .arg(&destination)
        .arg(&path)
        .output()
        .expect("CLI must run");
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
    assert_eq!(
        decode_png(
            &std::fs::read(destination).expect("PNG must be saved"),
            (2, 2)
        ),
        gray(&[0, 64, 128, 255])
    );
}
