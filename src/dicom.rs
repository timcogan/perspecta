use std::path::Path;
use std::{fs, io::Cursor};

use anyhow::{bail, Context, Result};
use dicom_object::{from_reader, open_file, DefaultDicomObject, ReadError, Tag};
use dicom_pixeldata::PixelDecoder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageColorMode {
    Monochrome,
    Rgb,
}

pub const METADATA_FIELD_NAMES: &[&str] = &[
    "PatientName",
    "PatientID",
    "PatientSex",
    "PatientBirthDate",
    "StudyDate",
    "StudyDescription",
    "SeriesDescription",
    "Modality",
    "Manufacturer",
    "InstitutionName",
    "BodyPartExamined",
    "SliceThickness",
    "KVP",
    "Rows",
    "Columns",
    "SamplesPerPixel",
    "PlanarConfiguration",
    "NumberOfFrames",
    "FrameTime",
    "BitsAllocated",
    "BitsStored",
    "PixelRepresentation",
    "PhotometricInterpretation",
    "ViewPosition",
    "ImageLaterality",
    "Laterality",
    "FrameLaterality",
    "InstanceNumber",
];

#[derive(Debug, Clone)]
pub struct DicomImage {
    pub width: usize,
    pub height: usize,
    mono_frames: Vec<Vec<i32>>,
    rgb_frames: Vec<Vec<u8>>,
    pub color_mode: ImageColorMode,
    pub samples_per_pixel: u16,
    pub invert: bool,
    pub window_center: f32,
    pub window_width: f32,
    pub min_value: i32,
    pub max_value: i32,
    pub recommended_cine_fps: Option<f32>,
    pub view_position: Option<String>,
    pub image_laterality: Option<String>,
    pub instance_number: Option<i32>,
    pub metadata: Vec<(String, String)>,
}

impl DicomImage {
    pub fn is_monochrome(&self) -> bool {
        self.color_mode == ImageColorMode::Monochrome
    }

    pub fn frame_count(&self) -> usize {
        match self.color_mode {
            ImageColorMode::Monochrome => self.mono_frames.len(),
            ImageColorMode::Rgb => self.rgb_frames.len(),
        }
    }

    pub fn frame_mono_pixels(&self, frame_index: usize) -> Option<&[i32]> {
        self.mono_frames
            .get(frame_index)
            .map(|frame| frame.as_slice())
    }

    pub fn frame_rgb_pixels(&self, frame_index: usize) -> Option<&[u8]> {
        self.rgb_frames
            .get(frame_index)
            .map(|frame| frame.as_slice())
    }
}

pub fn load_dicom(path: &Path) -> Result<DicomImage> {
    let obj = open_dicom_object(path)?;

    let width: usize = obj
        .element_by_name("Columns")
        .context("Missing Columns tag")?
        .to_int()
        .context("Invalid Columns value")?;
    let height: usize = obj
        .element_by_name("Rows")
        .context("Missing Rows tag")?
        .to_int()
        .context("Invalid Rows value")?;

    let photometric = read_string_or_default(&obj, "PhotometricInterpretation", "MONOCHROME2");
    let invert = photometric.eq_ignore_ascii_case("MONOCHROME1");

    let decoded = obj
        .decode_pixel_data()
        .context("Failed to decode PixelData")?;

    let decoded_width = decoded.columns() as usize;
    let decoded_height = decoded.rows() as usize;
    if decoded_width != width || decoded_height != height {
        bail!(
            "Decoded frame dimensions mismatch: decoded={}x{}, tags={}x{}",
            decoded_width,
            decoded_height,
            width,
            height
        );
    }

    let frame_count = decoded.number_of_frames() as usize;
    if frame_count == 0 {
        bail!("Decoded pixel data has no frames");
    }

    let samples_per_pixel = decoded.samples_per_pixel();
    let recommended_cine_fps = read_float_first(&obj, "FrameTime")
        .filter(|value| *value > 0.0)
        .map(|frame_time_ms| 1000.0 / frame_time_ms)
        .or_else(|| read_float_first(&obj, "CineRate").filter(|value| *value > 0.0));
    let view_position = read_view_position(&obj);
    let image_laterality = read_laterality(&obj);
    let instance_number = read_int_first(&obj, "InstanceNumber");
    let metadata = collect_metadata(&obj);

    match samples_per_pixel {
        1 => {
            let bits_allocated = decoded.bits_allocated();
            if bits_allocated != 8 && bits_allocated != 16 {
                bail!("BitsAllocated={} is not supported (only 8/16)", bits_allocated);
            }

            let mut mono_frames = Vec::with_capacity(frame_count);
            for frame_index in 0..frame_count {
                let frame_pixels: Vec<i32> = decoded
                    .to_vec_frame(frame_index as u32)
                    .with_context(|| format!("Could not convert decoded frame {} to i32 samples", frame_index))?;
                if frame_pixels.len() != width * height {
                    bail!(
                        "Decoded pixel count mismatch in frame {}: got {}, expected {}",
                        frame_index,
                        frame_pixels.len(),
                        width * height
                    );
                }
                mono_frames.push(frame_pixels);
            }

            let (min_value, max_value) =
                min_max_frames(&mono_frames).context("No pixels available for rendering")?;

            let default_center = read_float_first(&obj, "WindowCenter")
                .unwrap_or_else(|| (min_value + max_value) as f32 / 2.0);
            let default_width = read_float_first(&obj, "WindowWidth")
                .unwrap_or_else(|| (max_value - min_value).max(1) as f32);

            Ok(DicomImage {
                width,
                height,
                mono_frames,
                rgb_frames: Vec::new(),
                color_mode: ImageColorMode::Monochrome,
                samples_per_pixel,
                invert,
                window_center: default_center,
                window_width: default_width.max(1.0),
                min_value,
                max_value,
                recommended_cine_fps,
                view_position,
                image_laterality,
                instance_number,
                metadata,
            })
        }
        spp if spp >= 3 => {
            let bits_allocated = decoded.bits_allocated();
            if bits_allocated != 8 && bits_allocated != 16 {
                bail!("BitsAllocated={} is not supported for color images (only 8/16)", bits_allocated);
            }

            let expected_len = width
                .checked_mul(height)
                .and_then(|v| v.checked_mul(samples_per_pixel as usize))
                .context("Overflow while calculating color frame size")?;
            let bits_shift = decoded.bits_stored().saturating_sub(8);

            let mut rgb_frames = Vec::with_capacity(frame_count);
            for frame_index in 0..frame_count {
                let frame_pixels: Vec<u8> = if bits_allocated == 8 {
                    decoded
                        .to_vec_frame(frame_index as u32)
                        .with_context(|| format!("Could not convert decoded frame {} to u8 samples", frame_index))?
                } else {
                    let frame_pixels_u16: Vec<u16> = decoded
                        .to_vec_frame(frame_index as u32)
                        .with_context(|| format!("Could not convert decoded frame {} to u16 samples", frame_index))?;
                    frame_pixels_u16
                        .into_iter()
                        .map(|sample| (sample >> bits_shift) as u8)
                        .collect()
                };

                if frame_pixels.len() != expected_len {
                    bail!(
                        "Decoded color pixel count mismatch in frame {}: got {}, expected {}",
                        frame_index,
                        frame_pixels.len(),
                        expected_len
                    );
                }
                rgb_frames.push(frame_pixels);
            }

            Ok(DicomImage {
                width,
                height,
                mono_frames: Vec::new(),
                rgb_frames,
                color_mode: ImageColorMode::Rgb,
                samples_per_pixel,
                invert: false,
                window_center: 127.5,
                window_width: 255.0,
                min_value: 0,
                max_value: 255,
                recommended_cine_fps,
                view_position,
                image_laterality,
                instance_number,
                metadata,
            })
        }
        other => bail!(
            "Unsupported SamplesPerPixel={} (currently supports 1 for monochrome and >=3 for color)",
            other
        ),
    }
}

fn open_dicom_object(path: &Path) -> Result<DefaultDicomObject> {
    match open_file(path) {
        Ok(obj) => Ok(obj),
        Err(err) => {
            if is_missing_meta_group_length_error(&err) {
                let bytes =
                    fs::read(path).with_context(|| format!("Could not read {}", path.display()))?;

                if let Some(repaired) = repair_missing_meta_group_length(&bytes) {
                    return from_reader(Cursor::new(repaired)).with_context(|| {
                        format!(
                            "Could not open {} after repairing missing File Meta Information Group Length (0002,0000)",
                            path.display()
                        )
                    });
                }
            }

            Err(err).with_context(|| format!("Could not open {}", path.display()))
        }
    }
}

fn is_missing_meta_group_length_error(error: &ReadError) -> bool {
    matches!(
        error,
        ReadError::ParseMetaDataSet {
            source: dicom_object::meta::Error::UnexpectedTag { tag, .. }
        } if tag.group() == 0x0002 && tag.element() != 0x0000
    )
}

fn repair_missing_meta_group_length(bytes: &[u8]) -> Option<Vec<u8>> {
    let offset = detect_dicom_prefix_offset(bytes)?;
    if bytes.len() < offset + 4 {
        return None;
    }

    let first_group = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
    let first_element = u16::from_le_bytes([bytes[offset + 2], bytes[offset + 3]]);
    if first_group != 0x0002 || first_element == 0x0000 {
        return None;
    }

    let meta_group_len = scan_meta_group_len_without_group_length(bytes, offset)?;
    let meta_group_len_u32 = u32::try_from(meta_group_len).ok()?;

    let mut repaired = Vec::with_capacity(bytes.len() + 12);
    repaired.extend_from_slice(&bytes[..offset]);
    repaired.extend_from_slice(&build_group_length_element(meta_group_len_u32));
    repaired.extend_from_slice(&bytes[offset..]);
    Some(repaired)
}

fn detect_dicom_prefix_offset(bytes: &[u8]) -> Option<usize> {
    if bytes.len() >= 132 && &bytes[128..132] == b"DICM" {
        return Some(132);
    }
    if bytes.len() >= 4 && &bytes[..4] == b"DICM" {
        return Some(4);
    }
    None
}

fn scan_meta_group_len_without_group_length(bytes: &[u8], start: usize) -> Option<usize> {
    let mut position = start;
    while position + 8 <= bytes.len() {
        let group = u16::from_le_bytes([bytes[position], bytes[position + 1]]);
        if group != 0x0002 {
            break;
        }

        let vr = [bytes[position + 4], bytes[position + 5]];
        let (header_len, value_len) = read_explicit_vr_element_length(bytes, position, vr)?;
        let next = position
            .checked_add(header_len)?
            .checked_add(value_len as usize)?;

        if next > bytes.len() {
            return None;
        }
        position = next;
    }

    if position > start {
        Some(position - start)
    } else {
        None
    }
}

fn read_explicit_vr_element_length(
    bytes: &[u8],
    position: usize,
    vr: [u8; 2],
) -> Option<(usize, u32)> {
    let uses_u32_len = matches!(
        vr,
        [b'O', b'B']
            | [b'O', b'D']
            | [b'O', b'F']
            | [b'O', b'L']
            | [b'O', b'W']
            | [b'S', b'Q']
            | [b'U', b'C']
            | [b'U', b'R']
            | [b'U', b'T']
            | [b'U', b'N']
    );

    if uses_u32_len {
        if position + 12 > bytes.len() {
            return None;
        }
        let value_len = u32::from_le_bytes([
            bytes[position + 8],
            bytes[position + 9],
            bytes[position + 10],
            bytes[position + 11],
        ]);
        if value_len == u32::MAX {
            return None;
        }
        Some((12, value_len))
    } else {
        let value_len = u16::from_le_bytes([bytes[position + 6], bytes[position + 7]]) as u32;
        Some((8, value_len))
    }
}

fn build_group_length_element(group_len: u32) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[0..2].copy_from_slice(&0x0002u16.to_le_bytes());
    out[2..4].copy_from_slice(&0x0000u16.to_le_bytes());
    out[4..6].copy_from_slice(b"UL");
    out[6..8].copy_from_slice(&4u16.to_le_bytes());
    out[8..12].copy_from_slice(&group_len.to_le_bytes());
    out
}

fn collect_metadata(obj: &DefaultDicomObject) -> Vec<(String, String)> {
    METADATA_FIELD_NAMES
        .iter()
        .filter_map(|name| {
            obj.element_by_name(name)
                .ok()
                .and_then(|el| el.to_str().ok().map(|v| (*name, v.to_string())))
        })
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

fn read_string_or_default(obj: &DefaultDicomObject, name: &str, default: &str) -> String {
    obj.element_by_name(name)
        .ok()
        .and_then(|el| el.to_str().ok())
        .map(|v| v.to_string())
        .unwrap_or_else(|| default.to_string())
}

fn read_string(obj: &DefaultDicomObject, name: &str) -> Option<String> {
    obj.element_by_name(name)
        .ok()
        .and_then(|el| el.to_str().ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn read_nested_string(
    obj: &DefaultDicomObject,
    seq_tag: Tag,
    item_index: u32,
    element_tag: Tag,
) -> Option<String> {
    obj.value_at((seq_tag, item_index, element_tag))
        .ok()
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_nested_string2(
    obj: &DefaultDicomObject,
    seq_tag_1: Tag,
    item_index_1: u32,
    seq_tag_2: Tag,
    item_index_2: u32,
    element_tag: Tag,
) -> Option<String> {
    obj.value_at((
        seq_tag_1,
        item_index_1,
        seq_tag_2,
        item_index_2,
        element_tag,
    ))
    .ok()
    .and_then(|value| value.to_str().ok())
    .map(|value| value.trim().to_string())
    .filter(|value| !value.is_empty())
}

fn normalize_view_position(raw: &str) -> Option<String> {
    let token = raw
        .trim()
        .to_ascii_uppercase()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>();

    if token.is_empty() {
        return None;
    }

    if token.contains("R10242") || token.contains("CRANIOCAUDAL") || token.contains("CC") {
        return Some("CC".to_string());
    }

    if token.contains("R10226") || token.contains("MEDIOLATERALOBLIQUE") || token.contains("MLO") {
        return Some("MLO".to_string());
    }

    Some(raw.trim().to_string())
}

fn read_view_position(obj: &DefaultDicomObject) -> Option<String> {
    const VIEW_CODE_SEQUENCE: Tag = Tag(0x0054, 0x0220);
    const CODE_MEANING: Tag = Tag(0x0008, 0x0104);
    const CODE_VALUE: Tag = Tag(0x0008, 0x0100);

    read_string(obj, "ViewPosition")
        .or_else(|| read_nested_string(obj, VIEW_CODE_SEQUENCE, 0, CODE_MEANING))
        .or_else(|| read_nested_string(obj, VIEW_CODE_SEQUENCE, 0, CODE_VALUE))
        .or_else(|| read_string(obj, "SeriesDescription"))
        .and_then(|raw| normalize_view_position(&raw))
}

fn normalize_laterality(raw: &str) -> Option<String> {
    let token = raw
        .trim()
        .to_ascii_uppercase()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>();

    if token.is_empty() {
        return None;
    }

    if token.starts_with('R') || token.contains("RIGHT") {
        return Some("R".to_string());
    }

    if token.starts_with('L') || token.contains("LEFT") {
        return Some("L".to_string());
    }

    Some(raw.trim().to_string())
}

fn read_laterality(obj: &DefaultDicomObject) -> Option<String> {
    const FRAME_ANATOMY_SEQUENCE: Tag = Tag(0x0020, 0x9071);
    const FRAME_LATERALITY: Tag = Tag(0x0020, 0x9072);
    const SHARED_FUNCTIONAL_GROUPS_SEQUENCE: Tag = Tag(0x5200, 0x9229);

    read_string(obj, "ImageLaterality")
        .or_else(|| read_string(obj, "Laterality"))
        .or_else(|| read_string(obj, "FrameLaterality"))
        .or_else(|| read_nested_string(obj, FRAME_ANATOMY_SEQUENCE, 0, FRAME_LATERALITY))
        .or_else(|| {
            read_nested_string2(
                obj,
                SHARED_FUNCTIONAL_GROUPS_SEQUENCE,
                0,
                FRAME_ANATOMY_SEQUENCE,
                0,
                FRAME_LATERALITY,
            )
        })
        .or_else(|| read_string(obj, "ImageType"))
        .and_then(|raw| normalize_laterality(&raw))
}

fn read_float_first(obj: &DefaultDicomObject, name: &str) -> Option<f32> {
    obj.element_by_name(name)
        .ok()
        .and_then(|el| el.to_str().ok())
        .and_then(|s| parse_multi_valued_number(&s))
}

fn read_int_first(obj: &DefaultDicomObject, name: &str) -> Option<i32> {
    obj.element_by_name(name)
        .ok()
        .and_then(|el| el.to_str().ok())
        .and_then(|value| {
            value
                .split('\\')
                .next()
                .and_then(|v| v.trim().parse::<i32>().ok())
        })
}

fn parse_multi_valued_number(value: &str) -> Option<f32> {
    value.split('\\').next()?.trim().parse::<f32>().ok()
}

fn min_max(values: &[i32]) -> Option<(i32, i32)> {
    let mut iter = values.iter().copied();
    let first = iter.next()?;
    let mut min_v = first;
    let mut max_v = first;
    for v in iter {
        if v < min_v {
            min_v = v;
        }
        if v > max_v {
            max_v = v;
        }
    }
    Some((min_v, max_v))
}

fn min_max_frames(frames: &[Vec<i32>]) -> Option<(i32, i32)> {
    let mut frame_iter = frames.iter();
    let first = frame_iter.next()?;
    let (mut min_value, mut max_value) = min_max(first)?;

    for frame in frame_iter {
        if let Some((frame_min, frame_max)) = min_max(frame) {
            if frame_min < min_value {
                min_value = frame_min;
            }
            if frame_max > max_value {
                max_value = frame_max;
            }
        }
    }

    Some((min_value, max_value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_inserts_group_length_when_missing() {
        let mut bytes = vec![0u8; 128];
        bytes.extend_from_slice(b"DICM");

        // (0002,0002) UI, length 4, value "ABCD"
        bytes.extend_from_slice(&[
            0x02, 0x00, 0x02, 0x00, b'U', b'I', 0x04, 0x00, b'A', b'B', b'C', b'D',
        ]);
        // (0002,0010) UI, length 20, value "TRANSFER-SYNTAX-TEST"
        bytes.extend_from_slice(&[
            0x02, 0x00, 0x10, 0x00, b'U', b'I', 0x14, 0x00, b'T', b'R', b'A', b'N', b'S', b'F',
            b'E', b'R', b'-', b'S', b'Y', b'N', b'T', b'A', b'X', b'-', b'T', b'E', b'S', b'T',
        ]);
        // First data set element (group != 0002) to mark end of meta
        bytes.extend_from_slice(&[0x08, 0x00, 0x16, 0x00, b'U', b'I', 0x02, 0x00, b'1', 0x00]);

        let repaired = repair_missing_meta_group_length(&bytes).expect("expected repaired bytes");

        // Expect insertion right after DICM at offset 132.
        let offset = 132;
        assert_eq!(&repaired[offset..offset + 4], &[0x02, 0x00, 0x00, 0x00]);
        assert_eq!(&repaired[offset + 4..offset + 6], b"UL");
        assert_eq!(&repaired[offset + 6..offset + 8], &[0x04, 0x00]);

        // Group length should match original meta content size (40 bytes).
        assert_eq!(&repaired[offset + 8..offset + 12], &40u32.to_le_bytes());
        assert_eq!(repaired.len(), bytes.len() + 12);
    }

    #[test]
    fn repair_is_noop_when_group_length_already_exists() {
        let mut bytes = vec![0u8; 128];
        bytes.extend_from_slice(b"DICM");
        bytes.extend_from_slice(&[
            0x02, 0x00, 0x00, 0x00, b'U', b'L', 0x04, 0x00, 0x08, 0x00, 0x00, 0x00, 0x08, 0x00,
            0x16, 0x00, b'U', b'I', 0x02, 0x00, b'1', 0x00,
        ]);

        assert!(repair_missing_meta_group_length(&bytes).is_none());
    }

    #[test]
    fn view_position_from_tag_when_available() {
        let path = std::path::Path::new("samples/sample0/1.dcm");
        if !path.exists() {
            return;
        }

        let obj = open_dicom_object(path).expect("sample0/1.dcm should open");
        assert_eq!(read_view_position(&obj).as_deref(), Some("CC"));
    }

    #[test]
    fn view_position_falls_back_to_view_code_sequence() {
        let cc_path = std::path::Path::new("samples/sample2/D0000006");
        let mlo_path = std::path::Path::new("samples/sample2/D0000010");
        if !cc_path.exists() || !mlo_path.exists() {
            return;
        }

        let cc_obj = open_dicom_object(cc_path).expect("sample2/D0000006 should open");
        let mlo_obj = open_dicom_object(mlo_path).expect("sample2/D0000010 should open");
        assert_eq!(read_view_position(&cc_obj).as_deref(), Some("CC"));
        assert_eq!(read_view_position(&mlo_obj).as_deref(), Some("MLO"));
    }

    #[test]
    fn laterality_falls_back_to_frame_laterality() {
        let left_path = std::path::Path::new("samples/sample3/IMG-0005-00001.dcm");
        let right_path = std::path::Path::new("samples/sample3/IMG-0011-00001.dcm");
        if !left_path.exists() || !right_path.exists() {
            return;
        }

        let left_obj = open_dicom_object(left_path).expect("sample3 left image should open");
        let right_obj = open_dicom_object(right_path).expect("sample3 right image should open");
        assert_eq!(read_laterality(&left_obj).as_deref(), Some("L"));
        assert_eq!(read_laterality(&right_obj).as_deref(), Some("R"));
    }
}
