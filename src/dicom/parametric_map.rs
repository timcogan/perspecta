use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use dicom_object::{DefaultDicomObject, Tag};
use dicom_pixeldata::PixelDecoder;

use super::{
    classify_dicom_object, collect_full_metadata, collect_metadata, open_dicom_object,
    read_int_first, read_item_multi_int, read_item_string, read_laterality, read_string,
    read_view_position, sequence_items_from_item, sequence_items_from_object, DicomImage,
    DicomPathKind, DicomSource, ImageColorMode, MonoFrames, RgbFrames,
};

const FLOAT_PIXEL_DATA: Tag = Tag(0x7FE0, 0x0008);
const DOUBLE_FLOAT_PIXEL_DATA: Tag = Tag(0x7FE0, 0x0009);
const REFERENCED_IMAGE_SEQUENCE: Tag = Tag(0x0008, 0x1140);
const SOURCE_IMAGE_SEQUENCE: Tag = Tag(0x0008, 0x2112);
const REFERENCED_SOP_INSTANCE_UID: Tag = Tag(0x0008, 0x1155);
const REFERENCED_FRAME_NUMBER: Tag = Tag(0x0008, 0x1160);
const DERIVATION_IMAGE_SEQUENCE: Tag = Tag(0x0008, 0x9124);
const SHARED_FUNCTIONAL_GROUPS_SEQUENCE: Tag = Tag(0x5200, 0x9229);
const PER_FRAME_FUNCTIONAL_GROUPS_SEQUENCE: Tag = Tag(0x5200, 0x9230);
const DEFAULT_OVERLAY_ALPHA: f32 = 0.45;

type SourceFrameRgbaIndices = HashMap<usize, Vec<usize>>;

#[derive(Debug, Clone)]
pub struct ParametricMapOverlayLayer {
    pub width: usize,
    pub height: usize,
    rgba_frames: Arc<[Arc<[u8]>]>,
    source_frame_rgba_indices: Option<SourceFrameRgbaIndices>,
}

impl ParametricMapOverlayLayer {
    fn source_frame_indices(&self, frame_count: usize) -> Vec<usize> {
        if frame_count == 0 {
            return Vec::new();
        }

        let mut indices =
            if let Some(source_frame_rgba_indices) = self.source_frame_rgba_indices.as_ref() {
                source_frame_rgba_indices
                    .keys()
                    .copied()
                    .filter(|frame_index| *frame_index < frame_count)
                    .collect::<Vec<_>>()
            } else if self.rgba_frames.len() == 1 || self.rgba_frames.len() == frame_count {
                (0..frame_count).collect::<Vec<_>>()
            } else {
                Vec::new()
            };

        indices.sort_unstable();
        indices.dedup();
        indices
    }

    fn is_compatible_with_target(&self, width: usize, height: usize, frame_count: usize) -> bool {
        self.width == width
            && self.height == height
            && !self.rgba_frames.is_empty()
            && !self.source_frame_indices(frame_count).is_empty()
    }

    fn rgba_for_source_frame(
        &self,
        stored_frame_index: usize,
        target_frame_count: usize,
    ) -> Vec<&Arc<[u8]>> {
        if !self.is_compatible_with_target(self.width, self.height, target_frame_count) {
            return Vec::new();
        }

        if let Some(source_frame_rgba_indices) = self.source_frame_rgba_indices.as_ref() {
            return source_frame_rgba_indices
                .get(&stored_frame_index)
                .into_iter()
                .flat_map(|rgba_indices| rgba_indices.iter())
                .filter_map(|rgba_index| self.rgba_frames.get(*rgba_index))
                .collect::<Vec<_>>();
        }

        if self.rgba_frames.len() == 1 {
            return self.rgba_frames.first().into_iter().collect::<Vec<_>>();
        }

        self.rgba_frames
            .get(stored_frame_index)
            .into_iter()
            .collect::<Vec<_>>()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ParametricMapOverlay {
    pub layers: Vec<ParametricMapOverlayLayer>,
}

impl ParametricMapOverlay {
    pub fn is_empty(&self) -> bool {
        self.layers.is_empty()
    }

    pub fn filtered_for_target(&self, width: usize, height: usize, frame_count: usize) -> Self {
        Self {
            layers: self
                .layers
                .iter()
                .filter(|layer| layer.is_compatible_with_target(width, height, frame_count))
                .cloned()
                .collect(),
        }
    }

    pub fn rgba_frames_for_source_frame(
        &self,
        stored_frame_index: usize,
        target_frame_count: usize,
    ) -> Vec<&Arc<[u8]>> {
        let mut rgba_frames = Vec::new();
        for layer in &self.layers {
            rgba_frames.extend(layer.rgba_for_source_frame(stored_frame_index, target_frame_count));
        }
        rgba_frames
    }

    pub fn source_frame_indices(&self, frame_count: usize) -> Vec<usize> {
        let mut indices = self
            .layers
            .iter()
            .flat_map(|layer| layer.source_frame_indices(frame_count))
            .collect::<Vec<_>>();
        indices.sort_unstable();
        indices.dedup();
        indices
    }
}

#[derive(Debug)]
struct ParsedParametricMap {
    display_image: DicomImage,
    overlay_layer: ParametricMapOverlayLayer,
    references: HashMap<String, Option<SourceFrameRgbaIndices>>,
}

pub fn load_parametric_map(source: impl Into<DicomSource>) -> Result<DicomImage> {
    let source = source.into();
    let obj = open_dicom_object(&source)?;
    let parsed = parse_parametric_map(&obj, &source)?;
    Ok(parsed.display_image)
}

pub fn load_parametric_map_overlays(
    source: impl Into<DicomSource>,
) -> Result<HashMap<String, ParametricMapOverlay>> {
    let source = source.into();
    let obj = open_dicom_object(&source)?;
    let parsed = parse_parametric_map(&obj, &source)?;

    let mut overlays = HashMap::<String, ParametricMapOverlay>::new();
    for (sop_instance_uid, source_frame_rgba_indices) in parsed.references {
        let mut overlay_layer = parsed.overlay_layer.clone();
        overlay_layer.source_frame_rgba_indices = source_frame_rgba_indices;
        overlays
            .entry(sop_instance_uid)
            .or_default()
            .layers
            .push(overlay_layer);
    }
    overlays.retain(|_, overlay| !overlay.is_empty());
    Ok(overlays)
}

fn parse_parametric_map(
    obj: &DefaultDicomObject,
    source_label: &DicomSource,
) -> Result<ParsedParametricMap> {
    if classify_dicom_object(obj) != DicomPathKind::ParametricMap {
        let sop_class = read_string(obj, "SOPClassUID").unwrap_or_else(|| "unknown".to_string());
        bail!(
            "{} is not a Parametric Map object (SOPClassUID={})",
            source_label,
            sop_class
        );
    }

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

    let samples_per_pixel: usize = obj
        .element_by_name("SamplesPerPixel")
        .context("Missing SamplesPerPixel tag")?
        .to_int()
        .context("Invalid SamplesPerPixel value")?;
    if samples_per_pixel != 1 {
        bail!(
            "Parametric Map SamplesPerPixel={} is not supported",
            samples_per_pixel
        );
    }

    let photometric =
        read_string(obj, "PhotometricInterpretation").unwrap_or_else(|| "MONOCHROME2".to_string());
    if !photometric.eq_ignore_ascii_case("MONOCHROME2")
        && !photometric.eq_ignore_ascii_case("MONOCHROME1")
    {
        bail!(
            "Parametric Map PhotometricInterpretation={} is not supported",
            photometric
        );
    }

    if read_string(obj, "PixelPresentation")
        .is_some_and(|value| !value.eq_ignore_ascii_case("MONOCHROME"))
    {
        bail!("Parametric Map PixelPresentation is not supported");
    }

    if obj.element(Tag(0x0028, 0x1101)).is_ok()
        || obj.element(Tag(0x0028, 0x1102)).is_ok()
        || obj.element(Tag(0x0028, 0x1103)).is_ok()
    {
        bail!("Parametric Map Palette Color LUT rendering is not supported");
    }

    let frame_count = match read_int_first(obj, "NumberOfFrames") {
        Some(value) if value > 0 => value as usize,
        Some(value) => bail!("Invalid NumberOfFrames={} (must be >= 1)", value),
        None => 1,
    };

    let scalar_frames = decode_parametric_map_frames(obj, width, height, frame_count)?;
    let (min_value, max_value) = scalar_min_max(&scalar_frames)?;
    let rgb_frames = scalar_frames
        .iter()
        .map(|frame| render_heatmap_rgb(frame, min_value, max_value))
        .collect::<Vec<_>>();
    let rgba_frames = scalar_frames
        .iter()
        .map(|frame| render_heatmap_rgba(frame, min_value, max_value, DEFAULT_OVERLAY_ALPHA))
        .collect::<Vec<_>>();

    let display_image = DicomImage {
        width,
        height,
        mono_frames: MonoFrames::None,
        rgb_frames: RgbFrames::Eager(rgb_frames),
        frame_count,
        color_mode: ImageColorMode::Rgb,
        samples_per_pixel: 3,
        invert: false,
        window_center: 127.5,
        window_width: 255.0,
        min_value: 0,
        max_value: 255,
        recommended_cine_fps: None,
        view_position: read_view_position(obj),
        image_laterality: read_laterality(obj),
        instance_number: read_int_first(obj, "InstanceNumber"),
        sop_instance_uid: read_string(obj, "SOPInstanceUID"),
        reverse_frame_order: false,
        gsps_overlay: None,
        sr_overlay: None,
        pm_overlay: None,
        metadata: collect_metadata(obj),
        full_metadata: collect_full_metadata(obj),
    };

    Ok(ParsedParametricMap {
        display_image,
        overlay_layer: ParametricMapOverlayLayer {
            width,
            height,
            rgba_frames: Arc::<[Arc<[u8]>]>::from(rgba_frames.into_boxed_slice()),
            source_frame_rgba_indices: None,
        },
        references: build_reference_maps(collect_parametric_map_references(obj), frame_count),
    })
}

fn decode_parametric_map_frames(
    obj: &DefaultDicomObject,
    width: usize,
    height: usize,
    frame_count: usize,
) -> Result<Vec<Arc<[f32]>>> {
    let expected_samples = width
        .checked_mul(height)
        .context("Overflow while calculating Parametric Map frame size")?;

    if let Ok(element) = obj.element(FLOAT_PIXEL_DATA) {
        let values = element
            .to_multi_float32()
            .context("Could not decode Float Pixel Data")?;
        return split_scalar_frames(values, frame_count, expected_samples);
    }

    if let Ok(element) = obj.element(DOUBLE_FLOAT_PIXEL_DATA) {
        let values = element
            .to_multi_float64()
            .context("Could not decode Double Float Pixel Data")?
            .into_iter()
            .map(|value| value as f32)
            .collect::<Vec<_>>();
        return split_scalar_frames(values, frame_count, expected_samples);
    }

    if obj.element(Tag(0x7FE0, 0x0010)).is_ok() {
        let mut frames = Vec::with_capacity(frame_count);
        for frame_index in 0..frame_count {
            let decoded = obj
                .decode_pixel_data_frame(frame_index as u32)
                .with_context(|| {
                    format!("Failed to decode Parametric Map PixelData frame {frame_index}")
                })?;
            if decoded.samples_per_pixel() != 1 {
                bail!(
                    "Parametric Map PixelData frame {} has SamplesPerPixel={}",
                    frame_index,
                    decoded.samples_per_pixel()
                );
            }

            let frame_pixels: Vec<i32> = decoded.to_vec_frame(0).with_context(|| {
                format!("Could not convert Parametric Map frame {frame_index} to samples")
            })?;
            if frame_pixels.len() != expected_samples {
                bail!(
                    "Parametric Map frame {} pixel count mismatch: got {}, expected {}",
                    frame_index,
                    frame_pixels.len(),
                    expected_samples
                );
            }
            let scalar_frame = frame_pixels
                .into_iter()
                .map(|value| value as f32)
                .collect::<Vec<_>>();
            frames.push(Arc::<[f32]>::from(scalar_frame.into_boxed_slice()));
        }
        return Ok(frames);
    }

    bail!(
        "Parametric Map does not contain Pixel Data, Float Pixel Data, or Double Float Pixel Data"
    )
}

fn split_scalar_frames(
    values: Vec<f32>,
    frame_count: usize,
    samples_per_frame: usize,
) -> Result<Vec<Arc<[f32]>>> {
    let expected_values = frame_count
        .checked_mul(samples_per_frame)
        .context("Overflow while calculating Parametric Map sample count")?;
    if values.len() != expected_values {
        bail!(
            "Parametric Map sample count mismatch: got {}, expected {}",
            values.len(),
            expected_values
        );
    }

    Ok(values
        .chunks_exact(samples_per_frame)
        .map(|chunk| Arc::<[f32]>::from(chunk.to_vec().into_boxed_slice()))
        .collect())
}

fn scalar_min_max(frames: &[Arc<[f32]>]) -> Result<(f32, f32)> {
    let mut min_value = f32::INFINITY;
    let mut max_value = f32::NEG_INFINITY;
    for frame in frames {
        for value in frame.iter().copied().filter(|value| value.is_finite()) {
            min_value = min_value.min(value);
            max_value = max_value.max(value);
        }
    }

    if !min_value.is_finite() || !max_value.is_finite() {
        bail!("Parametric Map does not contain any finite scalar samples");
    }

    Ok((min_value, max_value))
}

fn render_heatmap_rgb(samples: &[f32], min_value: f32, max_value: f32) -> Arc<[u8]> {
    let mut pixels = Vec::with_capacity(samples.len().saturating_mul(3));
    for &sample in samples {
        let (r, g, b) = heatmap_rgb(sample, min_value, max_value);
        pixels.extend_from_slice(&[r, g, b]);
    }
    Arc::<[u8]>::from(pixels.into_boxed_slice())
}

fn render_heatmap_rgba(samples: &[f32], min_value: f32, max_value: f32, alpha: f32) -> Arc<[u8]> {
    let mut pixels = Vec::with_capacity(samples.len().saturating_mul(4));
    for &sample in samples {
        let normalized = heatmap_normalized(sample, min_value, max_value);
        let (r, g, b) = heatmap_rgb_from_normalized(normalized);
        let alpha_value = if normalized == 0.0 {
            0
        } else {
            (normalized.clamp(0.0, 1.0) * alpha.clamp(0.0, 1.0) * 255.0).round() as u8
        };
        pixels.extend_from_slice(&[r, g, b, alpha_value]);
    }
    Arc::<[u8]>::from(pixels.into_boxed_slice())
}

fn heatmap_rgb(sample: f32, min_value: f32, max_value: f32) -> (u8, u8, u8) {
    let normalized = heatmap_normalized(sample, min_value, max_value);
    heatmap_rgb_from_normalized(normalized)
}

fn heatmap_rgb_from_normalized(normalized: f32) -> (u8, u8, u8) {
    if normalized == 0.0 {
        return (0, 0, 0);
    }

    let (r, g, b) = if normalized < 0.33 {
        let t = normalized / 0.33;
        (0.0, 64.0 + t * 191.0, 255.0)
    } else if normalized < 0.66 {
        let t = (normalized - 0.33) / 0.33;
        (t * 255.0, 255.0, (1.0 - t) * 255.0)
    } else {
        let t = (normalized - 0.66) / 0.34;
        (255.0, (1.0 - t) * 255.0, 0.0)
    };

    (r.round() as u8, g.round() as u8, b.round() as u8)
}

fn heatmap_normalized(sample: f32, min_value: f32, max_value: f32) -> f32 {
    if !sample.is_finite() {
        return 0.0;
    }

    if (max_value - min_value).abs() <= f32::EPSILON {
        return 1.0;
    }

    normalize_sample(sample, min_value, max_value)
}

fn normalize_sample(sample: f32, min_value: f32, max_value: f32) -> f32 {
    if !sample.is_finite() {
        return 0.0;
    }

    let range = (max_value - min_value).max(f32::EPSILON);
    ((sample - min_value) / range).clamp(0.0, 1.0)
}

#[derive(Debug)]
struct RawReferenceTarget {
    sop_instance_uid: String,
    referenced_frames: Option<Vec<usize>>,
}

fn collect_parametric_map_references(obj: &DefaultDicomObject) -> Vec<RawReferenceTarget> {
    let mut references = Vec::new();

    for seq_tag in [
        SOURCE_IMAGE_SEQUENCE,
        REFERENCED_IMAGE_SEQUENCE,
        DERIVATION_IMAGE_SEQUENCE,
        SHARED_FUNCTIONAL_GROUPS_SEQUENCE,
        PER_FRAME_FUNCTIONAL_GROUPS_SEQUENCE,
    ] {
        for item in sequence_items_from_object(obj, seq_tag)
            .into_iter()
            .flatten()
        {
            collect_references_from_item(item, &mut references);
        }
    }

    references
}

fn collect_references_from_item(
    item: &dicom_object::InMemDicomObject,
    references: &mut Vec<RawReferenceTarget>,
) {
    if let Some(sop_instance_uid) = read_item_string(item, REFERENCED_SOP_INSTANCE_UID) {
        references.push(RawReferenceTarget {
            sop_instance_uid,
            referenced_frames: read_item_multi_int(item, REFERENCED_FRAME_NUMBER).map(|frames| {
                frames
                    .into_iter()
                    .filter_map(|frame| usize::try_from(frame).ok())
                    .filter(|frame| *frame > 0)
                    .collect::<Vec<_>>()
            }),
        });
    }

    for seq_tag in [
        SOURCE_IMAGE_SEQUENCE,
        REFERENCED_IMAGE_SEQUENCE,
        DERIVATION_IMAGE_SEQUENCE,
    ] {
        for nested_item in sequence_items_from_item(item, seq_tag)
            .into_iter()
            .flatten()
        {
            collect_references_from_item(nested_item, references);
        }
    }
}

fn build_reference_maps(
    references: Vec<RawReferenceTarget>,
    rgba_frame_count: usize,
) -> HashMap<String, Option<SourceFrameRgbaIndices>> {
    let mut explicit_mappings = HashMap::<String, Option<SourceFrameRgbaIndices>>::new();
    let mut next_rgba_index = 0usize;

    for reference in references {
        let referenced_frames = reference
            .referenced_frames
            .filter(|frames| !frames.is_empty());
        match referenced_frames {
            Some(frames) => {
                for frame_number in frames {
                    if !matches!(
                        explicit_mappings.get(&reference.sop_instance_uid),
                        Some(None)
                    ) {
                        let mapping = explicit_mappings
                            .entry(reference.sop_instance_uid.clone())
                            .or_insert_with(|| Some(HashMap::new()));
                        if next_rgba_index < rgba_frame_count {
                            if let Some(source_frame_rgba_indices) = mapping.as_mut() {
                                source_frame_rgba_indices
                                    .entry(frame_number - 1)
                                    .or_default()
                                    .push(next_rgba_index);
                            }
                        }
                    }
                    next_rgba_index = next_rgba_index.saturating_add(1);
                }
            }
            None => {
                explicit_mappings.insert(reference.sop_instance_uid, None);
            }
        }
    }

    explicit_mappings.retain(|_, mapping| {
        mapping.is_none() || mapping.as_ref().is_some_and(|indices| !indices.is_empty())
    });
    explicit_mappings
}

#[cfg(test)]
mod tests {
    use super::*;
    use dicom_core::{DataElement, PrimitiveValue, VR};
    use dicom_object::{mem::InMemElement, FileMetaTableBuilder};

    use crate::dicom::{
        DIGITAL_MAMMOGRAPHY_XRAY_IMAGE_PRESENTATION_SOP_CLASS_UID, EXPLICIT_VR_LITTLE_ENDIAN_UID,
        PARAMETRIC_MAP_SOP_CLASS_UID,
    };

    #[derive(Clone, Copy)]
    struct TestReference<'a> {
        uid: &'a str,
        frames: Option<&'a [usize]>,
    }

    fn build_parametric_map_test_object(
        pixel_data: InMemElement,
        number_of_frames: usize,
        referenced_uid: Option<&str>,
        referenced_frames: Option<&[usize]>,
    ) -> DefaultDicomObject {
        let references = referenced_uid
            .map(|uid| {
                vec![TestReference {
                    uid,
                    frames: referenced_frames,
                }]
            })
            .unwrap_or_default();

        build_parametric_map_test_object_with_references(pixel_data, number_of_frames, &references)
    }

    fn build_parametric_map_test_object_with_references(
        pixel_data: InMemElement,
        number_of_frames: usize,
        references: &[TestReference<'_>],
    ) -> DefaultDicomObject {
        let mut elements = vec![
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, PARAMETRIC_MAP_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, "9.8.7.6.5"),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "MG"),
            DataElement::new(Tag(0x0028, 0x0002), VR::US, PrimitiveValue::from(1u16)),
            DataElement::new(Tag(0x0028, 0x0004), VR::CS, "MONOCHROME2"),
            DataElement::new(Tag(0x0028, 0x0010), VR::US, PrimitiveValue::from(1u16)),
            DataElement::new(Tag(0x0028, 0x0011), VR::US, PrimitiveValue::from(1u16)),
            DataElement::new(Tag(0x0028, 0x0008), VR::IS, number_of_frames.to_string()),
        ];

        if !references.is_empty() {
            let sequence_items = references
                .iter()
                .map(|reference_spec| {
                    let mut reference = dicom_object::InMemDicomObject::from_element_iter([
                        DataElement::new(
                            Tag(0x0008, 0x1150),
                            VR::UI,
                            DIGITAL_MAMMOGRAPHY_XRAY_IMAGE_PRESENTATION_SOP_CLASS_UID,
                        ),
                        DataElement::new(Tag(0x0008, 0x1155), VR::UI, reference_spec.uid),
                    ]);
                    if let Some(frames) = reference_spec.frames {
                        let frame_text = frames
                            .iter()
                            .map(|frame| frame.to_string())
                            .collect::<Vec<_>>()
                            .join("\\");
                        reference.put(DataElement::new(Tag(0x0008, 0x1160), VR::IS, frame_text));
                    }
                    reference
                })
                .collect::<Vec<_>>();
            elements.push(DataElement::new(
                SOURCE_IMAGE_SEQUENCE,
                VR::SQ,
                dicom_core::value::DataSetSequence::from(sequence_items),
            ));
        }

        elements.push(pixel_data);

        let object = dicom_object::InMemDicomObject::from_element_iter(elements)
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(PARAMETRIC_MAP_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid("9.8.7.6.5"),
            )
            .expect("Parametric Map test object should build file meta");

        let mut bytes = Vec::new();
        object
            .write_all(&mut bytes)
            .expect("Parametric Map test object should serialize");
        super::super::open_dicom_object_from_bytes(&bytes, "parametric-map-test")
            .expect("serialized Parametric Map test object should parse")
    }

    fn overlay_for_uid(parsed: &ParsedParametricMap, uid: &str) -> ParametricMapOverlay {
        let mut overlay = ParametricMapOverlay::default();
        let mut layer = parsed.overlay_layer.clone();
        layer.source_frame_rgba_indices = parsed.references.get(uid).cloned().flatten();
        overlay.layers.push(layer);
        overlay
    }

    #[test]
    fn load_parametric_map_renders_rgb_frames_from_float_pixel_data() {
        let obj = build_parametric_map_test_object(
            DataElement::new(
                FLOAT_PIXEL_DATA,
                VR::OF,
                PrimitiveValue::F32(vec![0.0f32, 1.0f32].into()),
            ),
            2,
            None,
            None,
        );

        let image = parse_parametric_map(&obj, &DicomSource::from_memory("pm", Vec::new()))
            .expect("Parametric Map should parse")
            .display_image;
        assert_eq!(image.frame_count(), 2);
        assert_eq!(image.color_mode, ImageColorMode::Rgb);
        assert_eq!(image.samples_per_pixel, 3);
        assert!(image.frame_rgb_pixels(0).is_some());
    }

    #[test]
    fn parse_parametric_map_populates_full_metadata() {
        let obj = build_parametric_map_test_object(
            DataElement::new(
                FLOAT_PIXEL_DATA,
                VR::OF,
                PrimitiveValue::F32(vec![0.25f32].into()),
            ),
            1,
            None,
            None,
        );

        let image = parse_parametric_map(&obj, &DicomSource::from_memory("pm", Vec::new()))
            .expect("Parametric Map should parse")
            .display_image;

        assert!(image
            .full_metadata
            .iter()
            .any(|field| field.keyword == "FloatPixelData"));
        assert!(image
            .full_metadata
            .iter()
            .any(|field| field.keyword == "SOPInstanceUID"));
    }

    #[test]
    fn load_parametric_map_overlays_keys_by_referenced_sop_uid() {
        let obj = build_parametric_map_test_object(
            DataElement::new(
                FLOAT_PIXEL_DATA,
                VR::OF,
                PrimitiveValue::F32(vec![0.5f32].into()),
            ),
            1,
            Some("1.2.3"),
            Some(&[1]),
        );

        let source = DicomSource::from_memory("pm", Vec::new());
        let parsed = parse_parametric_map(&obj, &source).expect("Parametric Map should parse");
        assert!(parsed.references.contains_key("1.2.3"));

        let overlays = parsed
            .references
            .into_iter()
            .map(|(uid, source_frame_rgba_indices)| {
                let mut overlay = ParametricMapOverlay::default();
                let mut layer = parsed.overlay_layer.clone();
                layer.source_frame_rgba_indices = source_frame_rgba_indices;
                overlay.layers.push(layer);
                (uid, overlay)
            })
            .collect::<HashMap<_, _>>();

        let overlay = overlays
            .get("1.2.3")
            .expect("overlay should be keyed by source UID");
        assert_eq!(overlay.source_frame_indices(1), vec![0]);
    }

    #[test]
    fn filtered_parametric_map_overlay_requires_matching_dimensions() {
        let overlay = ParametricMapOverlay {
            layers: vec![ParametricMapOverlayLayer {
                width: 1,
                height: 1,
                rgba_frames: Arc::<[Arc<[u8]>]>::from(
                    vec![Arc::<[u8]>::from([255, 0, 0, 255])].into_boxed_slice(),
                ),
                source_frame_rgba_indices: Some(HashMap::from([(0usize, vec![0usize])])),
            }],
        };

        assert!(overlay.filtered_for_target(1, 1, 1).layers.len() == 1);
        assert!(overlay.filtered_for_target(2, 1, 1).is_empty());
    }

    #[test]
    fn constant_parametric_map_renders_consistent_non_black_frames() {
        let obj = build_parametric_map_test_object(
            DataElement::new(
                FLOAT_PIXEL_DATA,
                VR::OF,
                PrimitiveValue::F32(vec![0.0f32, 0.0f32].into()),
            ),
            2,
            Some("1.2.3"),
            Some(&[1, 2]),
        );

        let parsed = parse_parametric_map(&obj, &DicomSource::from_memory("pm", Vec::new()))
            .expect("constant Parametric Map should parse");
        let frame_0 = parsed
            .display_image
            .frame_rgb_pixels(0)
            .expect("first constant frame should render");
        let frame_1 = parsed
            .display_image
            .frame_rgb_pixels(1)
            .expect("second constant frame should render");

        assert_eq!(frame_0.as_ref(), frame_1.as_ref());
        assert_ne!(frame_0.as_ref(), [0, 0, 0].as_slice());

        let overlay = overlay_for_uid(&parsed, "1.2.3");
        assert_eq!(overlay.source_frame_indices(2), vec![0, 1]);
        assert_eq!(overlay.rgba_frames_for_source_frame(0, 2).len(), 1);
        assert_eq!(overlay.rgba_frames_for_source_frame(1, 2).len(), 1);
    }

    #[test]
    fn load_parametric_map_overlays_preserves_multi_sop_out_of_order_frame_mapping() {
        let obj = build_parametric_map_test_object_with_references(
            DataElement::new(
                FLOAT_PIXEL_DATA,
                VR::OF,
                PrimitiveValue::F32(vec![0.1f32, 0.5f32, 0.9f32].into()),
            ),
            3,
            &[
                TestReference {
                    uid: "1.2.3",
                    frames: Some(&[3, 1]),
                },
                TestReference {
                    uid: "4.5.6",
                    frames: Some(&[2]),
                },
            ],
        );

        let parsed = parse_parametric_map(&obj, &DicomSource::from_memory("pm", Vec::new()))
            .expect("multi-SOP Parametric Map should parse");
        assert!(parsed.references.contains_key("1.2.3"));
        assert!(parsed.references.contains_key("4.5.6"));

        let overlay_a = overlay_for_uid(&parsed, "1.2.3");
        let overlay_b = overlay_for_uid(&parsed, "4.5.6");

        assert_eq!(overlay_a.source_frame_indices(3), vec![0, 2]);
        assert_eq!(overlay_b.source_frame_indices(3), vec![1]);
        assert_eq!(
            overlay_a
                .filtered_for_target(1, 1, 3)
                .source_frame_indices(3),
            vec![0, 2]
        );

        let rgba_for_frame_3 = overlay_a.rgba_frames_for_source_frame(2, 3);
        let rgba_for_frame_1 = overlay_a.rgba_frames_for_source_frame(0, 3);
        let rgba_for_frame_2 = overlay_b.rgba_frames_for_source_frame(1, 3);

        assert_eq!(rgba_for_frame_3.len(), 1);
        assert_eq!(rgba_for_frame_1.len(), 1);
        assert_eq!(rgba_for_frame_2.len(), 1);
        assert_eq!(
            rgba_for_frame_3[0].as_ref(),
            parsed.overlay_layer.rgba_frames[0].as_ref()
        );
        assert_eq!(
            rgba_for_frame_1[0].as_ref(),
            parsed.overlay_layer.rgba_frames[1].as_ref()
        );
        assert_eq!(
            rgba_for_frame_2[0].as_ref(),
            parsed.overlay_layer.rgba_frames[2].as_ref()
        );
    }
}
