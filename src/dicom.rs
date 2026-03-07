use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::{fs, io::Cursor};

use anyhow::{bail, Context, Result};
use dicom_object::{from_reader, open_file, DefaultDicomObject, InMemDicomObject, ReadError, Tag};
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

pub const GSPS_SOP_CLASS_UID: &str = "1.2.840.10008.5.1.4.1.1.11.1";
#[cfg(test)]
pub const EXPLICIT_VR_LITTLE_ENDIAN_UID: &str = "1.2.840.10008.1.2.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DicomPathKind {
    Image,
    Gsps,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GspsUnits {
    Pixel,
    Display,
}

#[derive(Debug, Clone)]
pub enum GspsGraphic {
    Point {
        x: f32,
        y: f32,
        units: GspsUnits,
    },
    Polyline {
        points: Vec<(f32, f32)>,
        units: GspsUnits,
        closed: bool,
    },
}

#[derive(Debug, Clone)]
pub struct GspsOverlayGraphic {
    pub graphic: GspsGraphic,
    pub referenced_frames: Option<Vec<usize>>,
}

#[derive(Debug, Clone, Default)]
pub struct GspsOverlay {
    pub graphics: Vec<GspsOverlayGraphic>,
}

impl GspsOverlay {
    pub fn is_empty(&self) -> bool {
        self.graphics.is_empty()
    }

    #[cfg(test)]
    pub fn from_graphics(graphics: Vec<GspsGraphic>) -> Self {
        Self {
            graphics: graphics
                .into_iter()
                .map(|graphic| GspsOverlayGraphic {
                    graphic,
                    referenced_frames: None,
                })
                .collect(),
        }
    }

    pub fn graphics_for_frame(
        &self,
        frame_index: usize,
    ) -> impl Iterator<Item = &GspsGraphic> + '_ {
        let dicom_frame_number = frame_index.saturating_add(1);
        self.graphics.iter().filter_map(move |graphic| {
            let applies = match graphic.referenced_frames.as_ref() {
                Some(frames) => frames.contains(&dicom_frame_number),
                None => true,
            };
            applies.then_some(&graphic.graphic)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ReferencedImageTarget {
    sop_instance_uid: String,
    referenced_frames: Option<Vec<usize>>,
}

type MonoFrameCache = Arc<Mutex<Vec<Option<Arc<[i32]>>>>>;
type RgbFrameCache = Arc<Mutex<Vec<Option<Arc<[u8]>>>>>;

#[derive(Debug, Clone)]
pub struct DicomImage {
    pub width: usize,
    pub height: usize,
    mono_frames: MonoFrames,
    rgb_frames: RgbFrames,
    frame_count: usize,
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
    pub sop_instance_uid: Option<String>,
    pub gsps_overlay: Option<GspsOverlay>,
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
enum MonoFrames {
    None,
    Eager(Vec<Arc<[i32]>>),
    Lazy(LazyMonoFrames),
}

#[derive(Debug, Clone)]
enum RgbFrames {
    None,
    Eager(Vec<Arc<[u8]>>),
    Lazy(LazyRgbFrames),
}

#[derive(Debug, Clone)]
struct LazyMonoFrames {
    path: PathBuf,
    cache: MonoFrameCache,
    preload_started: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
struct LazyRgbFrames {
    path: PathBuf,
    cache: RgbFrameCache,
    preload_started: Arc<AtomicBool>,
}

impl DicomImage {
    pub fn is_monochrome(&self) -> bool {
        self.color_mode == ImageColorMode::Monochrome
    }

    pub fn frame_count(&self) -> usize {
        self.frame_count
    }

    pub fn frame_mono_pixels(&self, frame_index: usize) -> Option<Arc<[i32]>> {
        match &self.mono_frames {
            MonoFrames::None => None,
            MonoFrames::Eager(frames) => frames.get(frame_index).cloned(),
            MonoFrames::Lazy(lazy) => lazy.frame(frame_index),
        }
    }

    pub fn frame_rgb_pixels(&self, frame_index: usize) -> Option<Arc<[u8]>> {
        match &self.rgb_frames {
            RgbFrames::None => None,
            RgbFrames::Eager(frames) => frames.get(frame_index).cloned(),
            RgbFrames::Lazy(lazy) => lazy.frame(frame_index),
        }
    }
}

impl LazyMonoFrames {
    fn frame(&self, frame_index: usize) -> Option<Arc<[i32]>> {
        if let Ok(cache) = self.cache.lock() {
            if let Some(frame) = cache.get(frame_index).and_then(|slot| slot.clone()) {
                self.ensure_background_preload();
                return Some(frame);
            }
        }

        self.ensure_background_preload();
        None
    }

    fn ensure_background_preload(&self) {
        if self.preload_started.swap(true, Ordering::Relaxed) {
            return;
        }
        let path = self.path.clone();
        let cache = Arc::clone(&self.cache);
        let preload_started = Arc::clone(&self.preload_started);
        thread::spawn(move || {
            if let Err(err) = preload_mono_frames_from_path(&path, &cache) {
                preload_started.store(false, Ordering::Relaxed);
                log::warn!(
                    "preload_mono_frames_from_path failed for {}: {err:#}",
                    path.display()
                );
            }
        });
    }
}

impl LazyRgbFrames {
    fn frame(&self, frame_index: usize) -> Option<Arc<[u8]>> {
        if let Ok(cache) = self.cache.lock() {
            if let Some(frame) = cache.get(frame_index).and_then(|slot| slot.clone()) {
                self.ensure_background_preload();
                return Some(frame);
            }
        }

        self.ensure_background_preload();
        None
    }

    fn ensure_background_preload(&self) {
        if self.preload_started.swap(true, Ordering::Relaxed) {
            return;
        }
        let path = self.path.clone();
        let cache = Arc::clone(&self.cache);
        let preload_started = Arc::clone(&self.preload_started);
        thread::spawn(move || {
            if let Err(err) = preload_rgb_frames_from_path(&path, &cache) {
                preload_started.store(false, Ordering::Relaxed);
                log::warn!(
                    "preload_rgb_frames_from_path failed for {}: {err:#}",
                    path.display()
                );
            }
        });
    }
}

pub fn is_gsps_sop_class_uid(uid: &str) -> bool {
    uid.trim() == GSPS_SOP_CLASS_UID
}

pub fn classify_dicom_path(path: &Path) -> Result<DicomPathKind> {
    let obj = open_dicom_object(path)?;
    Ok(classify_dicom_object(&obj))
}

pub fn load_gsps_overlays(path: &Path) -> Result<HashMap<String, GspsOverlay>> {
    let obj = open_dicom_object(path)?;
    if classify_dicom_object(&obj) != DicomPathKind::Gsps {
        let sop_class = read_string(&obj, "SOPClassUID").unwrap_or_else(|| "unknown".to_string());
        bail!(
            "{} is not a GSPS object (SOPClassUID={})",
            path.display(),
            sop_class
        );
    }
    Ok(parse_gsps_overlays(&obj))
}

fn classify_dicom_object(obj: &DefaultDicomObject) -> DicomPathKind {
    if read_string(obj, "SOPClassUID").is_some_and(|uid| is_gsps_sop_class_uid(&uid)) {
        return DicomPathKind::Gsps;
    }
    if obj.element(Tag(0x7FE0, 0x0010)).is_ok() || obj.element_by_name("PixelData").is_ok() {
        return DicomPathKind::Image;
    }
    DicomPathKind::Other
}

fn parse_gsps_overlays(obj: &DefaultDicomObject) -> HashMap<String, GspsOverlay> {
    const REFERENCED_SERIES_SEQUENCE: Tag = Tag(0x0008, 0x1115);
    const GRAPHIC_ANNOTATION_SEQUENCE: Tag = Tag(0x0070, 0x0001);

    let mut overlays_by_uid = HashMap::<String, GspsOverlay>::new();
    let default_refs = collect_root_referenced_image_targets(obj, REFERENCED_SERIES_SEQUENCE);

    let Some(annotations) = sequence_items_from_object(obj, GRAPHIC_ANNOTATION_SEQUENCE) else {
        return overlays_by_uid;
    };

    for annotation in annotations {
        let references = collect_item_referenced_image_targets(annotation);
        let target_refs = if references.is_empty() {
            &default_refs
        } else {
            &references
        };
        if target_refs.is_empty() {
            continue;
        }

        let graphics = collect_graphics_from_annotation(annotation);
        if graphics.is_empty() {
            continue;
        }

        for target in target_refs {
            overlays_by_uid
                .entry(target.sop_instance_uid.clone())
                .or_default()
                .graphics
                .extend(graphics.iter().cloned().map(|graphic| GspsOverlayGraphic {
                    graphic,
                    referenced_frames: target.referenced_frames.clone(),
                }));
        }
    }

    overlays_by_uid.retain(|_, overlay| !overlay.is_empty());
    overlays_by_uid
}

fn collect_root_referenced_image_targets(
    obj: &DefaultDicomObject,
    referenced_series_sequence: Tag,
) -> Vec<ReferencedImageTarget> {
    const REFERENCED_IMAGE_SEQUENCE: Tag = Tag(0x0008, 0x1140);

    let mut targets = Vec::new();
    if let Some(series_items) = sequence_items_from_object(obj, referenced_series_sequence) {
        for series_item in series_items {
            targets.extend(collect_item_referenced_image_targets(series_item));
        }
    }
    if targets.is_empty() {
        if let Some(image_items) = sequence_items_from_object(obj, REFERENCED_IMAGE_SEQUENCE) {
            for image_item in image_items {
                if let Some(target) = referenced_image_target_from_item(image_item) {
                    targets.push(target);
                }
            }
        }
    }
    targets.sort();
    targets.dedup();
    targets
}

fn collect_item_referenced_image_targets(item: &InMemDicomObject) -> Vec<ReferencedImageTarget> {
    const REFERENCED_IMAGE_SEQUENCE: Tag = Tag(0x0008, 0x1140);

    let mut targets = Vec::new();
    let Some(image_items) = sequence_items_from_item(item, REFERENCED_IMAGE_SEQUENCE) else {
        return targets;
    };
    for image_item in image_items {
        if let Some(target) = referenced_image_target_from_item(image_item) {
            targets.push(target);
        }
    }
    targets.sort();
    targets.dedup();
    targets
}

fn referenced_image_target_from_item(item: &InMemDicomObject) -> Option<ReferencedImageTarget> {
    const REFERENCED_SOP_INSTANCE_UID: Tag = Tag(0x0008, 0x1155);
    const REFERENCED_FRAME_NUMBER: Tag = Tag(0x0008, 0x1160);

    let sop_instance_uid = read_item_string(item, REFERENCED_SOP_INSTANCE_UID)?;
    let referenced_frames = read_item_multi_int(item, REFERENCED_FRAME_NUMBER)
        .map(|frames| {
            frames
                .into_iter()
                .filter_map(|frame| usize::try_from(frame).ok())
                .filter(|frame| *frame > 0)
                .collect::<Vec<_>>()
        })
        .filter(|frames| !frames.is_empty())
        .map(|mut frames| {
            frames.sort_unstable();
            frames.dedup();
            frames
        });

    Some(ReferencedImageTarget {
        sop_instance_uid,
        referenced_frames,
    })
}

fn collect_graphics_from_annotation(annotation: &InMemDicomObject) -> Vec<GspsGraphic> {
    const GRAPHIC_OBJECT_SEQUENCE: Tag = Tag(0x0070, 0x0009);

    let mut graphics = Vec::new();
    let Some(graphic_items) = sequence_items_from_item(annotation, GRAPHIC_OBJECT_SEQUENCE) else {
        return graphics;
    };

    for graphic_item in graphic_items {
        graphics.extend(collect_graphics_from_graphic_object(graphic_item));
    }
    graphics
}

fn collect_graphics_from_graphic_object(graphic_item: &InMemDicomObject) -> Vec<GspsGraphic> {
    const GRAPHIC_ANNOTATION_UNITS: Tag = Tag(0x0070, 0x0005);
    const GRAPHIC_DATA: Tag = Tag(0x0070, 0x0022);
    const GRAPHIC_TYPE: Tag = Tag(0x0070, 0x0023);
    const GRAPHIC_FILLED: Tag = Tag(0x0070, 0x0024);

    let units = match read_item_string(graphic_item, GRAPHIC_ANNOTATION_UNITS)
        .map(|value| value.to_ascii_uppercase())
    {
        Some(value) if value == "DISPLAY" => GspsUnits::Display,
        _ => GspsUnits::Pixel,
    };

    let points = read_item_multi_float(graphic_item, GRAPHIC_DATA)
        .map(parse_graphic_points)
        .unwrap_or_default();
    if points.is_empty() {
        return Vec::new();
    }

    let graphic_type = read_item_string(graphic_item, GRAPHIC_TYPE)
        .unwrap_or_else(|| "POLYLINE".to_string())
        .to_ascii_uppercase();

    match graphic_type.as_str() {
        "POINT" => points
            .into_iter()
            .map(|(x, y)| GspsGraphic::Point { x, y, units })
            .collect(),
        "CIRCLE" if points.len() >= 2 => {
            let polyline = approximate_circle(points[0], points[1]);
            vec![GspsGraphic::Polyline {
                points: polyline,
                units,
                closed: true,
            }]
        }
        "ELLIPSE" if points.len() >= 4 => {
            let polyline = approximate_ellipse(points[0], points[1], points[2], points[3]);
            vec![GspsGraphic::Polyline {
                points: polyline,
                units,
                closed: true,
            }]
        }
        _ => {
            let closed = read_item_string(graphic_item, GRAPHIC_FILLED)
                .is_some_and(|value| value.eq_ignore_ascii_case("Y"));
            vec![GspsGraphic::Polyline {
                points,
                units,
                closed,
            }]
        }
    }
}

fn parse_graphic_points(values: Vec<f32>) -> Vec<(f32, f32)> {
    let mut points = Vec::with_capacity(values.len() / 2);
    for pair in values.chunks_exact(2) {
        points.push((pair[0], pair[1]));
    }
    points
}

fn approximate_circle(center: (f32, f32), perimeter: (f32, f32)) -> Vec<(f32, f32)> {
    const STEPS: usize = 64;
    let radius = ((perimeter.0 - center.0).powi(2) + (perimeter.1 - center.1).powi(2)).sqrt();
    if radius <= f32::EPSILON {
        return vec![center];
    }

    (0..STEPS)
        .map(|index| {
            let t = 2.0_f32 * std::f32::consts::PI * (index as f32 / STEPS as f32);
            (center.0 + radius * t.cos(), center.1 + radius * t.sin())
        })
        .collect()
}

fn approximate_ellipse(
    major_start: (f32, f32),
    major_end: (f32, f32),
    minor_start: (f32, f32),
    minor_end: (f32, f32),
) -> Vec<(f32, f32)> {
    const STEPS: usize = 64;
    let center_x = (major_start.0 + major_end.0 + minor_start.0 + minor_end.0) * 0.25;
    let center_y = (major_start.1 + major_end.1 + minor_start.1 + minor_end.1) * 0.25;
    let major_vector = (
        (major_end.0 - major_start.0) * 0.5,
        (major_end.1 - major_start.1) * 0.5,
    );
    let minor_vector = (
        (minor_end.0 - minor_start.0) * 0.5,
        (minor_end.1 - minor_start.1) * 0.5,
    );

    let major_len = (major_vector.0.powi(2) + major_vector.1.powi(2)).sqrt();
    let minor_len = (minor_vector.0.powi(2) + minor_vector.1.powi(2)).sqrt();
    if major_len <= f32::EPSILON || minor_len <= f32::EPSILON {
        return vec![(center_x, center_y)];
    }

    (0..STEPS)
        .map(|index| {
            let t = 2.0_f32 * std::f32::consts::PI * (index as f32 / STEPS as f32);
            (
                center_x + major_vector.0 * t.cos() + minor_vector.0 * t.sin(),
                center_y + major_vector.1 * t.cos() + minor_vector.1 * t.sin(),
            )
        })
        .collect()
}

fn sequence_items_from_object(obj: &DefaultDicomObject, tag: Tag) -> Option<&[InMemDicomObject]> {
    obj.element(tag).ok()?.items()
}

fn sequence_items_from_item(item: &InMemDicomObject, tag: Tag) -> Option<&[InMemDicomObject]> {
    item.element(tag).ok()?.items()
}

fn read_item_string(item: &InMemDicomObject, tag: Tag) -> Option<String> {
    item.element(tag)
        .ok()
        .and_then(|element| element.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_item_multi_float(item: &InMemDicomObject, tag: Tag) -> Option<Vec<f32>> {
    item.element(tag)
        .ok()
        .and_then(|element| element.to_multi_float32().ok())
}

fn read_item_multi_int(item: &InMemDicomObject, tag: Tag) -> Option<Vec<i32>> {
    item.element(tag)
        .ok()
        .and_then(|element| element.to_str().ok())
        .map(|value| {
            value
                .split('\\')
                .filter_map(|part| part.trim().parse::<i32>().ok())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
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
        .decode_pixel_data_frame(0)
        .context("Failed to decode PixelData frame 0")?;

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

    let frame_count = match read_int_first(&obj, "NumberOfFrames") {
        Some(value) if value > 0 => value as usize,
        Some(value) => bail!("Invalid NumberOfFrames={} (must be >= 1)", value),
        None => 1,
    };

    let samples_per_pixel = decoded.samples_per_pixel();
    let recommended_cine_fps = read_float_first(&obj, "FrameTime")
        .filter(|value| *value > 0.0)
        .map(|frame_time_ms| 1000.0 / frame_time_ms)
        .or_else(|| read_float_first(&obj, "CineRate").filter(|value| *value > 0.0));
    let view_position = read_view_position(&obj);
    let image_laterality = read_laterality(&obj);
    let instance_number = read_int_first(&obj, "InstanceNumber");
    let sop_instance_uid = read_string(&obj, "SOPInstanceUID");
    let metadata = collect_metadata(&obj);

    match samples_per_pixel {
        1 => {
            let bits_allocated = decoded.bits_allocated();
            if bits_allocated != 8 && bits_allocated != 16 {
                bail!("BitsAllocated={} is not supported (only 8/16)", bits_allocated);
            }

            let first_frame_pixels: Vec<i32> = decoded
                .to_vec_frame(0)
                .context("Could not convert decoded frame 0 to i32 samples")?;
            if first_frame_pixels.len() != width * height {
                bail!(
                    "Decoded pixel count mismatch in frame 0: got {}, expected {}",
                    first_frame_pixels.len(),
                    width * height
                );
            }

            let (min_value, max_value) =
                min_max(&first_frame_pixels).context("No pixels available for rendering")?;

            let default_center = read_float_first(&obj, "WindowCenter")
                .unwrap_or_else(|| (min_value + max_value) as f32 / 2.0);
            let default_width = read_float_first(&obj, "WindowWidth")
                .unwrap_or_else(|| (max_value - min_value).max(1) as f32);

            let mono_frames = if frame_count == 1 {
                MonoFrames::Eager(vec![Arc::<[i32]>::from(first_frame_pixels.into_boxed_slice())])
            } else {
                let mut cache = vec![None; frame_count];
                cache[0] = Some(Arc::<[i32]>::from(first_frame_pixels.into_boxed_slice()));
                MonoFrames::Lazy(LazyMonoFrames {
                    path: path.to_path_buf(),
                    cache: Arc::new(Mutex::new(cache)),
                    preload_started: Arc::new(AtomicBool::new(false)),
                })
            };

            Ok(DicomImage {
                width,
                height,
                mono_frames,
                rgb_frames: RgbFrames::None,
                frame_count,
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
                sop_instance_uid,
                gsps_overlay: None,
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

            let first_frame_pixels: Vec<u8> = if bits_allocated == 8 {
                decoded
                    .to_vec_frame(0)
                    .context("Could not convert decoded frame 0 to u8 samples")?
            } else {
                let frame_pixels_u16: Vec<u16> = decoded
                    .to_vec_frame(0)
                    .context("Could not convert decoded frame 0 to u16 samples")?;
                frame_pixels_u16
                    .into_iter()
                    .map(|sample| (sample >> bits_shift) as u8)
                    .collect()
            };

            if first_frame_pixels.len() != expected_len {
                bail!(
                    "Decoded color pixel count mismatch in frame 0: got {}, expected {}",
                    first_frame_pixels.len(),
                    expected_len
                );
            }

            let rgb_frames = if frame_count == 1 {
                RgbFrames::Eager(vec![Arc::<[u8]>::from(first_frame_pixels.into_boxed_slice())])
            } else {
                let mut cache = vec![None; frame_count];
                cache[0] = Some(Arc::<[u8]>::from(first_frame_pixels.into_boxed_slice()));
                RgbFrames::Lazy(LazyRgbFrames {
                    path: path.to_path_buf(),
                    cache: Arc::new(Mutex::new(cache)),
                    preload_started: Arc::new(AtomicBool::new(false)),
                })
            };

            Ok(DicomImage {
                width,
                height,
                mono_frames: MonoFrames::None,
                rgb_frames,
                frame_count,
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
                sop_instance_uid,
                gsps_overlay: None,
                metadata,
            })
        }
        other => bail!(
            "Unsupported SamplesPerPixel={} (currently supports 1 for monochrome and >=3 for color)",
            other
        ),
    }
}

fn preload_mono_frames_from_path(path: &Path, cache: &MonoFrameCache) -> Result<()> {
    let frame_count = match cache.lock() {
        Ok(guard) => guard.len(),
        Err(err) => {
            bail!("Background monochrome preload cache lock poisoned: {err}");
        }
    };
    if frame_count <= 1 {
        return Ok(());
    }

    let worker_count = preload_worker_count(frame_count);
    let mut workers = Vec::with_capacity(worker_count);

    for worker_id in 0..worker_count {
        let path = path.to_path_buf();
        let cache = Arc::clone(cache);
        workers.push(thread::spawn(move || -> Result<()> {
            let obj = open_dicom_object(&path)?;
            for frame_index in (worker_id..frame_count).step_by(worker_count) {
                let already_loaded = match cache.lock() {
                    Ok(guard) => guard
                        .get(frame_index)
                        .and_then(|slot| slot.as_ref())
                        .is_some(),
                    Err(err) => {
                        bail!(
                            "Background monochrome preload cache lock poisoned while checking frame {}: {err}",
                            frame_index
                        );
                    }
                };
                if already_loaded {
                    continue;
                }

                let decoded = obj
                    .decode_pixel_data_frame(frame_index as u32)
                    .with_context(|| {
                        format!(
                            "Failed to decode PixelData frame {} for background preload",
                            frame_index
                        )
                    })?;
                if decoded.samples_per_pixel() != 1 {
                    bail!(
                        "Background preload expected monochrome pixels, got SamplesPerPixel={}",
                        decoded.samples_per_pixel()
                    );
                }
                let frame_pixels: Vec<i32> = decoded.to_vec_frame(0).with_context(|| {
                    format!(
                        "Could not convert decoded frame {} to i32 samples",
                        frame_index
                    )
                })?;
                let frame_pixels = Arc::<[i32]>::from(frame_pixels.into_boxed_slice());

                match cache.lock() {
                    Ok(mut guard) => {
                        if let Some(slot) = guard.get_mut(frame_index) {
                            if slot.is_none() {
                                *slot = Some(frame_pixels);
                            }
                        }
                    }
                    Err(err) => {
                        bail!(
                            "Background monochrome preload cache lock poisoned while storing frame {}: {err}",
                            frame_index
                        );
                    }
                }
            }
            Ok(())
        }));
    }

    for worker in workers {
        match worker.join() {
            Ok(result) => result?,
            Err(_) => bail!("Background monochrome preload worker panicked"),
        }
    }

    Ok(())
}

fn preload_rgb_frames_from_path(path: &Path, cache: &RgbFrameCache) -> Result<()> {
    let frame_count = match cache.lock() {
        Ok(guard) => guard.len(),
        Err(err) => {
            bail!("Background RGB preload cache lock poisoned: {err}");
        }
    };
    if frame_count <= 1 {
        return Ok(());
    }

    let worker_count = preload_worker_count(frame_count);
    let mut workers = Vec::with_capacity(worker_count);

    for worker_id in 0..worker_count {
        let path = path.to_path_buf();
        let cache = Arc::clone(cache);
        workers.push(thread::spawn(move || -> Result<()> {
            let obj = open_dicom_object(&path)?;
            for frame_index in (worker_id..frame_count).step_by(worker_count) {
                let already_loaded = match cache.lock() {
                    Ok(guard) => guard
                        .get(frame_index)
                        .and_then(|slot| slot.as_ref())
                        .is_some(),
                    Err(err) => {
                        bail!(
                            "Background RGB preload cache lock poisoned while checking frame {}: {err}",
                            frame_index
                        );
                    }
                };
                if already_loaded {
                    continue;
                }

                let decoded = obj
                    .decode_pixel_data_frame(frame_index as u32)
                    .with_context(|| {
                        format!(
                            "Failed to decode PixelData frame {} for background preload",
                            frame_index
                        )
                    })?;
                let bits_allocated = decoded.bits_allocated();
                if bits_allocated != 8 && bits_allocated != 16 {
                    bail!(
                        "BitsAllocated={} is not supported for color images (only 8/16)",
                        bits_allocated
                    );
                }

                let frame_pixels: Vec<u8> = if bits_allocated == 8 {
                    decoded.to_vec_frame(0).with_context(|| {
                        format!(
                            "Could not convert decoded frame {} to u8 samples",
                            frame_index
                        )
                    })?
                } else {
                    let bits_shift = decoded.bits_stored().saturating_sub(8);
                    let frame_pixels_u16: Vec<u16> =
                        decoded.to_vec_frame(0).with_context(|| {
                            format!(
                                "Could not convert decoded frame {} to u16 samples",
                                frame_index
                            )
                        })?;
                    frame_pixels_u16
                        .into_iter()
                        .map(|sample| (sample >> bits_shift) as u8)
                        .collect()
                };
                let frame_pixels = Arc::<[u8]>::from(frame_pixels.into_boxed_slice());

                match cache.lock() {
                    Ok(mut guard) => {
                        if let Some(slot) = guard.get_mut(frame_index) {
                            if slot.is_none() {
                                *slot = Some(frame_pixels);
                            }
                        }
                    }
                    Err(err) => {
                        bail!(
                            "Background RGB preload cache lock poisoned while storing frame {}: {err}",
                            frame_index
                        );
                    }
                }
            }
            Ok(())
        }));
    }

    for worker in workers {
        match worker.join() {
            Ok(result) => result?,
            Err(_) => bail!("Background RGB preload worker panicked"),
        }
    }

    Ok(())
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

fn preload_worker_count(frame_count: usize) -> usize {
    let auto_workers = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        .clamp(1, 4);

    let configured = configured_preload_workers().unwrap_or(auto_workers);
    configured.clamp(1, 32).min(frame_count.max(1))
}

fn configured_preload_workers() -> Option<usize> {
    static CONFIG: OnceLock<Option<usize>> = OnceLock::new();

    *CONFIG.get_or_init(|| {
        let raw = std::env::var("PERSPECTA_PRELOAD_WORKERS").ok()?;
        let value = raw.trim().parse::<usize>().ok()?;
        if value == 0 {
            return None;
        }
        Some(value)
    })
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

#[cfg(test)]
impl DicomImage {
    pub(crate) fn test_stub(gsps_overlay: Option<GspsOverlay>) -> Self {
        Self {
            width: 1,
            height: 1,
            mono_frames: MonoFrames::None,
            rgb_frames: RgbFrames::None,
            frame_count: 0,
            color_mode: ImageColorMode::Monochrome,
            samples_per_pixel: 1,
            invert: false,
            window_center: 0.0,
            window_width: 1.0,
            min_value: 0,
            max_value: 0,
            recommended_cine_fps: None,
            view_position: None,
            image_laterality: None,
            instance_number: None,
            sop_instance_uid: None,
            gsps_overlay,
            metadata: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dicom_core::value::DataSetSequence;
    use dicom_core::{DataElement, PrimitiveValue, VR};
    use dicom_object::FileMetaTableBuilder;

    fn referenced_image_item(sop_instance_uid: &str) -> InMemDicomObject {
        InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0008, 0x1155),
            VR::UI,
            sop_instance_uid,
        )])
    }

    fn referenced_image_item_with_frames(
        sop_instance_uid: &str,
        referenced_frames: &[i32],
    ) -> InMemDicomObject {
        let mut item = referenced_image_item(sop_instance_uid);
        item.put(DataElement::new(
            Tag(0x0008, 0x1160),
            VR::IS,
            referenced_frames
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join("\\"),
        ));
        item
    }

    fn graphic_object_item(
        graphic_type: &str,
        graphic_data: &[f32],
        units: &str,
        filled: Option<&str>,
    ) -> InMemDicomObject {
        let mut item = InMemDicomObject::new_empty();
        item.put(DataElement::new(Tag(0x0070, 0x0005), VR::CS, units));
        item.put(DataElement::new(
            Tag(0x0070, 0x0022),
            VR::FL,
            PrimitiveValue::F32(graphic_data.iter().copied().collect()),
        ));
        item.put(DataElement::new(Tag(0x0070, 0x0023), VR::CS, graphic_type));
        if let Some(flag) = filled {
            item.put(DataElement::new(Tag(0x0070, 0x0024), VR::CS, flag));
        }
        item
    }

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

    #[test]
    fn parse_graphic_points_reads_xy_pairs() {
        let points = parse_graphic_points(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0]);
        assert_eq!(points, vec![(10.0, 20.0), (30.0, 40.0), (50.0, 60.0)]);
    }

    #[test]
    fn collect_referenced_image_targets_deduplicates_exact_matches() {
        let item = InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0008, 0x1140),
            VR::SQ,
            DataSetSequence::from(vec![
                referenced_image_item("1.2.3"),
                referenced_image_item("1.2.3"),
                referenced_image_item_with_frames("1.2.3", &[1, 3]),
                referenced_image_item_with_frames("1.2.3", &[1, 3]),
                referenced_image_item("2.4.6"),
            ]),
        )]);

        let refs = collect_item_referenced_image_targets(&item);
        assert_eq!(
            refs,
            vec![
                ReferencedImageTarget {
                    sop_instance_uid: "1.2.3".to_string(),
                    referenced_frames: None,
                },
                ReferencedImageTarget {
                    sop_instance_uid: "1.2.3".to_string(),
                    referenced_frames: Some(vec![1, 3]),
                },
                ReferencedImageTarget {
                    sop_instance_uid: "2.4.6".to_string(),
                    referenced_frames: None,
                },
            ]
        );
    }

    #[test]
    fn collect_graphic_object_parses_polyline_units_and_closed_flag() {
        let item = graphic_object_item(
            "POLYLINE",
            &[0.0, 0.0, 1.0, 1.0, 2.0, 2.0],
            "DISPLAY",
            Some("Y"),
        );
        let graphics = collect_graphics_from_graphic_object(&item);
        assert_eq!(graphics.len(), 1);

        match &graphics[0] {
            GspsGraphic::Polyline {
                points,
                units,
                closed,
            } => {
                assert_eq!(points.len(), 3);
                assert_eq!(*units, GspsUnits::Display);
                assert!(*closed);
            }
            other => panic!("Expected polyline, got {other:?}"),
        }
    }

    #[test]
    fn parse_gsps_overlays_maps_annotation_reference_to_target_sop() {
        let annotation = InMemDicomObject::from_element_iter([
            DataElement::new(
                Tag(0x0008, 0x1140),
                VR::SQ,
                DataSetSequence::from(vec![referenced_image_item("1.2.840.1")]),
            ),
            DataElement::new(
                Tag(0x0070, 0x0009),
                VR::SQ,
                DataSetSequence::from(vec![graphic_object_item(
                    "POINT",
                    &[100.0, 120.0],
                    "PIXEL",
                    None,
                )]),
            ),
        ]);

        let gsps_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, GSPS_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, "9.9.9.9"),
            DataElement::new(
                Tag(0x0070, 0x0001),
                VR::SQ,
                DataSetSequence::from(vec![annotation]),
            ),
        ]);

        let gsps_obj = gsps_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(GSPS_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid("9.9.9.9"),
            )
            .expect("GSPS test object should build file meta");

        let overlays = parse_gsps_overlays(&gsps_obj);
        let overlay = overlays
            .get("1.2.840.1")
            .expect("Overlay should be mapped to referenced SOP instance");
        assert_eq!(overlay.graphics.len(), 1);
        assert!(overlay.graphics[0].referenced_frames.is_none());
    }

    #[test]
    fn parse_gsps_overlays_tracks_annotation_referenced_frames() {
        let annotation = InMemDicomObject::from_element_iter([
            DataElement::new(
                Tag(0x0008, 0x1140),
                VR::SQ,
                DataSetSequence::from(vec![referenced_image_item_with_frames(
                    "1.2.840.1",
                    &[2, 4],
                )]),
            ),
            DataElement::new(
                Tag(0x0070, 0x0009),
                VR::SQ,
                DataSetSequence::from(vec![graphic_object_item(
                    "POINT",
                    &[100.0, 120.0],
                    "PIXEL",
                    None,
                )]),
            ),
        ]);

        let gsps_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, GSPS_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, "9.9.9.10"),
            DataElement::new(
                Tag(0x0070, 0x0001),
                VR::SQ,
                DataSetSequence::from(vec![annotation]),
            ),
        ]);

        let gsps_obj = gsps_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(GSPS_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid("9.9.9.10"),
            )
            .expect("GSPS test object should build file meta");

        let overlays = parse_gsps_overlays(&gsps_obj);
        let overlay = overlays
            .get("1.2.840.1")
            .expect("Overlay should be mapped to referenced SOP instance");
        assert_eq!(overlay.graphics.len(), 1);
        assert_eq!(overlay.graphics[0].referenced_frames, Some(vec![2, 4]));
        assert_eq!(overlay.graphics_for_frame(0).count(), 0);
        assert_eq!(overlay.graphics_for_frame(1).count(), 1);
        assert_eq!(overlay.graphics_for_frame(3).count(), 1);
    }

    #[test]
    fn parse_gsps_overlays_uses_root_reference_when_annotation_reference_missing() {
        let series_item = InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0008, 0x1140),
            VR::SQ,
            DataSetSequence::from(vec![referenced_image_item_with_frames("7.7.7.7", &[3])]),
        )]);
        let annotation = InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0070, 0x0009),
            VR::SQ,
            DataSetSequence::from(vec![graphic_object_item(
                "POLYLINE",
                &[10.0, 10.0, 20.0, 20.0],
                "PIXEL",
                None,
            )]),
        )]);

        let gsps_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, GSPS_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, "8.8.8.8"),
            DataElement::new(
                Tag(0x0008, 0x1115),
                VR::SQ,
                DataSetSequence::from(vec![series_item]),
            ),
            DataElement::new(
                Tag(0x0070, 0x0001),
                VR::SQ,
                DataSetSequence::from(vec![annotation]),
            ),
        ]);

        let gsps_obj = gsps_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(GSPS_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid("8.8.8.8"),
            )
            .expect("GSPS test object should build file meta");

        let overlays = parse_gsps_overlays(&gsps_obj);
        assert!(
            overlays.contains_key("7.7.7.7"),
            "Root-level references should be used when annotation-level references are absent"
        );
        let overlay = overlays
            .get("7.7.7.7")
            .expect("Overlay should exist for root-level referenced SOP instance");
        assert_eq!(overlay.graphics[0].referenced_frames, Some(vec![3]));
    }
}
