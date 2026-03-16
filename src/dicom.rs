use std::borrow::Cow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::{fs, io::Cursor};

use anyhow::{bail, Context, Result};
use dicom_object::{from_reader, open_file, DefaultDicomObject, InMemDicomObject, ReadError, Tag};
use dicom_pixeldata::PixelDecoder;

mod gsps;
mod sr;

#[allow(unused_imports)]
pub use gsps::GspsOverlayGraphic;
pub use gsps::{load_gsps_overlays, GspsGraphic, GspsOverlay, GspsUnits};
pub use sr::{load_structured_report, StructuredReportDocument, StructuredReportNode};

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
    "ContentDate",
    "ContentTime",
    "CompletionFlag",
    "VerificationFlag",
];

pub const GSPS_SOP_CLASS_UID: &str = "1.2.840.10008.5.1.4.1.1.11.1";
pub const STRUCTURED_REPORT_SOP_CLASS_UID_PREFIX: &str = "1.2.840.10008.5.1.4.1.1.88.";
pub const EXPLICIT_VR_LITTLE_ENDIAN_UID: &str = "1.2.840.10008.1.2.1";
const IMPLICIT_VR_LITTLE_ENDIAN_UID: &str = "1.2.840.10008.1.2";
const EXPLICIT_VR_BIG_ENDIAN_UID: &str = "1.2.840.10008.1.2.2";
#[cfg(test)]
pub const BASIC_TEXT_SR_SOP_CLASS_UID: &str = "1.2.840.10008.5.1.4.1.1.88.11";
// Treat cumulative_delta from read_per_frame_image_positions as meaningful only above 0.001 mm so float noise does not flip reverse-order detection.
const IMAGE_POSITION_PATIENT_DOMINANT_DELTA_TOLERANCE_MM: f32 = 0.001;

#[derive(Debug, Clone)]
pub enum DicomSource {
    File(PathBuf),
    Memory {
        id: u64,
        label: Arc<str>,
        identity_key: Arc<str>,
        bytes: Arc<[u8]>,
    },
}

impl DicomSource {
    pub fn from_memory(preferred_name: &str, bytes: Vec<u8>) -> Self {
        let identity_key = infer_memory_source_identity_key(preferred_name, bytes.as_slice());
        Self::from_memory_with_identity(preferred_name, identity_key, bytes)
    }

    pub fn from_memory_with_identity(
        preferred_name: &str,
        identity_key: impl Into<Arc<str>>,
        bytes: Vec<u8>,
    ) -> Self {
        static IN_MEMORY_DICOM_COUNTER: AtomicU64 = AtomicU64::new(1);

        let id = IN_MEMORY_DICOM_COUNTER.fetch_add(1, Ordering::Relaxed);
        let label = Arc::<str>::from(sanitize_memory_source_label(preferred_name));
        let bytes = Arc::<[u8]>::from(bytes.into_boxed_slice());
        let identity_key = identity_key.into();
        Self::Memory {
            id,
            label,
            identity_key,
            bytes,
        }
    }

    pub fn stable_id(&self) -> String {
        match self {
            Self::File(path) => format!("file:{}", path.to_string_lossy()),
            Self::Memory { identity_key, .. } => identity_key.to_string(),
        }
    }

    pub fn short_label(&self) -> Cow<'_, str> {
        match self {
            Self::File(path) => path
                .file_name()
                .and_then(|value| value.to_str())
                .map(Cow::Borrowed)
                .unwrap_or_else(|| Cow::Owned(path.display().to_string())),
            Self::Memory { label, .. } => Cow::Borrowed(label.as_ref()),
        }
    }

    pub fn identity_key(&self) -> Cow<'_, str> {
        match self {
            Self::File(path) => Cow::Owned(format!("file:{}", path.to_string_lossy())),
            Self::Memory { identity_key, .. } => Cow::Borrowed(identity_key.as_ref()),
        }
    }

    pub fn to_meta(&self) -> DicomSourceMeta {
        DicomSourceMeta {
            display_label: Arc::<str>::from(self.short_label().into_owned()),
            identity_key: Arc::<str>::from(self.identity_key().into_owned()),
        }
    }

    fn bytes(&self) -> Option<&Arc<[u8]>> {
        match self {
            Self::File(_) => None,
            Self::Memory { bytes, .. } => Some(bytes),
        }
    }

    fn file_path(&self) -> Option<&Path> {
        match self {
            Self::File(path) => Some(path.as_path()),
            Self::Memory { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DicomSourceMeta {
    display_label: Arc<str>,
    identity_key: Arc<str>,
}

impl DicomSourceMeta {
    pub fn display_label(&self) -> &str {
        self.display_label.as_ref()
    }

    pub fn identity_key(&self) -> &str {
        self.identity_key.as_ref()
    }
}

impl From<DicomSource> for DicomSourceMeta {
    fn from(value: DicomSource) -> Self {
        value.to_meta()
    }
}

impl From<&DicomSource> for DicomSourceMeta {
    fn from(value: &DicomSource) -> Self {
        value.to_meta()
    }
}

impl From<PathBuf> for DicomSourceMeta {
    fn from(value: PathBuf) -> Self {
        DicomSource::from(value).to_meta()
    }
}

impl From<&PathBuf> for DicomSourceMeta {
    fn from(value: &PathBuf) -> Self {
        DicomSource::from(value).to_meta()
    }
}

impl From<&Path> for DicomSourceMeta {
    fn from(value: &Path) -> Self {
        DicomSource::from(value).to_meta()
    }
}

impl From<&DicomSourceMeta> for DicomSourceMeta {
    fn from(value: &DicomSourceMeta) -> Self {
        value.clone()
    }
}

impl PartialEq for DicomSourceMeta {
    fn eq(&self, other: &Self) -> bool {
        self.identity_key == other.identity_key
    }
}

impl Eq for DicomSourceMeta {}

impl Hash for DicomSourceMeta {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identity_key.hash(state);
    }
}

impl From<PathBuf> for DicomSource {
    fn from(value: PathBuf) -> Self {
        Self::File(value)
    }
}

impl From<&PathBuf> for DicomSource {
    fn from(value: &PathBuf) -> Self {
        Self::File(value.clone())
    }
}

impl From<&Path> for DicomSource {
    fn from(value: &Path) -> Self {
        Self::File(value.to_path_buf())
    }
}

impl From<&DicomSource> for DicomSource {
    fn from(value: &DicomSource) -> Self {
        value.clone()
    }
}

impl PartialEq for DicomSource {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::File(left), Self::File(right)) => left == right,
            (Self::Memory { id: left, .. }, Self::Memory { id: right, .. }) => left == right,
            _ => false,
        }
    }
}

impl Eq for DicomSource {}

impl Hash for DicomSource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::File(path) => {
                0u8.hash(state);
                path.hash(state);
            }
            Self::Memory { id, .. } => {
                1u8.hash(state);
                id.hash(state);
            }
        }
    }
}

impl PartialEq<PathBuf> for DicomSource {
    fn eq(&self, other: &PathBuf) -> bool {
        matches!(self, Self::File(path) if path == other)
    }
}

impl PartialEq<DicomSource> for PathBuf {
    fn eq(&self, other: &DicomSource) -> bool {
        other == self
    }
}

impl fmt::Display for DicomSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File(path) => write!(f, "{}", path.display()),
            Self::Memory { id, label, .. } => write!(f, "memory:{label}#{id}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DicomPathKind {
    Image,
    Gsps,
    StructuredReport,
    Other,
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
    reverse_frame_order: bool,
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
    source: DicomSource,
    cache: MonoFrameCache,
    preload_started: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
struct LazyRgbFrames {
    source: DicomSource,
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

    pub(crate) fn display_frame_index_to_stored(&self, frame_index: usize) -> Option<usize> {
        if frame_index >= self.frame_count {
            return None;
        }

        if self.reverse_frame_order {
            Some(
                self.frame_count
                    .saturating_sub(1)
                    .saturating_sub(frame_index),
            )
        } else {
            Some(frame_index)
        }
    }

    pub(crate) fn stored_frame_index_to_display(&self, frame_index: usize) -> Option<usize> {
        if frame_index >= self.frame_count {
            return None;
        }

        if self.reverse_frame_order {
            Some(
                self.frame_count
                    .saturating_sub(1)
                    .saturating_sub(frame_index),
            )
        } else {
            Some(frame_index)
        }
    }

    pub fn frame_mono_pixels(&self, frame_index: usize) -> Option<Arc<[i32]>> {
        let stored_frame_index = self.display_frame_index_to_stored(frame_index)?;
        match &self.mono_frames {
            MonoFrames::None => None,
            MonoFrames::Eager(frames) => frames.get(stored_frame_index).cloned(),
            MonoFrames::Lazy(lazy) => lazy.frame(stored_frame_index),
        }
    }

    pub fn frame_rgb_pixels(&self, frame_index: usize) -> Option<Arc<[u8]>> {
        let stored_frame_index = self.display_frame_index_to_stored(frame_index)?;
        match &self.rgb_frames {
            RgbFrames::None => None,
            RgbFrames::Eager(frames) => frames.get(stored_frame_index).cloned(),
            RgbFrames::Lazy(lazy) => lazy.frame(stored_frame_index),
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
        let source = self.source.clone();
        let cache = Arc::clone(&self.cache);
        let preload_started = Arc::clone(&self.preload_started);
        thread::spawn(move || {
            if let Err(err) = preload_mono_frames_from_source(&source, &cache) {
                preload_started.store(false, Ordering::Relaxed);
                log::warn!("preload_mono_frames_from_source failed for {source}: {err:#}");
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
        let source = self.source.clone();
        let cache = Arc::clone(&self.cache);
        let preload_started = Arc::clone(&self.preload_started);
        thread::spawn(move || {
            if let Err(err) = preload_rgb_frames_from_source(&source, &cache) {
                preload_started.store(false, Ordering::Relaxed);
                log::warn!("preload_rgb_frames_from_source failed for {source}: {err:#}");
            }
        });
    }
}

pub fn is_gsps_sop_class_uid(uid: &str) -> bool {
    uid.trim() == GSPS_SOP_CLASS_UID
}

pub fn is_structured_report_sop_class_uid(uid: &str) -> bool {
    uid.trim()
        .starts_with(STRUCTURED_REPORT_SOP_CLASS_UID_PREFIX)
}

pub fn dicom_source_from_bytes_with_identity(
    preferred_name: &str,
    identity_key: impl Into<Arc<str>>,
    bytes: Vec<u8>,
) -> DicomSource {
    DicomSource::from_memory_with_identity(preferred_name, identity_key, bytes)
}

pub fn classify_dicom_path(source: impl Into<DicomSource>) -> Result<DicomPathKind> {
    let obj = open_dicom_object(source)?;
    Ok(classify_dicom_object(&obj))
}

fn classify_dicom_object(obj: &DefaultDicomObject) -> DicomPathKind {
    let sop_class_uid = read_string(obj, "SOPClassUID");

    if sop_class_uid.as_deref().is_some_and(is_gsps_sop_class_uid) {
        return DicomPathKind::Gsps;
    }
    if sop_class_uid
        .as_deref()
        .is_some_and(is_structured_report_sop_class_uid)
        || read_string(obj, "Modality").is_some_and(|value| value.eq_ignore_ascii_case("SR"))
    {
        return DicomPathKind::StructuredReport;
    }
    if obj.element(Tag(0x7FE0, 0x0010)).is_ok() || obj.element_by_name("PixelData").is_ok() {
        return DicomPathKind::Image;
    }
    DicomPathKind::Other
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
}

fn prime_reverse_frame_cache<T, F>(
    frame_count: usize,
    reverse_frame_order: bool,
    first_frame_pixels: Arc<[T]>,
    decode_initial_display_frame: F,
) -> Result<Vec<Option<Arc<[T]>>>>
where
    F: FnOnce(usize) -> Result<Arc<[T]>>,
{
    let mut cache = vec![None; frame_count];
    cache[0] = Some(Arc::clone(&first_frame_pixels));

    if reverse_frame_order {
        let initial_display_frame = frame_count.saturating_sub(1);
        cache[initial_display_frame] = Some(decode_initial_display_frame(initial_display_frame)?);
    }

    Ok(cache)
}

pub fn load_dicom(source: impl Into<DicomSource>) -> Result<DicomImage> {
    let source = source.into();
    let obj = open_dicom_object(&source)?;
    if classify_dicom_object(&obj) == DicomPathKind::StructuredReport {
        let sop_class = read_string(&obj, "SOPClassUID").unwrap_or_else(|| "unknown".to_string());
        bail!(
            "{} is a Structured Report object (SOPClassUID={}); use load_structured_report() instead",
            source,
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
    let reverse_frame_order = infer_reverse_frame_order(&obj, frame_count);
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

            let first_frame_pixels = Arc::<[i32]>::from(first_frame_pixels.into_boxed_slice());

            let mono_frames = if frame_count == 1 {
                MonoFrames::Eager(vec![first_frame_pixels])
            } else {
                let cache = prime_reverse_frame_cache(
                    frame_count,
                    reverse_frame_order,
                    Arc::clone(&first_frame_pixels),
                    |initial_display_frame| {
                        let decoded_initial_display =
                            obj.decode_pixel_data_frame(initial_display_frame as u32)
                                .with_context(|| {
                                    format!(
                                        "Failed to decode PixelData frame {} for initial reverse-order preview",
                                        initial_display_frame
                                    )
                                })?;
                        if decoded_initial_display.samples_per_pixel() != 1 {
                            bail!(
                                "Initial reverse-order preview expected monochrome pixels, got SamplesPerPixel={}",
                                decoded_initial_display.samples_per_pixel()
                            );
                        }
                        let initial_display_pixels: Vec<i32> = decoded_initial_display
                            .to_vec_frame(0)
                            .with_context(|| {
                                format!(
                                    "Could not convert decoded frame {} to i32 samples for initial reverse-order preview",
                                    initial_display_frame
                                )
                            })?;
                        if initial_display_pixels.len() != width * height {
                            bail!(
                                "Decoded pixel count mismatch in frame {}: got {}, expected {}",
                                initial_display_frame,
                                initial_display_pixels.len(),
                                width * height
                            );
                        }
                        Ok(Arc::<[i32]>::from(initial_display_pixels.into_boxed_slice()))
                    },
                )?;
                MonoFrames::Lazy(LazyMonoFrames {
                    source: source.clone(),
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
                reverse_frame_order,
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

            let first_frame_pixels = Arc::<[u8]>::from(first_frame_pixels.into_boxed_slice());

            let rgb_frames = if frame_count == 1 {
                RgbFrames::Eager(vec![first_frame_pixels])
            } else {
                let cache = prime_reverse_frame_cache(
                    frame_count,
                    reverse_frame_order,
                    Arc::clone(&first_frame_pixels),
                    |initial_display_frame| {
                        let decoded_initial_display =
                            obj.decode_pixel_data_frame(initial_display_frame as u32)
                                .with_context(|| {
                                    format!(
                                        "Failed to decode PixelData frame {} for initial reverse-order preview",
                                        initial_display_frame
                                    )
                                })?;
                        let initial_display_pixels: Vec<u8> = if bits_allocated == 8 {
                            decoded_initial_display.to_vec_frame(0).with_context(|| {
                                format!(
                                    "Could not convert decoded frame {} to u8 samples for initial reverse-order preview",
                                    initial_display_frame
                                )
                            })?
                        } else {
                            let bits_shift =
                                decoded_initial_display.bits_stored().saturating_sub(8);
                            let frame_pixels_u16: Vec<u16> =
                                decoded_initial_display.to_vec_frame(0).with_context(|| {
                                    format!(
                                        "Could not convert decoded frame {} to u16 samples for initial reverse-order preview",
                                        initial_display_frame
                                    )
                                })?;
                            frame_pixels_u16
                                .into_iter()
                                .map(|sample| (sample >> bits_shift) as u8)
                                .collect()
                        };

                        if initial_display_pixels.len() != expected_len {
                            bail!(
                                "Decoded color pixel count mismatch in frame {}: got {}, expected {}",
                                initial_display_frame,
                                initial_display_pixels.len(),
                                expected_len
                            );
                        }
                        Ok(Arc::<[u8]>::from(initial_display_pixels.into_boxed_slice()))
                    },
                )?;
                RgbFrames::Lazy(LazyRgbFrames {
                    source: source.clone(),
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
                reverse_frame_order,
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

fn preload_mono_frames_from_source(source: &DicomSource, cache: &MonoFrameCache) -> Result<()> {
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
        let source = source.clone();
        let cache = Arc::clone(cache);
        workers.push(thread::spawn(move || -> Result<()> {
            let obj = open_dicom_object(&source)?;
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

fn preload_rgb_frames_from_source(source: &DicomSource, cache: &RgbFrameCache) -> Result<()> {
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
        let source = source.clone();
        let cache = Arc::clone(cache);
        workers.push(thread::spawn(move || -> Result<()> {
            let obj = open_dicom_object(&source)?;
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

fn open_dicom_object(source: impl Into<DicomSource>) -> Result<DefaultDicomObject> {
    let source = source.into();
    if let Some(bytes) = source.bytes() {
        return open_dicom_object_from_bytes(bytes.as_ref(), &source.to_string());
    }

    let path = source
        .file_path()
        .ok_or_else(|| anyhow::anyhow!("Could not resolve local file path for {source}"))?;

    match open_file(path) {
        Ok(obj) => Ok(obj),
        Err(err) => {
            let bytes =
                fs::read(path).with_context(|| format!("Could not read {}", path.display()))?;
            try_open_dicom_object_with_repairs(&bytes, &path.display().to_string(), err)
        }
    }
}

fn open_dicom_object_from_bytes(bytes: &[u8], source_label: &str) -> Result<DefaultDicomObject> {
    match from_reader(Cursor::new(bytes)) {
        Ok(obj) => Ok(obj),
        Err(err) => try_open_dicom_object_with_repairs(bytes, source_label, err),
    }
}

fn try_open_dicom_object_with_repairs(
    bytes: &[u8],
    source_label: &str,
    original_error: ReadError,
) -> Result<DefaultDicomObject> {
    let mut repaired_meta = None;

    if is_missing_meta_group_length_error(&original_error) {
        if let Some(repaired) = repair_missing_meta_group_length(bytes) {
            log::debug!(
                "Applying DICOM repair for {source_label}: inserted missing File Meta Information Group Length (0002,0000)"
            );
            if let Ok(obj) = from_reader(Cursor::new(repaired.as_slice())) {
                return Ok(obj);
            }
            repaired_meta = Some(repaired);
        }
    }

    let repair_input = repaired_meta.as_deref().unwrap_or(bytes);
    if let Some(repaired) = repair_private_malformed_binary_vrs_to_un(repair_input) {
        log::debug!(
            "Applying DICOM repair for {source_label}: degraded malformed private explicit-VR binary fields to UN"
        );
        return from_reader(Cursor::new(repaired.as_slice())).with_context(|| {
            format!(
                "Could not open {source_label} after degrading malformed private explicit-VR binary fields to UN"
            )
        });
    }

    Err(original_error).with_context(|| format!("Could not open {source_label}"))
}

fn sanitize_memory_source_label(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();

    if sanitized.is_empty() {
        "dicom".to_string()
    } else {
        sanitized
    }
}

fn infer_memory_source_identity_key(preferred_name: &str, bytes: &[u8]) -> String {
    let Some(identity) = open_dicom_object_from_bytes(bytes, preferred_name)
        .ok()
        .and_then(|obj| dicom_identity_key_from_object(&obj))
    else {
        return fallback_memory_identity_key(preferred_name, bytes);
    };

    identity
}

pub fn dicom_identity_key_from_parts(
    study_uid: Option<&str>,
    series_uid: Option<&str>,
    instance_uid: Option<&str>,
    sop_class_uid: Option<&str>,
    modality: Option<&str>,
) -> String {
    format!(
        "dicom:study={};series={};instance={};class={};modality={}",
        study_uid.unwrap_or("_"),
        series_uid.unwrap_or("_"),
        instance_uid.unwrap_or("_"),
        sop_class_uid.unwrap_or("_"),
        modality.unwrap_or("_"),
    )
}

fn dicom_identity_key_from_object(obj: &DefaultDicomObject) -> Option<String> {
    let study_uid = read_string(obj, "StudyInstanceUID");
    let series_uid = read_string(obj, "SeriesInstanceUID");
    let instance_uid = read_string(obj, "SOPInstanceUID");
    let sop_class_uid = read_string(obj, "SOPClassUID");
    let modality = read_string(obj, "Modality");

    if study_uid.is_none()
        && series_uid.is_none()
        && instance_uid.is_none()
        && sop_class_uid.is_none()
        && modality.is_none()
    {
        return None;
    }

    Some(dicom_identity_key_from_parts(
        study_uid.as_deref(),
        series_uid.as_deref(),
        instance_uid.as_deref(),
        sop_class_uid.as_deref(),
        modality.as_deref(),
    ))
}

fn fallback_memory_identity_key(preferred_name: &str, bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    format!(
        "memory-fallback:{}:{hash:016x}",
        sanitize_memory_source_label(preferred_name)
    )
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

fn repair_private_malformed_binary_vrs_to_un(bytes: &[u8]) -> Option<Vec<u8>> {
    let offset = detect_dicom_prefix_offset(bytes)?;
    if !transfer_syntax_uses_explicit_vr_little_endian(&file_meta_transfer_syntax_uid(
        bytes, offset,
    )?) {
        return None;
    }

    let dataset_start = dataset_start_offset(bytes, offset)?;
    let mut repaired = Vec::with_capacity(bytes.len() + 64);
    repaired.extend_from_slice(&bytes[..dataset_start]);

    let mut position = dataset_start;
    let mut modified = false;
    let mut containers = Vec::new();

    while position < bytes.len() {
        close_defined_containers(&mut containers, position, &mut repaired)?;

        if position + 8 > bytes.len() {
            return None;
        }

        let group = u16::from_le_bytes([bytes[position], bytes[position + 1]]);
        let element = u16::from_le_bytes([bytes[position + 2], bytes[position + 3]]);

        if group == 0xFFFE {
            let value_len = u32::from_le_bytes([
                bytes[position + 4],
                bytes[position + 5],
                bytes[position + 6],
                bytes[position + 7],
            ]);
            let header_end = position.checked_add(8)?;
            repaired.extend_from_slice(&bytes[position..header_end]);
            position = header_end;

            match element {
                0xE000 => {
                    push_item_container(&mut containers, position, repaired.len(), value_len)?;
                }
                0xE00D => {
                    let container = containers.pop()?;
                    if container.kind != ContainerKind::UndefinedItem {
                        return None;
                    }
                }
                0xE0DD => {
                    let container = containers.pop()?;
                    if container.kind != ContainerKind::UndefinedSequence {
                        return None;
                    }
                }
                _ => return None,
            }

            continue;
        }

        if group == 0x7FE0 && element == 0x0010 && containers.is_empty() {
            repaired.extend_from_slice(&bytes[position..]);
            position = bytes.len();
            break;
        }

        let vr = [bytes[position + 4], bytes[position + 5]];
        let (header_len, value_len, undefined_len) =
            read_dataset_explicit_vr_length(bytes, position, vr)?;
        let value_start = position.checked_add(header_len)?;

        if undefined_len {
            if vr != *b"SQ" {
                return None;
            }

            repaired.extend_from_slice(&bytes[position..value_start]);
            containers.push(ContainerState::new_undefined_sequence());
            position = value_start;
            continue;
        }

        let value_end = value_start.checked_add(value_len as usize)?;
        if value_end > bytes.len() {
            return None;
        }

        if vr == *b"SQ" {
            let header_start_out = repaired.len();
            repaired.extend_from_slice(&bytes[position..value_start]);
            let length_field_out = header_start_out.checked_add(header_len)?.checked_sub(4)?;
            containers.push(ContainerState::new_defined_sequence(
                value_end,
                length_field_out,
                repaired.len(),
            ));
            position = value_start;
            continue;
        }

        let value_bytes = &bytes[value_start..value_end];
        if group % 2 == 1 && should_degrade_private_binary_vr_to_un(vr, value_len, value_bytes) {
            append_explicit_vr_un_element(&mut repaired, group, element, value_bytes)?;
            modified = true;
        } else {
            repaired.extend_from_slice(&bytes[position..value_end]);
        }
        position = value_end;
    }

    close_defined_containers(&mut containers, position, &mut repaired)?;
    if !containers.is_empty() || position != bytes.len() {
        return None;
    }

    modified.then_some(repaired)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContainerKind {
    DefinedItem,
    DefinedSequence,
    UndefinedItem,
    UndefinedSequence,
}

#[derive(Clone, Copy, Debug)]
struct ContainerState {
    kind: ContainerKind,
    original_end: Option<usize>,
    length_field_out: Option<usize>,
    value_start_out: usize,
}

impl ContainerState {
    fn new_defined_item(
        original_end: usize,
        length_field_out: usize,
        value_start_out: usize,
    ) -> Self {
        Self {
            kind: ContainerKind::DefinedItem,
            original_end: Some(original_end),
            length_field_out: Some(length_field_out),
            value_start_out,
        }
    }

    fn new_defined_sequence(
        original_end: usize,
        length_field_out: usize,
        value_start_out: usize,
    ) -> Self {
        Self {
            kind: ContainerKind::DefinedSequence,
            original_end: Some(original_end),
            length_field_out: Some(length_field_out),
            value_start_out,
        }
    }

    fn new_undefined_sequence() -> Self {
        Self {
            kind: ContainerKind::UndefinedSequence,
            original_end: None,
            length_field_out: None,
            value_start_out: 0,
        }
    }
}

fn push_item_container(
    containers: &mut Vec<ContainerState>,
    value_start_in: usize,
    value_start_out: usize,
    value_len: u32,
) -> Option<()> {
    if value_len == u32::MAX {
        containers.push(ContainerState {
            kind: ContainerKind::UndefinedItem,
            original_end: None,
            length_field_out: None,
            value_start_out: 0,
        });
        return Some(());
    }

    let original_end = value_start_in.checked_add(value_len as usize)?;
    let length_field_out = value_start_out.checked_sub(4)?;
    containers.push(ContainerState::new_defined_item(
        original_end,
        length_field_out,
        value_start_out,
    ));
    Some(())
}

fn close_defined_containers(
    containers: &mut Vec<ContainerState>,
    position: usize,
    repaired: &mut [u8],
) -> Option<()> {
    while containers
        .last()
        .is_some_and(|container| container.original_end == Some(position))
    {
        let container = containers.pop()?;
        let length_field_out = container.length_field_out?;
        let value_len = repaired.len().checked_sub(container.value_start_out)?;
        let value_len = u32::try_from(value_len).ok()?;
        repaired
            .get_mut(length_field_out..length_field_out + 4)?
            .copy_from_slice(&value_len.to_le_bytes());
    }
    Some(())
}

fn should_degrade_private_binary_vr_to_un(vr: [u8; 2], value_len: u32, value_bytes: &[u8]) -> bool {
    match vr {
        [b'F', b'D'] => value_len % 8 != 0 && bytes_look_like_dicom_numeric_text(value_bytes),
        [b'F', b'L'] => value_len % 4 != 0 && bytes_look_like_dicom_numeric_text(value_bytes),
        [b'S', b'S'] | [b'U', b'S'] | [b'O', b'W'] => value_len % 2 != 0,
        [b'S', b'L'] | [b'U', b'L'] | [b'O', b'L'] | [b'O', b'F'] | [b'A', b'T'] => {
            value_len % 4 != 0
        }
        [b'S', b'V'] | [b'U', b'V'] | [b'O', b'D'] | [b'O', b'V'] => value_len % 8 != 0,
        _ => false,
    }
}

fn append_explicit_vr_un_element(
    repaired: &mut Vec<u8>,
    group: u16,
    element: u16,
    value_bytes: &[u8],
) -> Option<()> {
    let value_len = u32::try_from(value_bytes.len()).ok()?;
    repaired.extend_from_slice(&group.to_le_bytes());
    repaired.extend_from_slice(&element.to_le_bytes());
    repaired.extend_from_slice(b"UN");
    repaired.extend_from_slice(&0u16.to_le_bytes());
    repaired.extend_from_slice(&value_len.to_le_bytes());
    repaired.extend_from_slice(value_bytes);
    Some(())
}

fn file_meta_transfer_syntax_uid(bytes: &[u8], meta_offset: usize) -> Option<String> {
    if bytes.len() < meta_offset + 8 {
        return None;
    }

    let mut position = meta_offset;
    while position + 8 <= bytes.len() {
        let group = u16::from_le_bytes([bytes[position], bytes[position + 1]]);
        let element = u16::from_le_bytes([bytes[position + 2], bytes[position + 3]]);
        if group != 0x0002 {
            break;
        }

        let vr = [bytes[position + 4], bytes[position + 5]];
        let (header_len, value_len) = read_explicit_vr_element_length(bytes, position, vr)?;
        let value_start = position.checked_add(header_len)?;
        let value_end = value_start.checked_add(value_len as usize)?;
        if value_end > bytes.len() {
            return None;
        }

        if element == 0x0010 {
            if vr != *b"UI" {
                return None;
            }
            return dicom_text_bytes(&bytes[value_start..value_end]);
        }

        position = value_end;
    }

    None
}

fn transfer_syntax_uses_explicit_vr_little_endian(uid: &str) -> bool {
    uid != IMPLICIT_VR_LITTLE_ENDIAN_UID
        && uid != EXPLICIT_VR_BIG_ENDIAN_UID
        && (uid == EXPLICIT_VR_LITTLE_ENDIAN_UID
            || uid.starts_with("1.2.840.10008.1.2.1.")
            || uid == "1.2.840.10008.1.2.5"
            || uid.starts_with("1.2.840.10008.1.2.4."))
}

fn dicom_text_bytes(bytes: &[u8]) -> Option<String> {
    let mut end = bytes.len();
    while end > 0 && matches!(bytes[end - 1], b' ' | b'\0') {
        end -= 1;
    }
    std::str::from_utf8(&bytes[..end]).ok().map(str::to_string)
}

fn dataset_start_offset(bytes: &[u8], meta_offset: usize) -> Option<usize> {
    if bytes.len() < meta_offset + 12 {
        return None;
    }

    let first_group = u16::from_le_bytes([bytes[meta_offset], bytes[meta_offset + 1]]);
    let first_element = u16::from_le_bytes([bytes[meta_offset + 2], bytes[meta_offset + 3]]);
    if first_group != 0x0002 {
        return Some(meta_offset);
    }

    if first_element == 0x0000 {
        let vr = [bytes[meta_offset + 4], bytes[meta_offset + 5]];
        let (header_len, value_len) = read_explicit_vr_element_length(bytes, meta_offset, vr)?;
        if vr != *b"UL" || header_len != 8 || value_len != 4 {
            return None;
        }

        let meta_group_len = u32::from_le_bytes([
            bytes[meta_offset + 8],
            bytes[meta_offset + 9],
            bytes[meta_offset + 10],
            bytes[meta_offset + 11],
        ]);
        return meta_offset
            .checked_add(header_len)?
            .checked_add(value_len as usize)?
            .checked_add(meta_group_len as usize);
    }

    scan_meta_group_len_without_group_length(bytes, meta_offset)?.checked_add(meta_offset)
}

fn vr_uses_u32_length(vr: [u8; 2]) -> bool {
    matches!(
        vr,
        [b'O', b'B']
            | [b'O', b'D']
            | [b'O', b'F']
            | [b'O', b'L']
            | [b'O', b'V']
            | [b'O', b'W']
            | [b'S', b'Q']
            | [b'U', b'C']
            | [b'U', b'R']
            | [b'U', b'T']
            | [b'U', b'N']
    )
}

fn read_dataset_explicit_vr_length(
    bytes: &[u8],
    position: usize,
    vr: [u8; 2],
) -> Option<(usize, u32, bool)> {
    if vr_uses_u32_length(vr) {
        if position + 12 > bytes.len() {
            return None;
        }
        let value_len = u32::from_le_bytes([
            bytes[position + 8],
            bytes[position + 9],
            bytes[position + 10],
            bytes[position + 11],
        ]);
        Some((12, value_len, value_len == u32::MAX))
    } else {
        let value_len = u16::from_le_bytes([bytes[position + 6], bytes[position + 7]]) as u32;
        Some((8, value_len, false))
    }
}

fn bytes_look_like_dicom_numeric_text(bytes: &[u8]) -> bool {
    !bytes.is_empty()
        && bytes.iter().all(|byte| {
            matches!(
                byte,
                b'0'..=b'9' | b' ' | b'+' | b'-' | b'.' | b'e' | b'E' | b'\\'
            )
        })
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
    if vr_uses_u32_length(vr) {
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

fn read_per_frame_image_positions(obj: &DefaultDicomObject) -> Vec<[f32; 3]> {
    const PER_FRAME_FUNCTIONAL_GROUPS_SEQUENCE: Tag = Tag(0x5200, 0x9230);
    const PLANE_POSITION_SEQUENCE: Tag = Tag(0x0020, 0x9113);
    const IMAGE_POSITION_PATIENT: Tag = Tag(0x0020, 0x0032);

    sequence_items_from_object(obj, PER_FRAME_FUNCTIONAL_GROUPS_SEQUENCE)
        .into_iter()
        .flatten()
        .filter_map(|frame_item| {
            let plane_position_item =
                sequence_items_from_item(frame_item, PLANE_POSITION_SEQUENCE)?.first()?;
            let values = read_item_multi_float(plane_position_item, IMAGE_POSITION_PATIENT)?;
            if values.len() < 3 {
                return None;
            }
            Some([values[0], values[1], values[2]])
        })
        .collect()
}

fn infer_reverse_frame_order(obj: &DefaultDicomObject, frame_count: usize) -> bool {
    if frame_count <= 1 {
        return false;
    }

    let frame_positions = read_per_frame_image_positions(obj);
    if frame_positions.len() != frame_count {
        return false;
    }

    let mut cumulative_delta = [0.0_f32; 3];
    for frame_pair in frame_positions.windows(2) {
        let [previous, current] = frame_pair else {
            continue;
        };
        for axis in 0..3 {
            cumulative_delta[axis] += current[axis] - previous[axis];
        }
    }

    let (_, dominant_delta) = cumulative_delta
        .into_iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| left.abs().total_cmp(&right.abs()))
        .unwrap_or((0, 0.0));

    dominant_delta > IMAGE_POSITION_PATIENT_DOMINANT_DELTA_TOLERANCE_MM
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
        Self::test_stub_with_mono_frames(gsps_overlay, 0)
    }

    pub(crate) fn test_stub_with_mono_frames(
        gsps_overlay: Option<GspsOverlay>,
        frame_count: usize,
    ) -> Self {
        Self::test_stub_with_mono_frames_and_reverse(gsps_overlay, frame_count, false)
    }

    pub(crate) fn test_stub_with_mono_frames_and_reverse(
        gsps_overlay: Option<GspsOverlay>,
        frame_count: usize,
        reverse_frame_order: bool,
    ) -> Self {
        let mono_frames = if frame_count == 0 {
            MonoFrames::None
        } else {
            let frames = (0..frame_count)
                .map(|frame_index| Arc::<[i32]>::from([frame_index as i32]))
                .collect();
            MonoFrames::Eager(frames)
        };

        Self {
            width: 1,
            height: 1,
            mono_frames,
            rgb_frames: RgbFrames::None,
            frame_count,
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
            reverse_frame_order,
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

    fn multiframe_position_item(image_position_patient: &str) -> InMemDicomObject {
        let plane_position = InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0020, 0x0032),
            VR::DS,
            image_position_patient,
        )]);
        InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0020, 0x9113),
            VR::SQ,
            DataSetSequence::from(vec![plane_position]),
        )])
    }

    fn multiframe_position_test_object_from_items(
        frame_items: Vec<InMemDicomObject>,
        frame_count: usize,
    ) -> DefaultDicomObject {
        let object = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, "1.2.840.10008.5.1.4.1.1.4.1"),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "MG"),
            DataElement::new(Tag(0x0028, 0x0008), VR::IS, frame_count.to_string()),
            DataElement::new(
                Tag(0x5200, 0x9230),
                VR::SQ,
                DataSetSequence::from(frame_items),
            ),
        ])
        .with_meta(
            FileMetaTableBuilder::new()
                .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                .media_storage_sop_class_uid("1.2.840.10008.5.1.4.1.1.4.1")
                .media_storage_sop_instance_uid("4.3.2.10"),
        )
        .expect("multi-frame test object should build file meta");

        let mut bytes = Vec::new();
        object
            .write_all(&mut bytes)
            .expect("multi-frame test object should serialize");
        open_dicom_object_from_bytes(&bytes, "multiframe-position-test")
            .expect("serialized multi-frame test object should parse")
    }

    fn multiframe_position_test_object(frame_positions: &[&str]) -> DefaultDicomObject {
        multiframe_position_test_object_from_items(
            frame_positions
                .iter()
                .map(|position| multiframe_position_item(position))
                .collect(),
            frame_positions.len(),
        )
    }

    fn multiframe_mono_test_bytes(frame_positions: &[&str], pixel_values: &[u8]) -> Vec<u8> {
        assert_eq!(
            frame_positions.len(),
            pixel_values.len(),
            "test frames and pixel values should align"
        );

        let object = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, "1.2.840.10008.5.1.4.1.1.4.1"),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "MG"),
            DataElement::new(Tag(0x0028, 0x0002), VR::US, PrimitiveValue::from(1u16)),
            DataElement::new(Tag(0x0028, 0x0004), VR::CS, "MONOCHROME2"),
            DataElement::new(
                Tag(0x0028, 0x0008),
                VR::IS,
                frame_positions.len().to_string(),
            ),
            DataElement::new(Tag(0x0028, 0x0010), VR::US, PrimitiveValue::from(1u16)),
            DataElement::new(Tag(0x0028, 0x0011), VR::US, PrimitiveValue::from(1u16)),
            DataElement::new(Tag(0x0028, 0x0100), VR::US, PrimitiveValue::from(8u16)),
            DataElement::new(Tag(0x0028, 0x0101), VR::US, PrimitiveValue::from(8u16)),
            DataElement::new(Tag(0x0028, 0x0102), VR::US, PrimitiveValue::from(7u16)),
            DataElement::new(Tag(0x0028, 0x0103), VR::US, PrimitiveValue::from(0u16)),
            DataElement::new(
                Tag(0x5200, 0x9230),
                VR::SQ,
                DataSetSequence::from(
                    frame_positions
                        .iter()
                        .map(|position| multiframe_position_item(position))
                        .collect::<Vec<_>>(),
                ),
            ),
            DataElement::new(
                Tag(0x7FE0, 0x0010),
                VR::OB,
                PrimitiveValue::from(pixel_values.to_vec()),
            ),
        ])
        .with_meta(
            FileMetaTableBuilder::new()
                .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                .media_storage_sop_class_uid("1.2.840.10008.5.1.4.1.1.4.1")
                .media_storage_sop_instance_uid("4.3.2.11"),
        )
        .expect("multi-frame pixel test object should build file meta");

        let mut bytes = Vec::new();
        object
            .write_all(&mut bytes)
            .expect("multi-frame pixel test object should serialize");
        bytes
    }

    fn unique_test_file_path(prefix: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after UNIX_EPOCH")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "perspecta-{prefix}-{}-{nanos}.dcm",
            std::process::id()
        ))
    }

    fn sr_test_bytes(instance_uid: &str) -> Vec<u8> {
        let sr_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, BASIC_TEXT_SR_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "SR"),
        ]);

        let sr_obj = sr_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(BASIC_TEXT_SR_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid(instance_uid),
            )
            .expect("SR test object should build file meta");

        let mut bytes = Vec::new();
        sr_obj
            .write_all(&mut bytes)
            .expect("SR test object should write to memory");
        bytes
    }

    fn private_text_test_bytes(value: &str, transfer_syntax_uid: &str) -> Vec<u8> {
        let dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, "1.2.840.10008.5.1.4.1.1.1"),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, "9.99.123456.1"),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "OT"),
            DataElement::new(Tag(0x0011, 0x0010), VR::LO, "FAKE_CREATOR"),
            DataElement::new(Tag(0x0011, 0x1011), VR::LO, value),
        ]);
        let file_obj = dataset
            .with_meta(FileMetaTableBuilder::new().transfer_syntax(transfer_syntax_uid))
            .expect("test object should build file meta");

        let mut bytes = Vec::new();
        file_obj
            .write_all(&mut bytes)
            .expect("test object should serialize");
        bytes
    }

    fn replace_explicit_vr(bytes: &mut [u8], marker: &[u8], new_vr: [u8; 2]) -> usize {
        let marker_offset = bytes
            .windows(marker.len())
            .position(|window| window == marker)
            .expect("serialized test object should contain target marker");
        bytes[marker_offset + 4..marker_offset + 6].copy_from_slice(&new_vr);
        marker_offset
    }

    fn code_item(code_meaning: &str) -> InMemDicomObject {
        InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0008, 0x0104),
            VR::LO,
            code_meaning,
        )])
    }

    fn numeric_measurement_item(value: &str, units: &str) -> InMemDicomObject {
        InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0040, 0xA30A), VR::DS, value),
            DataElement::new(
                Tag(0x0040, 0x08EA),
                VR::SQ,
                DataSetSequence::from(vec![code_item(units)]),
            ),
        ])
    }

    fn sr_content_item(
        value_type: &str,
        relationship_type: Option<&str>,
        concept_name: Option<&str>,
        value: Option<DataElement<InMemDicomObject>>,
        children: Vec<InMemDicomObject>,
    ) -> InMemDicomObject {
        let mut item = InMemDicomObject::new_empty();
        item.put(DataElement::new(Tag(0x0040, 0xA040), VR::CS, value_type));
        if let Some(relationship_type) = relationship_type {
            item.put(DataElement::new(
                Tag(0x0040, 0xA010),
                VR::CS,
                relationship_type,
            ));
        }
        if let Some(concept_name) = concept_name {
            item.put(DataElement::new(
                Tag(0x0040, 0xA043),
                VR::SQ,
                DataSetSequence::from(vec![code_item(concept_name)]),
            ));
        }
        if let Some(value) = value {
            item.put(value);
        }
        if !children.is_empty() {
            item.put(DataElement::new(
                Tag(0x0040, 0xA730),
                VR::SQ,
                DataSetSequence::from(children),
            ));
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
    fn repair_is_noop_when_group_length_is_already_correct() {
        let mut bytes = vec![0u8; 128];
        bytes.extend_from_slice(b"DICM");
        bytes.extend_from_slice(&[
            0x02, 0x00, 0x00, 0x00, b'U', b'L', 0x04, 0x00, 0x1c, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x10, 0x00, b'U', b'I', 0x14, 0x00, b'T', b'R', b'A', b'N', b'S', b'F', b'E', b'R',
            b'-', b'S', b'Y', b'N', b'T', b'A', b'X', b'-', b'T', b'E', b'S', b'T', 0x08, 0x00,
            0x16, 0x00, b'U', b'I', 0x02, 0x00, b'1', 0x00,
        ]);

        assert!(repair_missing_meta_group_length(&bytes).is_none());
    }

    #[test]
    fn open_dicom_object_degrades_private_text_value_mislabelled_as_fd_to_un() {
        let mut bytes = private_text_test_bytes("123.45", EXPLICIT_VR_LITTLE_ENDIAN_UID);
        let marker = [
            0x11, 0x00, 0x11, 0x10, b'L', b'O', 0x06, 0x00, b'1', b'2', b'3', b'.', b'4', b'5',
        ];
        replace_explicit_vr(&mut bytes, &marker, *b"FD");

        let obj = open_dicom_object_from_bytes(&bytes, "mislabelled-private-fd")
            .expect("mislabelled private FD should be degraded to UN");

        assert_eq!(read_string(&obj, "Modality").as_deref(), Some("OT"));
        let element = obj
            .element(Tag(0x0011, 0x1011))
            .expect("degraded private tag should exist");
        assert_eq!(element.vr(), VR::UN);
        assert_eq!(
            element
                .value()
                .to_bytes()
                .expect("UN value should preserve raw bytes")
                .as_ref(),
            b"123.45"
        );
    }

    #[test]
    fn repair_degrades_private_text_value_mislabelled_as_fl_to_un() {
        let mut bytes = private_text_test_bytes("123.45", EXPLICIT_VR_LITTLE_ENDIAN_UID);
        let marker = [
            0x11, 0x00, 0x11, 0x10, b'L', b'O', 0x06, 0x00, b'1', b'2', b'3', b'.', b'4', b'5',
        ];
        replace_explicit_vr(&mut bytes, &marker, *b"FL");

        let repaired = repair_private_malformed_binary_vrs_to_un(&bytes)
            .expect("mislabelled private FL should be degraded to UN");
        let obj = from_reader(Cursor::new(repaired.as_slice()))
            .expect("repaired private FL should parse");
        let element = obj
            .element(Tag(0x0011, 0x1011))
            .expect("degraded private FL tag should exist");
        assert_eq!(element.vr(), VR::UN);
        assert_eq!(
            element
                .value()
                .to_bytes()
                .expect("UN value should preserve raw bytes")
                .as_ref(),
            b"123.45"
        );
    }

    #[test]
    fn repair_degrades_private_binary_vr_with_invalid_length_to_un() {
        let mut bytes = private_text_test_bytes("123456", EXPLICIT_VR_LITTLE_ENDIAN_UID);
        let marker = [
            0x11, 0x00, 0x11, 0x10, b'L', b'O', 0x06, 0x00, b'1', b'2', b'3', b'4', b'5', b'6',
        ];
        replace_explicit_vr(&mut bytes, &marker, *b"UL");

        let repaired = repair_private_malformed_binary_vrs_to_un(&bytes)
            .expect("private UL with invalid length should be degraded to UN");
        let obj = from_reader(Cursor::new(repaired.as_slice()))
            .expect("repaired private UL should parse");
        let element = obj
            .element(Tag(0x0011, 0x1011))
            .expect("degraded private UL tag should exist");
        assert_eq!(element.vr(), VR::UN);
        assert_eq!(
            element
                .value()
                .to_bytes()
                .expect("UN value should preserve raw bytes")
                .as_ref(),
            b"123456"
        );
    }

    #[test]
    fn repair_ignores_valid_private_fd() {
        let dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, "1.2.840.10008.5.1.4.1.1.1"),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, "1.2.3.4.5"),
            DataElement::new(Tag(0x0011, 0x0010), VR::LO, "TEST_CREATOR"),
            DataElement::new(Tag(0x0011, 0x1011), VR::FD, PrimitiveValue::from(123.45f64)),
        ]);
        let file_obj = dataset
            .with_meta(FileMetaTableBuilder::new().transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID))
            .expect("valid private FD test object should build file meta");

        let mut bytes = Vec::new();
        file_obj
            .write_all(&mut bytes)
            .expect("valid private FD test object should serialize");

        assert!(repair_private_malformed_binary_vrs_to_un(&bytes).is_none());
    }

    #[test]
    fn repair_ignores_public_text_value_mislabelled_as_fd() {
        let dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, "1.2.840.10008.5.1.4.1.1.1"),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, "1.2.3.4.5"),
            DataElement::new(Tag(0x0008, 0x103E), VR::LO, "123.45"),
        ]);
        let file_obj = dataset
            .with_meta(FileMetaTableBuilder::new().transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID))
            .expect("public FD test object should build file meta");

        let mut bytes = Vec::new();
        file_obj
            .write_all(&mut bytes)
            .expect("public FD test object should serialize");

        let marker = [
            0x08, 0x00, 0x3E, 0x10, b'L', b'O', 0x06, 0x00, b'1', b'2', b'3', b'.', b'4', b'5',
        ];
        replace_explicit_vr(&mut bytes, &marker, *b"FD");

        assert!(repair_private_malformed_binary_vrs_to_un(&bytes).is_none());
    }

    #[test]
    fn repair_ignores_non_explicit_vr_little_endian_sources() {
        let bytes = private_text_test_bytes("123.45", "1.2.840.10008.1.2");
        assert!(repair_private_malformed_binary_vrs_to_un(&bytes).is_none());
    }

    #[test]
    fn repair_accepts_encapsulated_explicit_vr_little_endian_transfer_syntax() {
        let mut bytes = private_text_test_bytes("123.45", "1.2.840.10008.1.2.4.50");
        let marker = [
            0x11, 0x00, 0x11, 0x10, b'L', b'O', 0x06, 0x00, b'1', b'2', b'3', b'.', b'4', b'5',
        ];
        replace_explicit_vr(&mut bytes, &marker, *b"FD");

        let repaired = repair_private_malformed_binary_vrs_to_un(&bytes)
            .expect("encapsulated explicit VR LE syntax should still allow UN degradation");
        let obj = from_reader(Cursor::new(repaired.as_slice()))
            .expect("repaired encapsulated explicit VR LE test object should parse");
        let element = obj
            .element(Tag(0x0011, 0x1011))
            .expect("degraded private FD tag should exist");
        assert_eq!(element.vr(), VR::UN);
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
    fn classify_dicom_object_recognizes_structured_reports() {
        let sr_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, BASIC_TEXT_SR_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "SR"),
        ]);

        let sr_obj = sr_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(BASIC_TEXT_SR_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid("4.3.2.1"),
            )
            .expect("SR test object should build file meta");

        assert_eq!(
            classify_dicom_object(&sr_obj),
            DicomPathKind::StructuredReport
        );
    }

    #[test]
    fn load_dicom_rejects_structured_reports_with_clear_guidance() {
        let sr_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, BASIC_TEXT_SR_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "SR"),
        ]);

        let sr_obj = sr_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(BASIC_TEXT_SR_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid("4.3.2.3"),
            )
            .expect("SR test object should build file meta");

        let path = unique_test_file_path("load-dicom-rejects-sr");
        sr_obj
            .write_to_file(&path)
            .expect("SR test object should write to disk");

        let err = load_dicom(&path).expect_err("load_dicom should reject structured reports");
        let _ = std::fs::remove_file(&path);

        let message = format!("{err:#}");
        assert!(message.contains("Structured Report object"));
        assert!(message.contains(BASIC_TEXT_SR_SOP_CLASS_UID));
        assert!(message.contains("load_structured_report()"));
    }

    #[test]
    fn dicom_source_from_memory_roundtrip_opens_object() {
        let bytes = sr_test_bytes("4.3.2.4");
        let virtual_path = DicomSource::from_memory("report 4.3.2.4", bytes);

        assert!(matches!(virtual_path, DicomSource::Memory { .. }));
        assert_eq!(
            classify_dicom_path(&virtual_path).expect("virtual path should classify"),
            DicomPathKind::StructuredReport
        );
    }

    #[test]
    fn parse_structured_report_document_extracts_title_and_content() {
        let findings = sr_content_item(
            "TEXT",
            Some("CONTAINS"),
            Some("Findings"),
            Some(DataElement::new(
                Tag(0x0040, 0xA160),
                VR::UT,
                "No acute cardiopulmonary abnormality.",
            )),
            Vec::new(),
        );
        let heart_rate = sr_content_item(
            "NUM",
            Some("CONTAINS"),
            Some("Heart Rate"),
            Some(DataElement::new(
                Tag(0x0040, 0xA300),
                VR::SQ,
                DataSetSequence::from(vec![numeric_measurement_item("72", "bpm")]),
            )),
            Vec::new(),
        );
        let root = sr_content_item(
            "CONTAINER",
            None,
            Some("Impression"),
            None,
            vec![findings, heart_rate],
        );

        let sr_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, BASIC_TEXT_SR_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "SR"),
            DataElement::new(Tag(0x0008, 0x103E), VR::LO, "Chest SR"),
            DataElement::new(Tag(0x0040, 0xA491), VR::CS, "COMPLETE"),
            DataElement::new(Tag(0x0040, 0xA493), VR::CS, "UNVERIFIED"),
            DataElement::new(
                Tag(0x0040, 0xA730),
                VR::SQ,
                DataSetSequence::from(vec![root]),
            ),
        ]);

        let sr_obj = sr_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(BASIC_TEXT_SR_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid("4.3.2.2"),
            )
            .expect("SR test object should build file meta");

        let report = sr::parse_structured_report_document(&sr_obj);
        assert_eq!(report.title, "Impression");
        assert_eq!(report.modality.as_deref(), Some("SR"));
        assert_eq!(report.completion_flag.as_deref(), Some("COMPLETE"));
        assert_eq!(report.verification_flag.as_deref(), Some("UNVERIFIED"));
        assert_eq!(report.content.len(), 2);
        assert_eq!(report.content[0].label, "Findings");
        assert_eq!(
            report.content[0].value.as_deref(),
            Some("No acute cardiopulmonary abnormality.")
        );
        assert_eq!(report.content[1].label, "Heart Rate");
        assert_eq!(report.content[1].value.as_deref(), Some("72 bpm"));
    }

    #[test]
    fn infer_reverse_frame_order_when_patient_positions_ascend() {
        let descending = multiframe_position_test_object(&["0\\0\\3", "0\\0\\2", "0\\0\\1"]);
        let ascending = multiframe_position_test_object(&["0\\0\\1", "0\\0\\2", "0\\0\\3"]);

        assert_eq!(
            read_per_frame_image_positions(&descending),
            vec![[0.0, 0.0, 3.0], [0.0, 0.0, 2.0], [0.0, 0.0, 1.0]]
        );
        assert!(!infer_reverse_frame_order(&descending, 3));
        assert!(infer_reverse_frame_order(&ascending, 3));
    }

    #[test]
    fn infer_reverse_frame_order_requires_positions_for_every_frame() {
        let partial = multiframe_position_test_object_from_items(
            vec![
                multiframe_position_item("0\\0\\1"),
                InMemDicomObject::new_empty(),
                multiframe_position_item("0\\0\\3"),
            ],
            3,
        );

        assert_eq!(
            read_per_frame_image_positions(&partial),
            vec![[0.0, 0.0, 1.0], [0.0, 0.0, 3.0]]
        );
        assert!(!infer_reverse_frame_order(&partial, 3));
    }

    #[test]
    fn frame_pixel_access_uses_display_order_when_reversed() {
        let image = DicomImage::test_stub_with_mono_frames_and_reverse(None, 4, true);

        assert_eq!(image.frame_mono_pixels(0).as_deref(), Some([3].as_slice()));
        assert_eq!(image.frame_mono_pixels(1).as_deref(), Some([2].as_slice()));
        assert_eq!(image.frame_mono_pixels(3).as_deref(), Some([0].as_slice()));
        assert_eq!(image.stored_frame_index_to_display(0), Some(3));
        assert_eq!(image.stored_frame_index_to_display(3), Some(0));
    }

    #[test]
    fn frame_index_mapping_rejects_out_of_range_inputs() {
        let image = DicomImage::test_stub_with_mono_frames_and_reverse(None, 4, true);

        assert_eq!(image.display_frame_index_to_stored(4), None);
        assert_eq!(image.stored_frame_index_to_display(4), None);
        assert_eq!(image.frame_mono_pixels(4), None);
    }

    #[test]
    fn load_dicom_primes_initial_display_frame_for_reversed_multiframe_images() {
        let bytes = multiframe_mono_test_bytes(&["0\\0\\1", "0\\0\\2", "0\\0\\3"], &[11, 22, 33]);
        let source = DicomSource::from_memory("reverse-multiframe.dcm", bytes);

        let image = load_dicom(source).expect("reverse multiframe test object should load");

        assert_eq!(image.frame_count(), 3);
        assert_eq!(image.frame_mono_pixels(1).as_deref(), None);
        assert_eq!(image.frame_mono_pixels(0).as_deref(), Some([33].as_slice()));
        assert_eq!(image.frame_mono_pixels(2).as_deref(), Some([11].as_slice()));
    }
}
