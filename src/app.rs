use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use eframe::egui::{
    self, ColorImage, ResizeDirection, Sense, TextureHandle, TextureOptions, ViewportCommand,
};

use crate::dicom::{
    classify_dicom_path, load_dicom, load_gsps_overlays, load_structured_report, DicomImage,
    DicomPathKind, GspsGraphic, GspsOverlay, GspsUnits, StructuredReportDocument,
    StructuredReportNode, METADATA_FIELD_NAMES,
};
use crate::dicomweb::{
    download_dicomweb_group_request, download_dicomweb_request, DicomWebDownloadResult,
    DicomWebGroupStreamUpdate,
};
use crate::launch::{DicomWebGroupedLaunchRequest, DicomWebLaunchRequest, LaunchRequest};
use crate::mammo::{mammo_image_align, mammo_label, order_mammo_indices, preferred_mammo_slot};
use crate::renderer::{render_rgb, render_window_level};

const APP_TITLE: &str = "Perspecta Viewer";
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const HISTORY_MAX_ENTRIES: usize = 24;
const HISTORY_THUMB_MAX_DIM: usize = 96;
const HISTORY_LIST_THUMB_MAX_DIM: f32 = 56.0;
const DEFAULT_CINE_FPS: f32 = 24.0;
const VALID_GROUP_SIZES: &[usize] = &[1, 2, 3, 4, 8];
const PERSPECTA_BRAND_BLUE: egui::Color32 = egui::Color32::from_rgb(14, 165, 233);
const CONTROL_VALUE_WIDTH: f32 = 64.0;
const CONTROL_ACTION_BUTTON_WIDTH: f32 = 100.0;
const FILE_DROP_OVERLAY_WIDTH: f32 = 420.0;

#[derive(Clone)]
struct MammoViewport {
    path: PathBuf,
    image: DicomImage,
    texture: TextureHandle,
    label: String,
    window_center: f32,
    window_width: f32,
    current_frame: usize,
    zoom: f32,
    pan: egui::Vec2,
    frame_scroll_accum: f32,
}

struct PendingLoad {
    path: PathBuf,
    image: DicomImage,
}

enum PendingSingleLoad {
    Image(PendingLoad),
    StructuredReport {
        path: PathBuf,
        report: StructuredReportDocument,
    },
}

#[derive(Default, Clone)]
struct PreparedLoadPaths {
    image_paths: Vec<PathBuf>,
    structured_report_paths: Vec<PathBuf>,
    gsps_overlays: HashMap<String, GspsOverlay>,
    gsps_files_found: usize,
    other_files_found: usize,
}

enum HistoryPreloadResult {
    Single {
        path: PathBuf,
        image: Box<DicomImage>,
    },
    Group {
        viewports: Vec<(PathBuf, DicomImage)>,
    },
    Report {
        path: PathBuf,
        report: Box<StructuredReportDocument>,
    },
}

#[derive(Clone)]
struct HistorySingleData {
    path: PathBuf,
    image: DicomImage,
    texture: TextureHandle,
    window_center: f32,
    window_width: f32,
    current_frame: usize,
    cine_fps: f32,
}

#[derive(Clone)]
struct HistoryGroupViewportData {
    path: PathBuf,
    image: DicomImage,
    texture: TextureHandle,
    label: String,
    window_center: f32,
    window_width: f32,
    current_frame: usize,
}

#[derive(Clone)]
struct HistoryGroupData {
    viewports: Vec<HistoryGroupViewportData>,
    selected_index: usize,
}

#[derive(Clone)]
struct HistoryReportData {
    path: PathBuf,
    report: StructuredReportDocument,
}

#[derive(Clone)]
enum HistoryKind {
    Single(Box<HistorySingleData>),
    Group(HistoryGroupData),
    Report(Box<HistoryReportData>),
}

struct HistoryThumb {
    texture: TextureHandle,
}

struct HistoryEntry {
    id: String,
    kind: HistoryKind,
    thumbs: Vec<HistoryThumb>,
}

struct ActiveViewportState {
    is_single: bool,
    is_monochrome: bool,
    min_value: i32,
    max_value: i32,
    frame_count: usize,
    default_center: f32,
    default_width: f32,
    window_center: f32,
    window_width: f32,
    current_frame: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct GspsNavigationTarget {
    viewport_index: usize,
    frame_index: usize,
}

pub struct DicomViewerApp {
    image: Option<DicomImage>,
    report: Option<StructuredReportDocument>,
    current_single_path: Option<PathBuf>,
    texture: Option<TextureHandle>,
    mammo_group: Vec<Option<MammoViewport>>,
    mammo_selected_index: usize,
    history_entries: Vec<HistoryEntry>,
    visible_metadata_fields: HashSet<String>,
    settings_path: Option<PathBuf>,
    history_nonce: u64,
    pending_history_open_index: Option<usize>,
    pending_history_open_armed: bool,
    pending_local_open_paths: Option<Vec<PathBuf>>,
    pending_local_open_armed: bool,
    pending_launch_request: Option<LaunchRequest>,
    dicomweb_receiver: Option<Receiver<Result<DicomWebDownloadResult, String>>>,
    dicomweb_active_path_receiver: Option<Receiver<DicomWebGroupStreamUpdate>>,
    dicomweb_active_group_expected: Option<usize>,
    dicomweb_active_group_paths: Vec<PathBuf>,
    dicomweb_active_pending_paths: VecDeque<PathBuf>,
    single_load_receiver: Option<Receiver<Result<PendingSingleLoad, String>>>,
    mammo_load_receiver: Option<Receiver<Result<PendingLoad, String>>>,
    mammo_load_sender: Option<Sender<Result<PendingLoad, String>>>,
    history_pushed_for_active_group: bool,
    history_preload_receiver: Option<Receiver<Result<HistoryPreloadResult, String>>>,
    window_center: f32,
    window_width: f32,
    pending_gsps_overlays: HashMap<String, GspsOverlay>,
    gsps_overlay_visible: bool,
    current_frame: usize,
    cine_mode: bool,
    cine_fps: f32,
    last_cine_advance: Option<Instant>,
    single_view_zoom: f32,
    single_view_pan: egui::Vec2,
    single_view_frame_scroll_accum: f32,
    frame_wait_pending: bool,
    load_error_message: Option<String>,
}

impl Default for DicomViewerApp {
    fn default() -> Self {
        Self::new(None)
    }
}

impl DicomViewerApp {
    pub fn new(initial_request: Option<LaunchRequest>) -> Self {
        let settings_path = metadata_settings_file_path();
        let visible_metadata_fields = settings_path
            .as_deref()
            .and_then(load_visible_metadata_fields)
            .unwrap_or_else(default_visible_metadata_fields);

        Self {
            image: None,
            report: None,
            current_single_path: None,
            texture: None,
            mammo_group: Vec::new(),
            mammo_selected_index: 0,
            history_entries: Vec::new(),
            visible_metadata_fields,
            settings_path,
            history_nonce: 0,
            pending_history_open_index: None,
            pending_history_open_armed: false,
            pending_local_open_paths: None,
            pending_local_open_armed: false,
            pending_launch_request: initial_request,
            dicomweb_receiver: None,
            dicomweb_active_path_receiver: None,
            dicomweb_active_group_expected: None,
            dicomweb_active_group_paths: Vec::new(),
            dicomweb_active_pending_paths: VecDeque::new(),
            single_load_receiver: None,
            mammo_load_receiver: None,
            mammo_load_sender: None,
            history_pushed_for_active_group: false,
            history_preload_receiver: None,
            window_center: 0.0,
            window_width: 1.0,
            pending_gsps_overlays: HashMap::new(),
            gsps_overlay_visible: false,
            current_frame: 0,
            cine_mode: false,
            cine_fps: DEFAULT_CINE_FPS,
            last_cine_advance: None,
            single_view_zoom: 1.0,
            single_view_pan: egui::Vec2::ZERO,
            single_view_frame_scroll_accum: 0.0,
            frame_wait_pending: false,
            load_error_message: None,
        }
    }

    fn apply_black_background(ctx: &egui::Context) {
        let mut visuals = egui::Visuals::dark();
        let line_base = egui::Color32::from_gray(28);
        let line_hover = egui::Color32::from_gray(42);
        let line_active = egui::Color32::from_gray(56);

        visuals.panel_fill = egui::Color32::BLACK;
        visuals.window_fill = egui::Color32::BLACK;
        visuals.faint_bg_color = egui::Color32::BLACK;
        visuals.extreme_bg_color = egui::Color32::BLACK;
        visuals.window_stroke = egui::Stroke::new(1.0, line_base);
        visuals.widgets.noninteractive.bg_stroke = egui::Stroke::new(1.0, line_base);
        visuals.widgets.inactive.bg_stroke = egui::Stroke::new(1.0, line_base);
        visuals.widgets.hovered.bg_stroke = egui::Stroke::new(1.0, line_hover);
        visuals.widgets.active.bg_stroke = egui::Stroke::new(1.0, line_active);
        visuals.widgets.open.bg_stroke = egui::Stroke::new(1.0, line_base);
        ctx.set_visuals(visuals);
    }

    fn is_loading(&self) -> bool {
        self.dicomweb_receiver.is_some()
            || self.dicomweb_active_path_receiver.is_some()
            || !self.dicomweb_active_pending_paths.is_empty()
            || self.single_load_receiver.is_some()
            || self.mammo_load_receiver.is_some()
            || self.history_preload_receiver.is_some()
            || self.pending_history_open_index.is_some()
            || self.pending_local_open_paths.is_some()
    }

    fn merge_gsps_overlays(
        destination: &mut HashMap<String, GspsOverlay>,
        source: HashMap<String, GspsOverlay>,
    ) {
        for (sop_uid, mut overlay) in source {
            destination
                .entry(sop_uid)
                .or_default()
                .graphics
                .append(&mut overlay.graphics);
        }
    }

    fn prepare_load_paths(paths: Vec<PathBuf>) -> PreparedLoadPaths {
        let mut prepared = PreparedLoadPaths::default();

        for path in paths {
            match classify_dicom_path(&path) {
                Ok(DicomPathKind::Gsps) => {
                    prepared.gsps_files_found = prepared.gsps_files_found.saturating_add(1);
                    match load_gsps_overlays(&path) {
                        Ok(overlays) => {
                            Self::merge_gsps_overlays(&mut prepared.gsps_overlays, overlays)
                        }
                        Err(err) => {
                            log::warn!("Could not parse GSPS input: {err:#}");
                        }
                    }
                }
                Ok(DicomPathKind::StructuredReport) => {
                    prepared.structured_report_paths.push(path);
                }
                Ok(DicomPathKind::Image) | Err(_) => {
                    prepared.image_paths.push(path);
                }
                Ok(DicomPathKind::Other) => {
                    prepared.other_files_found = prepared.other_files_found.saturating_add(1);
                }
            }
        }

        prepared
    }

    fn attach_matching_gsps_overlay(
        image: &mut DicomImage,
        overlays: &HashMap<String, GspsOverlay>,
    ) {
        image.gsps_overlay = image
            .sop_instance_uid
            .as_ref()
            .and_then(|uid| overlays.get(uid))
            .cloned()
            .filter(|overlay| !overlay.is_empty());
    }

    fn has_available_gsps_overlay(&self) -> bool {
        if let Some(image) = self.image.as_ref() {
            return image
                .gsps_overlay
                .as_ref()
                .is_some_and(|overlay| !overlay.is_empty());
        }

        self.loaded_mammo_viewports().any(|viewport| {
            viewport
                .image
                .gsps_overlay
                .as_ref()
                .is_some_and(|overlay| !overlay.is_empty())
        })
    }

    fn toggle_gsps_overlay(&mut self) {
        if !self.has_available_gsps_overlay() {
            self.gsps_overlay_visible = false;
            log::debug!("No GSPS overlay available for the current image or group.");
            return;
        }
        self.gsps_overlay_visible = !self.gsps_overlay_visible;
    }

    fn gsps_target_frames(image: &DicomImage, frame_limit: usize) -> Vec<usize> {
        let frame_count = frame_limit.min(image.frame_count());
        if frame_count == 0 {
            return Vec::new();
        }

        let Some(overlay) = image.gsps_overlay.as_ref() else {
            return Vec::new();
        };
        if overlay.is_empty() {
            return Vec::new();
        }

        let mut frame_targets = Vec::new();
        for graphic in &overlay.graphics {
            match graphic.referenced_frames.as_ref() {
                None => return (0..frame_count).collect(),
                Some(referenced_frames) => {
                    for frame_number in referenced_frames {
                        let Some(frame_index) = frame_number.checked_sub(1) else {
                            continue;
                        };
                        if frame_index < frame_count {
                            frame_targets.push(frame_index);
                        }
                    }
                }
            }
        }

        frame_targets.sort_unstable();
        frame_targets.dedup();
        frame_targets
    }

    fn gsps_navigation_targets(&self) -> Vec<GspsNavigationTarget> {
        if let Some(image) = self.image.as_ref() {
            return Self::gsps_target_frames(image, image.frame_count())
                .into_iter()
                .map(|frame_index| GspsNavigationTarget {
                    viewport_index: 0,
                    frame_index,
                })
                .collect();
        }

        let common_frame_count = self.mammo_group_common_frame_count();
        let mut targets = Vec::new();
        for (viewport_index, viewport) in self.mammo_group.iter().enumerate() {
            let Some(viewport) = viewport.as_ref() else {
                continue;
            };

            for frame_index in Self::gsps_target_frames(&viewport.image, common_frame_count) {
                targets.push(GspsNavigationTarget {
                    viewport_index,
                    frame_index,
                });
            }
        }

        targets
    }

    fn next_gsps_navigation_target(&self) -> Option<GspsNavigationTarget> {
        let targets = self.gsps_navigation_targets();
        if targets.is_empty() {
            return None;
        }

        let current_target = if self.image.is_some() {
            GspsNavigationTarget {
                viewport_index: 0,
                frame_index: self.current_frame,
            }
        } else {
            let current_frame = self
                .mammo_group
                .get(self.mammo_selected_index)
                .and_then(Option::as_ref)
                .map(|viewport| viewport.current_frame)
                .unwrap_or(0);
            GspsNavigationTarget {
                viewport_index: self.mammo_selected_index,
                frame_index: current_frame,
            }
        };

        let target_index = match targets.iter().position(|target| *target == current_target) {
            Some(index) => (index + 1) % targets.len(),
            None => targets
                .iter()
                .position(|target| *target > current_target)
                .unwrap_or(0),
        };
        targets.get(target_index).copied()
    }

    fn jump_to_next_gsps_overlay(&mut self, ctx: &egui::Context) {
        let Some(target) = self.next_gsps_navigation_target() else {
            log::debug!("No GSPS overlay target available for the current image or group.");
            return;
        };

        self.gsps_overlay_visible = true;
        self.last_cine_advance = Some(Instant::now());

        if self.image.is_some() {
            self.current_frame = target.frame_index;
            self.rebuild_texture(ctx);
            ctx.request_repaint();
            return;
        }

        self.mammo_selected_index = target.viewport_index;
        if self.set_mammo_group_frame(target.frame_index) {
            ctx.request_repaint_after(Duration::from_millis(16));
        } else {
            ctx.request_repaint();
        }
    }

    fn is_supported_group_size(count: usize) -> bool {
        VALID_GROUP_SIZES.contains(&count)
    }

    fn is_supported_multi_view_group_size(count: usize) -> bool {
        Self::is_supported_group_size(count) && count != 1
    }

    fn multi_view_grid_dimensions(count: usize) -> Option<(usize, usize)> {
        match count {
            2 => Some((1, 2)),
            3 => Some((1, 3)),
            4 => Some((2, 2)),
            8 => Some((2, 4)),
            _ => None,
        }
    }

    fn multi_view_layout_label(count: usize) -> &'static str {
        match count {
            2 => "1x2",
            3 => "1x3",
            4 => "2x2",
            8 => "2x4",
            _ => "multi-view",
        }
    }

    fn join_with_or(parts: &[String]) -> String {
        match parts.len() {
            0 => String::new(),
            1 => parts[0].clone(),
            2 => format!("{} or {}", parts[0], parts[1]),
            _ => {
                let (last, leading) = parts
                    .split_last()
                    .expect("join_with_or: expected at least 1 part");
                format!("{}, or {}", leading.join(", "), last)
            }
        }
    }

    fn format_valid_group_sizes_list() -> String {
        let parts = VALID_GROUP_SIZES
            .iter()
            .map(|size| size.to_string())
            .collect::<Vec<_>>();
        Self::join_with_or(&parts)
    }

    fn group_layout_label(size: usize) -> &'static str {
        match size {
            1 => "1x1",
            _ => Self::multi_view_layout_label(size),
        }
    }

    fn format_supported_group_sizes_with_layouts(include_single: bool) -> String {
        let parts = VALID_GROUP_SIZES
            .iter()
            .copied()
            .filter(|size| include_single || *size != 1)
            .map(|size| format!("{size} ({})", Self::group_layout_label(size)))
            .collect::<Vec<_>>();
        Self::join_with_or(&parts)
    }

    fn format_select_paths_count_error(got: usize) -> String {
        format!(
            "Select one of the supported view sizes: {} DICOM files (got {}).",
            Self::format_supported_group_sizes_with_layouts(true),
            got
        )
    }

    fn format_multi_view_size_error(got: usize) -> String {
        format!(
            "Multi-view group must be one of these DICOM file counts: {} (got {}).",
            Self::format_supported_group_sizes_with_layouts(false),
            got
        )
    }

    fn format_group_size_error(index: usize, len: usize) -> String {
        format!(
            "Launch group {} has {} paths; each group must contain {} DICOM files.",
            index,
            len,
            Self::format_valid_group_sizes_list()
        )
    }

    fn is_supported_prepared_group(prepared: &PreparedLoadPaths) -> bool {
        Self::is_supported_group_size(prepared.image_paths.len())
            || (prepared.image_paths.is_empty() && prepared.structured_report_paths.len() == 1)
    }

    fn reorder_indices_cover_all(items_len: usize, ordered_indices: &[usize]) -> bool {
        if ordered_indices.len() != items_len {
            return false;
        }
        let mut seen = vec![false; items_len];
        for &index in ordered_indices {
            if index >= items_len || seen[index] {
                return false;
            }
            seen[index] = true;
        }
        seen.iter().all(|flag| *flag)
    }

    fn reorder_items_by_indices<T>(items: Vec<T>, ordered_indices: Vec<usize>) -> Vec<T> {
        debug_assert!(
            Self::reorder_indices_cover_all(items.len(), &ordered_indices),
            "reorder_items_by_indices: ordered_indices must cover all items exactly once"
        );
        let mut pending = items.into_iter().map(Some).collect::<Vec<_>>();
        let mut ordered = Vec::with_capacity(pending.len());
        for index in ordered_indices {
            let item = pending[index]
                .take()
                .expect("reorder_items_by_indices: prevalidated index cannot repeat");
            ordered.push(item);
        }
        ordered
    }

    fn restore_ordered_items_or_log<T>(
        ordered_viewports: Vec<T>,
        ordered_indices: Vec<usize>,
        selected_index: Option<usize>,
        context: &str,
    ) -> (Vec<T>, Option<usize>, bool) {
        if !Self::reorder_indices_cover_all(ordered_viewports.len(), &ordered_indices) {
            log::warn!("reorder_items_by_indices: invalid ordered_indices while {context}");
            return (ordered_viewports, selected_index, false);
        }

        let selected_index = selected_index.map(|selected| {
            ordered_indices
                .iter()
                .position(|index| *index == selected)
                .unwrap_or(selected)
        });
        let ordered_viewports = Self::reorder_items_by_indices(ordered_viewports, ordered_indices);
        (ordered_viewports, selected_index, true)
    }

    fn has_mammo_group(&self) -> bool {
        !self.mammo_group.is_empty()
            || self.mammo_load_receiver.is_some()
            || (self
                .dicomweb_active_group_expected
                .is_some_and(Self::is_supported_multi_view_group_size)
                && (self.dicomweb_active_path_receiver.is_some()
                    || !self.dicomweb_active_pending_paths.is_empty()))
    }

    fn loaded_mammo_viewports(&self) -> impl Iterator<Item = &MammoViewport> {
        self.mammo_group.iter().filter_map(Option::as_ref)
    }

    fn loaded_mammo_count(&self) -> usize {
        self.loaded_mammo_viewports().count()
    }

    fn mammo_group_complete(&self) -> bool {
        Self::is_supported_multi_view_group_size(self.mammo_group.len())
            && self.loaded_mammo_count() == self.mammo_group.len()
    }

    fn default_cine_fps_for_active_image(&self) -> f32 {
        self.image
            .as_ref()
            .map(|image| image.recommended_cine_fps)
            .or_else(|| {
                self.selected_mammo_viewport()
                    .map(|viewport| viewport.image.recommended_cine_fps)
            })
            .flatten()
            .unwrap_or(DEFAULT_CINE_FPS)
            .clamp(1.0, 120.0)
    }

    fn mammo_group_common_frame_count(&self) -> usize {
        self.loaded_mammo_viewports()
            .map(|viewport| viewport.image.frame_count())
            .min()
            .unwrap_or(0)
    }

    fn set_mammo_group_frame(&mut self, frame_index: usize) -> bool {
        if self.loaded_mammo_count() == 0 {
            return false;
        }

        let (mut rendered_frames, safe_frames, slots) = {
            let mut slots = Vec::new();
            let inputs = self
                .mammo_group
                .iter()
                .enumerate()
                .filter_map(|(slot, viewport)| viewport.as_ref().map(|viewport| (slot, viewport)))
                .map(|(slot, viewport)| {
                    let frame_count = viewport.image.frame_count();
                    let safe_frame = if frame_count == 0 {
                        0
                    } else {
                        frame_index.min(frame_count.saturating_sub(1))
                    };
                    slots.push(slot);
                    (
                        &viewport.image,
                        safe_frame,
                        viewport.window_center,
                        viewport.window_width,
                    )
                })
                .collect::<Vec<_>>();

            let mut rendered = (0..inputs.len())
                .map(|_| None::<ColorImage>)
                .collect::<Vec<_>>();
            let mut safe_frames = vec![0usize; inputs.len()];

            std::thread::scope(|scope| {
                let mut jobs = Vec::with_capacity(inputs.len());
                for (index, (image, safe_frame, center, width)) in inputs.iter().enumerate() {
                    safe_frames[index] = *safe_frame;
                    jobs.push((
                        index,
                        scope.spawn(move || {
                            Self::render_image_frame(image, *safe_frame, *center, *width)
                        }),
                    ));
                }

                for (index, job) in jobs {
                    rendered[index] = job.join().ok().flatten();
                }
            });

            (rendered, safe_frames, slots)
        };

        let mut missing_any = false;
        for (index, slot) in slots.into_iter().enumerate() {
            let Some(viewport) = self.mammo_group.get_mut(slot).and_then(Option::as_mut) else {
                continue;
            };
            let frame_count = viewport.image.frame_count();
            if frame_count == 0 {
                continue;
            }

            viewport.current_frame = safe_frames[index].min(frame_count.saturating_sub(1));
            if let Some(color_image) = rendered_frames[index].take() {
                viewport.texture.set(color_image, TextureOptions::LINEAR);
            } else {
                missing_any = true;
            }
        }
        self.frame_wait_pending = missing_any;
        missing_any
    }

    fn selected_mammo_frame_index(&self) -> usize {
        self.selected_mammo_viewport()
            .map(|viewport| viewport.current_frame)
            .unwrap_or(0)
    }

    fn show_metadata_field_options_menu(&mut self, ui: &mut egui::Ui) {
        let mut changed = false;
        ui.horizontal(|ui| {
            if ui.small_button("All").clicked() {
                self.visible_metadata_fields = default_visible_metadata_fields();
                changed = true;
            }
            if ui.small_button("None").clicked() {
                self.visible_metadata_fields.clear();
                changed = true;
            }
        });
        ui.add_space(4.0);
        egui::ScrollArea::vertical()
            .id_salt("metadata-fields-menu")
            .max_height(220.0)
            .show(ui, |ui| {
                for field in METADATA_FIELD_NAMES {
                    let mut checked = self.visible_metadata_fields.contains(*field);
                    if ui.checkbox(&mut checked, *field).changed() {
                        if checked {
                            self.visible_metadata_fields.insert((*field).to_string());
                        } else {
                            self.visible_metadata_fields.remove(*field);
                        }
                        changed = true;
                    }
                }
            });
        if changed {
            self.persist_metadata_settings();
        }
    }

    fn persist_metadata_settings(&self) {
        let Some(path) = self.settings_path.as_ref() else {
            return;
        };
        let Some(parent) = path.parent() else {
            return;
        };
        if let Err(err) = fs::create_dir_all(parent) {
            log::warn!(
                "Could not create settings directory {}: {err}",
                parent.display()
            );
            return;
        }

        let fields = ordered_visible_metadata_fields(&self.visible_metadata_fields);
        let contents = render_settings_toml(&fields);
        if let Err(err) = fs::write(path, contents) {
            log::warn!("Could not write settings file: {err}");
        }
    }

    fn queue_history_open(&mut self, index: usize) {
        if self.pending_history_open_index.is_none() {
            self.pending_history_open_armed = false;
        }
        self.pending_history_open_index = Some(index);
    }

    fn process_pending_history_open(&mut self, ctx: &egui::Context) {
        let Some(index) = self.pending_history_open_index else {
            return;
        };

        if !self.pending_history_open_armed {
            self.pending_history_open_armed = true;
            ctx.request_repaint();
            return;
        }

        self.pending_history_open_index = None;
        self.pending_history_open_armed = false;
        self.open_history_entry(index, ctx);
    }

    fn queue_local_paths_open(&mut self, paths: Vec<PathBuf>) {
        if paths.is_empty() {
            return;
        }
        self.pending_local_open_paths = Some(paths);
        self.pending_local_open_armed = false;
    }

    fn local_paths_from_dropped_files(dropped_files: &[egui::DroppedFile]) -> Vec<PathBuf> {
        dropped_files
            .iter()
            .filter_map(|file| file.path.clone())
            .collect()
    }

    fn hovered_file_count(hovered_files: &[egui::HoveredFile]) -> usize {
        let local_path_count = hovered_files
            .iter()
            .filter(|file| file.path.is_some())
            .count();
        if local_path_count > 0 {
            local_path_count
        } else {
            hovered_files.len()
        }
    }

    fn file_drop_overlay_heading(hovered_files: &[egui::HoveredFile]) -> String {
        if !hovered_files.is_empty() && hovered_files.iter().all(|file| file.path.is_none()) {
            return "Only local files can be dropped here".to_string();
        }

        match Self::hovered_file_count(hovered_files) {
            0 => "Drop DICOM files to open them".to_string(),
            1 => "Drop 1 file to open it".to_string(),
            count => format!("Drop {count} files to open them"),
        }
    }

    fn apply_dropped_files(&mut self, dropped_files: &[egui::DroppedFile], ctx: &egui::Context) {
        if dropped_files.is_empty() {
            return;
        }

        let paths = Self::local_paths_from_dropped_files(dropped_files);
        if paths.is_empty() {
            let message = "Dropped items did not include readable local file paths.";
            self.set_load_error(message);
            log::warn!("{message}");
            ctx.request_repaint();
            return;
        }

        log::info!("Opening {} dropped file(s).", paths.len());
        self.clear_load_error();
        self.queue_local_paths_open(paths);
        ctx.set_cursor_icon(egui::CursorIcon::Progress);
        ctx.request_repaint();
    }

    fn process_pending_local_open(&mut self, ctx: &egui::Context) {
        let Some(_) = self.pending_local_open_paths else {
            return;
        };

        if !self.pending_local_open_armed {
            self.pending_local_open_armed = true;
            ctx.request_repaint();
            return;
        }

        let Some(paths) = self.pending_local_open_paths.take() else {
            self.pending_local_open_armed = false;
            return;
        };
        self.pending_local_open_armed = false;
        let _ = self.load_selected_paths(paths, ctx);
    }

    fn clear_single_viewer(&mut self) {
        self.image = None;
        self.report = None;
        self.current_single_path = None;
        self.texture = None;
        self.gsps_overlay_visible = false;
        self.current_frame = 0;
        self.cine_mode = false;
        self.last_cine_advance = None;
        self.mammo_selected_index = 0;
        self.reset_single_view_transform();
        self.single_view_frame_scroll_accum = 0.0;
        self.frame_wait_pending = false;
    }

    fn reset_single_view_transform(&mut self) {
        self.single_view_zoom = 1.0;
        self.single_view_pan = egui::Vec2::ZERO;
    }

    fn clear_load_error(&mut self) {
        self.load_error_message = None;
    }

    fn set_load_error(&mut self, message: impl Into<String>) {
        self.load_error_message = Some(message.into());
    }

    fn next_history_texture_name(&mut self, prefix: &str) -> String {
        self.history_nonce = self.history_nonce.saturating_add(1);
        format!("history-{prefix}-{}", self.history_nonce)
    }

    fn build_history_thumb(
        &mut self,
        image: &DicomImage,
        frame_index: usize,
        window_center: f32,
        window_width: f32,
        texture_key_prefix: &str,
        ctx: &egui::Context,
    ) -> Option<TextureHandle> {
        let frame_count = image.frame_count();
        if frame_count == 0 {
            return None;
        }
        let safe_frame = frame_index.min(frame_count.saturating_sub(1));
        let rendered = Self::render_image_frame(image, safe_frame, window_center, window_width)?;
        let thumb = downsample_color_image(&rendered, HISTORY_THUMB_MAX_DIM);
        let texture_name = self.next_history_texture_name(texture_key_prefix);
        Some(ctx.load_texture(texture_name, thumb, TextureOptions::LINEAR))
    }

    fn build_group_history_thumb(
        &mut self,
        group: &[MammoViewport],
        texture_key_prefix: &str,
        ctx: &egui::Context,
    ) -> Option<TextureHandle> {
        let ordered_indices = order_mammo_indices(group, |viewport| &viewport.image);
        let mut rendered_views = Vec::new();
        for index in ordered_indices {
            let viewport = &group[index];
            let frame_count = viewport.image.frame_count();
            if frame_count == 0 {
                continue;
            }
            let safe_frame = viewport.current_frame.min(frame_count.saturating_sub(1));
            let rendered = Self::render_image_frame(
                &viewport.image,
                safe_frame,
                viewport.window_center,
                viewport.window_width,
            )?;
            rendered_views.push(rendered);
        }

        if rendered_views.is_empty() {
            return None;
        }

        let thumb = if rendered_views.len() == 1 {
            downsample_color_image(&rendered_views[0], HISTORY_THUMB_MAX_DIM)
        } else {
            compose_grid_thumb(&rendered_views, HISTORY_THUMB_MAX_DIM)
        };

        let texture_name = self.next_history_texture_name(texture_key_prefix);
        Some(ctx.load_texture(texture_name, thumb, TextureOptions::LINEAR))
    }

    fn build_report_history_thumb(
        &mut self,
        _report: &StructuredReportDocument,
        texture_key_prefix: &str,
        ctx: &egui::Context,
    ) -> TextureHandle {
        const OUTER_WIDTH: usize = 60;
        const OUTER_HEIGHT: usize = 76;
        const BORDER_INSET: usize = 2;
        const INNER_X: usize = BORDER_INSET;
        const INNER_Y: usize = BORDER_INSET;
        const INNER_WIDTH: usize = OUTER_WIDTH - (BORDER_INSET * 2);
        const INNER_HEIGHT: usize = OUTER_HEIGHT - (BORDER_INSET * 2);
        const HEADER_HEIGHT: usize = 8;
        const MAX_LINE_WIDTH: usize = 40;
        const TEXT_PAD: usize = (INNER_WIDTH - MAX_LINE_WIDTH) / 2;
        const TEXT_START_X: usize = INNER_X + TEXT_PAD;
        const TEXT_START_Y: usize = INNER_Y + HEADER_HEIGHT + TEXT_PAD;
        const TEXT_LINE_HEIGHT: usize = 4;
        const TEXT_LINE_GAP: usize = 7;
        const TEXT_LINE_STEP_Y: usize = TEXT_LINE_HEIGHT + TEXT_LINE_GAP;
        const TEXT_LINE_WIDTHS: &[usize] = &[
            MAX_LINE_WIDTH,
            MAX_LINE_WIDTH - 4,
            MAX_LINE_WIDTH - 10,
            MAX_LINE_WIDTH - 6,
            MAX_LINE_WIDTH - 18,
        ];

        let background = egui::Color32::TRANSPARENT;
        let border = egui::Color32::from_rgb(40, 49, 60);
        let header = border;
        let paper = egui::Color32::from_rgb(20, 27, 34);
        let text_line = egui::Color32::from_rgb(142, 152, 164);
        let mut pixels = vec![background; OUTER_WIDTH * OUTER_HEIGHT];

        let fill_rect = |pixels: &mut [egui::Color32],
                         x: usize,
                         y: usize,
                         rect_width: usize,
                         rect_height: usize,
                         color: egui::Color32| {
            let clamped_x1 = (x + rect_width).min(OUTER_WIDTH);
            let clamped_y1 = (y + rect_height).min(OUTER_HEIGHT);
            for row_y in y.min(OUTER_HEIGHT)..clamped_y1 {
                let row = row_y * OUTER_WIDTH;
                for col_x in x.min(OUTER_WIDTH)..clamped_x1 {
                    pixels[row + col_x] = color;
                }
            }
        };

        fill_rect(&mut pixels, 0, 0, OUTER_WIDTH, OUTER_HEIGHT, border);
        fill_rect(
            &mut pixels,
            INNER_X,
            INNER_Y,
            INNER_WIDTH,
            INNER_HEIGHT,
            paper,
        );
        fill_rect(
            &mut pixels,
            INNER_X,
            INNER_Y,
            INNER_WIDTH,
            HEADER_HEIGHT,
            header,
        );
        for (line_index, &line_width) in TEXT_LINE_WIDTHS.iter().enumerate() {
            fill_rect(
                &mut pixels,
                TEXT_START_X,
                TEXT_START_Y + (line_index * TEXT_LINE_STEP_Y),
                line_width,
                TEXT_LINE_HEIGHT,
                text_line,
            );
        }

        let texture_name = self.next_history_texture_name(texture_key_prefix);
        ctx.load_texture(
            texture_name,
            ColorImage {
                size: [OUTER_WIDTH, OUTER_HEIGHT],
                pixels,
            },
            TextureOptions::LINEAR,
        )
    }

    fn upsert_history_entry(&mut self, entry: HistoryEntry) {
        if let Some(existing_index) = self
            .history_entries
            .iter()
            .position(|existing| existing.id == entry.id)
        {
            self.history_entries.remove(existing_index);
        }
        self.history_entries.insert(0, entry);
        if self.history_entries.len() > HISTORY_MAX_ENTRIES {
            self.history_entries.truncate(HISTORY_MAX_ENTRIES);
        }
    }

    fn push_single_history_entry(&mut self, single: HistorySingleData, ctx: &egui::Context) {
        let Some(thumb_texture) = self.build_history_thumb(
            &single.image,
            single.current_frame,
            single.window_center,
            single.window_width,
            "single",
            ctx,
        ) else {
            return;
        };

        let history_paths = vec![single.path.clone()];
        self.upsert_history_entry(HistoryEntry {
            id: history_id_from_paths(&history_paths),
            kind: HistoryKind::Single(Box::new(single)),
            thumbs: vec![HistoryThumb {
                texture: thumb_texture,
            }],
        });
    }

    fn push_group_history_entry(
        &mut self,
        group: &[MammoViewport],
        selected_index: usize,
        ctx: &egui::Context,
    ) {
        if !Self::is_supported_multi_view_group_size(group.len()) {
            return;
        }

        let mut paths = Vec::new();
        let mut cached_viewports = Vec::new();
        for viewport in group {
            paths.push(viewport.path.clone());
            cached_viewports.push(HistoryGroupViewportData {
                path: viewport.path.clone(),
                image: viewport.image.clone(),
                texture: viewport.texture.clone(),
                label: viewport.label.clone(),
                window_center: viewport.window_center,
                window_width: viewport.window_width,
                current_frame: viewport.current_frame,
            });
        }
        let Some(group_thumb) = self.build_group_history_thumb(group, "group", ctx) else {
            return;
        };

        self.upsert_history_entry(HistoryEntry {
            id: history_id_from_paths(&paths),
            kind: HistoryKind::Group(HistoryGroupData {
                viewports: cached_viewports,
                selected_index: selected_index.min(group.len().saturating_sub(1)),
            }),
            thumbs: vec![HistoryThumb {
                texture: group_thumb,
            }],
        });
    }

    fn push_report_history_entry(
        &mut self,
        path: PathBuf,
        report: StructuredReportDocument,
        ctx: &egui::Context,
    ) {
        let thumb_texture = self.build_report_history_thumb(&report, "report", ctx);
        let history_paths = vec![path.clone()];
        self.upsert_history_entry(HistoryEntry {
            id: history_id_from_paths(&history_paths),
            kind: HistoryKind::Report(Box::new(HistoryReportData { path, report })),
            thumbs: vec![HistoryThumb {
                texture: thumb_texture,
            }],
        });
    }

    fn current_history_id(&self) -> Option<String> {
        if let Some(path) = self.current_single_path.as_ref() {
            let paths = vec![path.clone()];
            return Some(history_id_from_paths(&paths));
        }

        if !self.mammo_group_complete() {
            return None;
        }

        let paths = self
            .loaded_mammo_viewports()
            .map(|viewport| viewport.path.clone())
            .collect::<Vec<_>>();
        Some(history_id_from_paths(&paths))
    }

    fn has_open_study(&self) -> bool {
        self.image.is_some()
            || self.report.is_some()
            || self.current_single_path.is_some()
            || self.has_mammo_group()
            || self.single_load_receiver.is_some()
            || self.dicomweb_receiver.is_some()
            || self.dicomweb_active_path_receiver.is_some()
            || !self.dicomweb_active_pending_paths.is_empty()
    }

    fn clear_active_study(&mut self) {
        self.pending_launch_request = None;
        self.pending_local_open_paths = None;
        self.pending_local_open_armed = false;
        self.pending_history_open_index = None;
        self.pending_history_open_armed = false;
        self.dicomweb_receiver = None;
        self.dicomweb_active_path_receiver = None;
        self.dicomweb_active_group_expected = None;
        self.dicomweb_active_group_paths.clear();
        self.dicomweb_active_pending_paths.clear();
        self.single_load_receiver = None;
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.history_preload_receiver = None;
        self.history_pushed_for_active_group = false;
        self.pending_gsps_overlays.clear();
        self.clear_single_viewer();
        self.mammo_group.clear();
        self.mammo_selected_index = 0;
        self.clear_load_error();
    }

    fn close_current_group(&mut self, ctx: &egui::Context) {
        if !self.has_open_study() {
            return;
        }

        self.sync_current_state_to_history();
        let next_history_index = if let Some(current_id) = self.current_history_id() {
            if let Some(index) = self
                .history_entries
                .iter()
                .position(|entry| entry.id == current_id)
            {
                self.history_entries.remove(index);
                if self.history_entries.is_empty() {
                    None
                } else {
                    Some(index.min(self.history_entries.len().saturating_sub(1)))
                }
            } else if self.history_entries.is_empty() {
                None
            } else {
                Some(0)
            }
        } else if self.history_entries.is_empty() {
            None
        } else {
            Some(0)
        };

        self.clear_active_study();
        if let Some(index) = next_history_index {
            self.open_history_entry(index, ctx);
        } else {
            ctx.request_repaint();
        }
    }

    fn handle_close_group_shortcut(&mut self, ctx: &egui::Context) -> bool {
        if self.has_open_study() {
            self.close_current_group(ctx);
            false
        } else {
            true
        }
    }

    fn move_current_history_to_front(&mut self) {
        let Some(current_id) = self.current_history_id() else {
            return;
        };
        let Some(index) = self
            .history_entries
            .iter()
            .position(|entry| entry.id == current_id)
        else {
            return;
        };
        if index == 0 {
            return;
        }
        let entry = self.history_entries.remove(index);
        self.history_entries.insert(0, entry);
    }

    fn preload_group_into_history(
        prepared: PreparedLoadPaths,
        tx: &mpsc::Sender<Result<HistoryPreloadResult, String>>,
    ) {
        let load_paths = prepared.image_paths;
        let report_paths = prepared.structured_report_paths;
        let gsps_overlays = prepared.gsps_overlays;

        if load_paths.is_empty() {
            let result = match report_paths.as_slice() {
                [path] => load_structured_report(path)
                    .map(|report| HistoryPreloadResult::Report {
                        path: path.clone(),
                        report: Box::new(report),
                    })
                    .map_err(|err| format!("{err:#}")),
                _ => Err("Unsupported preload group size".to_string()),
            };
            let _ = tx.send(result);
            return;
        }

        for path in report_paths {
            let result = load_structured_report(&path)
                .map(|report| HistoryPreloadResult::Report {
                    path: path.clone(),
                    report: Box::new(report),
                })
                .map_err(|err| format!("{err:#}"));
            let _ = tx.send(result);
        }

        let result = match load_paths.len() {
            1 => {
                let path = load_paths[0].clone();
                load_dicom(&path)
                    .map(|mut image| {
                        Self::attach_matching_gsps_overlay(&mut image, &gsps_overlays);
                        image
                    })
                    .map(|image| HistoryPreloadResult::Single {
                        path,
                        image: Box::new(image),
                    })
                    .map_err(|err| format!("{err:#}"))
            }
            count if Self::is_supported_multi_view_group_size(count) => {
                let mut viewports = Vec::with_capacity(load_paths.len());
                for path in &load_paths {
                    let image = match load_dicom(path).map_err(|err| format!("{err:#}")) {
                        Ok(image) => image,
                        Err(err) => {
                            let _ = tx.send(Err(err));
                            return;
                        }
                    };
                    let mut image = image;
                    Self::attach_matching_gsps_overlay(&mut image, &gsps_overlays);
                    viewports.push((path.clone(), image));
                }
                Ok(HistoryPreloadResult::Group { viewports })
            }
            _ => Err("Unsupported preload group size".to_string()),
        };
        let _ = tx.send(result);
    }

    fn preload_non_active_groups_into_history(
        &mut self,
        groups: &[PreparedLoadPaths],
        open_group: usize,
    ) {
        let preload_jobs = groups
            .iter()
            .enumerate()
            .rev()
            .filter(|(index, _)| *index != open_group)
            .map(|(_, group)| group.clone())
            .collect::<Vec<_>>();
        if preload_jobs.is_empty() {
            self.history_preload_receiver = None;
            return;
        }

        let (tx, rx) = mpsc::channel::<Result<HistoryPreloadResult, String>>();
        thread::spawn(move || {
            for group in preload_jobs {
                Self::preload_group_into_history(group, &tx);
            }
        });
        self.history_preload_receiver = Some(rx);
    }

    fn sync_current_state_to_history(&mut self) {
        let loaded_mammo_count = self.loaded_mammo_count();
        let selected_index = self
            .mammo_selected_index
            .min(self.mammo_group.len().saturating_sub(1));
        let Some(current_id) = self.current_history_id() else {
            return;
        };
        let Some(entry) = self
            .history_entries
            .iter_mut()
            .find(|entry| entry.id == current_id)
        else {
            return;
        };

        match &mut entry.kind {
            HistoryKind::Single(single) => {
                if let Some(path) = self.current_single_path.as_ref() {
                    single.path = path.clone();
                }
                if let Some(texture) = self.texture.as_ref() {
                    single.texture = texture.clone();
                }
                single.window_center = self.window_center;
                single.window_width = self.window_width;
                single.current_frame = self.current_frame;
                single.cine_fps = self.cine_fps;
            }
            HistoryKind::Group(group) => {
                if loaded_mammo_count == 0 {
                    return;
                }
                group.selected_index = selected_index;
                for cached_viewport in &mut group.viewports {
                    if let Some(active_viewport) = self
                        .mammo_group
                        .iter()
                        .filter_map(Option::as_ref)
                        .find(|viewport| viewport.path == cached_viewport.path)
                    {
                        cached_viewport.texture = active_viewport.texture.clone();
                        cached_viewport.window_center = active_viewport.window_center;
                        cached_viewport.window_width = active_viewport.window_width;
                        cached_viewport.current_frame = active_viewport.current_frame;
                    }
                    Self::attach_matching_gsps_overlay(
                        &mut cached_viewport.image,
                        &self.pending_gsps_overlays,
                    );
                }
            }
            HistoryKind::Report(report) => {
                if let Some(path) = self.current_single_path.as_ref() {
                    report.path = path.clone();
                }
            }
        }
    }

    fn open_history_entry(&mut self, index: usize, ctx: &egui::Context) {
        self.sync_current_state_to_history();

        let Some(kind) = self
            .history_entries
            .get(index)
            .map(|entry| entry.kind.clone())
        else {
            return;
        };

        match kind {
            HistoryKind::Single(single) => {
                self.report = None;
                self.image = Some(single.image);
                self.current_single_path = Some(single.path);
                self.texture = None;
                self.gsps_overlay_visible = false;
                self.window_center = single.window_center;
                self.window_width = single.window_width.max(1.0);
                self.current_frame = single.current_frame;
                self.cine_mode = false;
                self.last_cine_advance = None;
                self.cine_fps = single.cine_fps.clamp(1.0, 120.0);
                self.mammo_group.clear();
                self.mammo_selected_index = 0;
                self.reset_single_view_transform();
                self.single_view_frame_scroll_accum = 0.0;
                if let Some(image) = self.image.as_ref() {
                    let frame_count = image.frame_count();
                    if frame_count == 0 {
                        self.current_frame = 0;
                    } else {
                        self.current_frame = self.current_frame.min(frame_count.saturating_sub(1));
                    }
                }
                self.clear_load_error();
                self.rebuild_texture(ctx);
                log::info!("Loaded study from memory cache.");
                ctx.request_repaint();
            }
            HistoryKind::Group(group) => {
                self.mammo_load_receiver = None;
                self.mammo_load_sender = None;
                self.clear_single_viewer();
                self.gsps_overlay_visible = false;
                let ordered_indices =
                    order_mammo_indices(&group.viewports, |viewport| &viewport.image);
                let (ordered_viewports, selected_index, _) = Self::restore_ordered_items_or_log(
                    group.viewports,
                    ordered_indices,
                    Some(group.selected_index),
                    "restoring history group",
                );
                self.mammo_group = ordered_viewports
                    .into_iter()
                    .map(|viewport| {
                        Some(MammoViewport {
                            path: viewport.path,
                            image: viewport.image,
                            texture: viewport.texture,
                            label: viewport.label,
                            window_center: viewport.window_center,
                            window_width: viewport.window_width,
                            current_frame: viewport.current_frame,
                            zoom: 1.0,
                            pan: egui::Vec2::ZERO,
                            frame_scroll_accum: 0.0,
                        })
                    })
                    .collect::<Vec<_>>();
                if self.loaded_mammo_count() == 0 {
                    log::warn!("History entry had no cached group images.");
                    return;
                }
                self.mammo_selected_index = selected_index
                    .unwrap_or(group.selected_index)
                    .min(self.mammo_group.len().saturating_sub(1));
                self.clear_load_error();
                log::info!("Loaded grouped study from memory cache.");
                ctx.request_repaint();
            }
            HistoryKind::Report(report) => {
                self.clear_single_viewer();
                self.mammo_group.clear();
                self.report = Some(report.report);
                self.current_single_path = Some(report.path);
                self.clear_load_error();
                log::info!("Loaded structured report from memory cache.");
                ctx.request_repaint();
            }
        }
    }

    fn cycle_history_entry(&mut self, direction: i32) {
        let len = self.history_entries.len();
        if len <= 1 {
            return;
        }

        let current_index = self
            .pending_history_open_index
            .or_else(|| {
                self.current_history_id()
                    .as_deref()
                    .and_then(|id| self.history_entries.iter().position(|entry| entry.id == id))
            })
            .map(|index| index % len)
            .unwrap_or(0);

        let next_index = if direction < 0 {
            if current_index == 0 {
                len - 1
            } else {
                current_index - 1
            }
        } else {
            (current_index + 1) % len
        };

        self.queue_history_open(next_index);
    }

    fn handle_launch_request(&mut self, request: LaunchRequest, ctx: &egui::Context) {
        match request {
            LaunchRequest::LocalPaths(paths) => self.queue_local_paths_open(paths),
            LaunchRequest::LocalGroups { groups, open_group } => {
                self.load_local_groups(groups, open_group, ctx)
            }
            LaunchRequest::DicomWebGroups(request) => self.start_dicomweb_group_download(request),
            LaunchRequest::DicomWeb(request) => self.start_dicomweb_download(request),
        }
    }

    fn load_local_groups(
        &mut self,
        groups: Vec<Vec<PathBuf>>,
        open_group: usize,
        ctx: &egui::Context,
    ) {
        if groups.is_empty() {
            self.set_load_error("Launch request had no groups to open.");
            log::warn!("Launch request had no groups to open.");
            return;
        }

        self.clear_load_error();
        let mut preload_groups = Vec::with_capacity(groups.len());
        for (index, group) in groups.iter().enumerate() {
            let prepared = Self::prepare_load_paths(group.clone());
            let entry_count = if prepared.image_paths.is_empty() {
                prepared.structured_report_paths.len()
            } else {
                prepared.image_paths.len()
            };
            if !Self::is_supported_prepared_group(&prepared) {
                let err = Self::format_group_size_error(index + 1, entry_count);
                self.set_load_error(err.clone());
                log::warn!("{err}");
                return;
            }
            preload_groups.push(prepared);
        }

        let active_group = open_group.min(groups.len().saturating_sub(1));
        let _ = self.load_selected_paths(groups[active_group].clone(), ctx);
        self.preload_non_active_groups_into_history(&preload_groups, active_group);
    }

    fn start_dicomweb_download(&mut self, request: DicomWebLaunchRequest) {
        if self.dicomweb_receiver.is_some() {
            log::warn!("DICOMweb download already in progress.");
            return;
        }

        self.clear_load_error();
        self.sync_current_state_to_history();
        self.history_preload_receiver = None;
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.history_pushed_for_active_group = false;
        self.pending_gsps_overlays.clear();
        self.gsps_overlay_visible = false;
        self.dicomweb_active_path_receiver = None;
        self.dicomweb_active_group_expected = None;
        self.dicomweb_active_group_paths.clear();
        self.dicomweb_active_pending_paths.clear();
        log::info!("Loading study from DICOMweb...");
        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        thread::spawn(move || {
            let result = download_dicomweb_request(&request).map_err(|err| format!("{err:#}"));
            let _ = tx.send(result);
        });
        self.dicomweb_receiver = Some(rx);
    }

    fn start_dicomweb_group_download(&mut self, request: DicomWebGroupedLaunchRequest) {
        if self.dicomweb_receiver.is_some() {
            log::warn!("DICOMweb download already in progress.");
            return;
        }

        self.clear_load_error();
        self.sync_current_state_to_history();
        self.history_preload_receiver = None;
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.history_pushed_for_active_group = false;
        self.pending_gsps_overlays.clear();
        self.gsps_overlay_visible = false;
        log::info!("Loading grouped study from DICOMweb...");
        self.dicomweb_active_group_expected = None;
        self.dicomweb_active_group_paths.clear();
        self.dicomweb_active_pending_paths.clear();

        let (active_path_tx, active_path_rx) = mpsc::channel::<DicomWebGroupStreamUpdate>();
        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        thread::spawn(move || {
            let result = download_dicomweb_group_request(&request, |update| {
                let _ = active_path_tx.send(update);
            })
            .map_err(|err| format!("{err:#}"));
            let _ = tx.send(result);
        });
        self.dicomweb_active_path_receiver = Some(active_path_rx);
        self.dicomweb_receiver = Some(rx);
    }

    fn insert_loaded_mammo(
        &mut self,
        mut pending: PendingLoad,
        ctx: &egui::Context,
    ) -> Result<(), String> {
        let slot = preferred_mammo_slot(&pending.image, self.mammo_group.len(), |index| {
            self.mammo_group
                .get(index)
                .and_then(Option::as_ref)
                .is_none()
        })
        .or_else(|| self.mammo_group.iter().position(Option::is_none));

        let Some(slot_index) = slot else {
            return Err(format!(
                "Discarded streamed image: no available mammo slot (loaded={}, capacity={})",
                self.loaded_mammo_count(),
                self.mammo_group.len()
            ));
        };

        Self::attach_matching_gsps_overlay(&mut pending.image, &self.pending_gsps_overlays);

        let default_center = pending.image.window_center;
        let default_width = pending.image.window_width;
        let Some(color_image) =
            Self::render_image_frame(&pending.image, 0, default_center, default_width)
        else {
            return Err("Could not prepare preview for image (no decodable frame).".to_string());
        };

        let texture_name = format!("mammo-group:{}", pending.path.display());
        let texture = ctx.load_texture(texture_name, color_image, TextureOptions::LINEAR);
        let label = mammo_label(&pending.image, &pending.path);
        self.mammo_group[slot_index] = Some(MammoViewport {
            path: pending.path,
            image: pending.image,
            texture,
            label,
            window_center: default_center,
            window_width: default_width,
            current_frame: 0,
            zoom: 1.0,
            pan: egui::Vec2::ZERO,
            frame_scroll_accum: 0.0,
        });

        if self.loaded_mammo_count() == 1 {
            if let Some(first_loaded_slot) = self.mammo_group.iter().position(Option::is_some) {
                self.mammo_selected_index = first_loaded_slot;
            }
        }
        Ok(())
    }

    fn reorder_complete_mammo_group(&mut self) {
        if !self.mammo_group_complete() {
            return;
        }

        let selected_path = self
            .mammo_group
            .get(self.mammo_selected_index)
            .and_then(Option::as_ref)
            .map(|viewport| viewport.path.clone());

        let viewports = self
            .mammo_group
            .iter_mut()
            .filter_map(Option::take)
            .collect::<Vec<_>>();
        if viewports.len() != self.mammo_group.len() {
            self.mammo_group = viewports.into_iter().map(Some).collect();
            self.mammo_selected_index = self
                .mammo_selected_index
                .min(self.mammo_group.len().saturating_sub(1));
            return;
        }

        let ordered_indices = order_mammo_indices(&viewports, |viewport| &viewport.image);
        let (ordered, _, reordered) = Self::restore_ordered_items_or_log(
            viewports,
            ordered_indices,
            None,
            "reordering active group",
        );
        if !reordered {
            self.mammo_group = ordered.into_iter().map(Some).collect::<Vec<_>>();
            self.mammo_selected_index = self
                .mammo_selected_index
                .min(self.mammo_group.len().saturating_sub(1));
            return;
        }
        self.mammo_group = ordered.into_iter().map(Some).collect::<Vec<_>>();

        if let Some(selected_path) = selected_path {
            if let Some(index) = self.mammo_group.iter().position(|slot| {
                slot.as_ref()
                    .is_some_and(|viewport| viewport.path == selected_path)
            }) {
                self.mammo_selected_index = index;
                return;
            }
        }

        self.mammo_selected_index = self
            .mammo_group
            .iter()
            .position(Option::is_some)
            .unwrap_or(0);
    }

    fn poll_dicomweb_active_paths(&mut self, ctx: &egui::Context) {
        let mut keep_receiver = false;
        if let Some(receiver) = self.dicomweb_active_path_receiver.take() {
            keep_receiver = true;
            loop {
                match receiver.try_recv() {
                    Ok(DicomWebGroupStreamUpdate::ActiveGroupInstanceCount(count)) => {
                        self.dicomweb_active_group_expected = Some(count);
                        if Self::is_supported_multi_view_group_size(count) {
                            self.mammo_load_receiver = None;
                            self.mammo_load_sender = None;
                            self.history_pushed_for_active_group = false;
                            self.clear_single_viewer();
                            self.mammo_group = (0..count).map(|_| None).collect();
                            self.mammo_selected_index = 0;
                            self.cine_mode = false;
                            self.last_cine_advance = None;
                            log::info!(
                                "Loading grouped study from DICOMweb (streaming active group {}, {} views)...",
                                Self::multi_view_layout_label(count),
                                count
                            );
                            let (tx, rx) = mpsc::channel::<Result<PendingLoad, String>>();
                            self.mammo_load_sender = Some(tx);
                            self.mammo_load_receiver = Some(rx);
                        }
                    }
                    Ok(DicomWebGroupStreamUpdate::ActivePath(path)) => {
                        self.dicomweb_active_pending_paths.push_back(path);
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        if self
                            .dicomweb_active_group_expected
                            .is_some_and(Self::is_supported_multi_view_group_size)
                        {
                            self.mammo_load_sender = None;
                        }
                        keep_receiver = false;
                        break;
                    }
                }
            }
            if keep_receiver {
                self.dicomweb_active_path_receiver = Some(receiver);
            }
        }

        let expected = self.dicomweb_active_group_expected.unwrap_or(0);
        if let Some(path) = self.dicomweb_active_pending_paths.pop_front() {
            match expected {
                1 => {
                    self.dicomweb_active_group_paths.push(path.clone());
                    let _ = self.load_selected_paths(vec![path], ctx);
                }
                count if Self::is_supported_multi_view_group_size(count) => {
                    match classify_dicom_path(&path) {
                        Ok(DicomPathKind::Gsps) => match load_gsps_overlays(&path) {
                            Ok(overlays) => {
                                Self::merge_gsps_overlays(
                                    &mut self.pending_gsps_overlays,
                                    overlays,
                                );
                                for viewport in
                                    self.mammo_group.iter_mut().filter_map(Option::as_mut)
                                {
                                    Self::attach_matching_gsps_overlay(
                                        &mut viewport.image,
                                        &self.pending_gsps_overlays,
                                    );
                                }
                                self.sync_current_state_to_history();
                            }
                            Err(err) => {
                                log::warn!("Could not parse streamed GSPS input: {err:#}");
                            }
                        },
                        Ok(DicomPathKind::Image) | Err(_) => {
                            self.dicomweb_active_group_paths.push(path.clone());
                            if let Some(sender) = self.mammo_load_sender.as_ref().cloned() {
                                thread::spawn(move || {
                                    let result = match load_dicom(&path) {
                                        Ok(image) => Ok(PendingLoad { path, image }),
                                        Err(err) => {
                                            Err(format!("Error opening streamed DICOM: {err:#}"))
                                        }
                                    };
                                    let _ = sender.send(result);
                                });
                            } else {
                                self.set_load_error(
                                    "Streaming multi-view load channel was not available.",
                                );
                                log::error!("Streaming multi-view load channel not available.");
                                self.mammo_group.clear();
                                self.mammo_load_receiver = None;
                                self.mammo_load_sender = None;
                                self.history_pushed_for_active_group = false;
                                self.cine_mode = false;
                                self.dicomweb_active_group_paths.clear();
                                self.dicomweb_active_pending_paths.clear();
                                self.dicomweb_active_group_expected = None;
                                self.dicomweb_active_path_receiver = None;
                            }
                        }
                        Ok(DicomPathKind::StructuredReport) => {
                            log::info!(
                                "Ignoring streamed Structured Report in multi-view image mode."
                            );
                        }
                        Ok(DicomPathKind::Other) => {
                            log::warn!("Ignoring streamed non-image DICOM input.");
                        }
                    }
                }
                _ => {}
            }
        }

        if keep_receiver || !self.dicomweb_active_pending_paths.is_empty() {
            ctx.request_repaint_after(Duration::from_millis(16));
        }
    }

    fn poll_history_preload(&mut self, ctx: &egui::Context) {
        let Some(receiver) = self.history_preload_receiver.take() else {
            return;
        };

        let mut keep_receiver = true;
        loop {
            match receiver.try_recv() {
                Ok(result) => match result {
                    Ok(HistoryPreloadResult::Single { path, image }) => {
                        let image = *image;
                        let center = image.window_center;
                        let width = image.window_width;
                        let Some(color_image) = Self::render_image_frame(&image, 0, center, width)
                        else {
                            break;
                        };
                        let texture_name = format!("history-preload-single:{}", path.display());
                        let texture =
                            ctx.load_texture(texture_name, color_image, TextureOptions::LINEAR);
                        self.push_single_history_entry(
                            HistorySingleData {
                                path,
                                image,
                                texture,
                                window_center: center,
                                window_width: width,
                                current_frame: 0,
                                cine_fps: DEFAULT_CINE_FPS,
                            },
                            ctx,
                        );
                        self.move_current_history_to_front();
                        break;
                    }
                    Ok(HistoryPreloadResult::Group { viewports }) => {
                        let mut loaded = Vec::with_capacity(viewports.len());
                        for (path, image) in viewports {
                            let center = image.window_center;
                            let width = image.window_width;
                            let Some(color_image) =
                                Self::render_image_frame(&image, 0, center, width)
                            else {
                                log::warn!(
                                    "History preload skipped group viewport (instance {:?}).",
                                    image.instance_number
                                );
                                continue;
                            };
                            let texture_name = format!("history-preload-group:{}", path.display());
                            let texture =
                                ctx.load_texture(texture_name, color_image, TextureOptions::LINEAR);
                            let label = mammo_label(&image, &path);
                            loaded.push(MammoViewport {
                                path,
                                image,
                                texture,
                                label,
                                window_center: center,
                                window_width: width,
                                current_frame: 0,
                                zoom: 1.0,
                                pan: egui::Vec2::ZERO,
                                frame_scroll_accum: 0.0,
                            });
                        }
                        if Self::is_supported_multi_view_group_size(loaded.len()) {
                            let ordered_indices =
                                order_mammo_indices(&loaded, |viewport| &viewport.image);
                            let (ordered, _, _) = Self::restore_ordered_items_or_log(
                                loaded,
                                ordered_indices,
                                None,
                                "preloading history group",
                            );
                            self.push_group_history_entry(&ordered, 0, ctx);
                            self.move_current_history_to_front();
                        }
                        break;
                    }
                    Ok(HistoryPreloadResult::Report { path, report }) => {
                        self.push_report_history_entry(path, *report, ctx);
                        self.move_current_history_to_front();
                        break;
                    }
                    Err(err) => {
                        log::warn!("History preload skipped: {err}");
                    }
                },
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    keep_receiver = false;
                    break;
                }
            }
        }

        if keep_receiver {
            self.history_preload_receiver = Some(receiver);
            ctx.request_repaint_after(Duration::from_millis(16));
        }
    }

    fn poll_dicomweb_download(&mut self, ctx: &egui::Context) {
        let Some(receiver) = self.dicomweb_receiver.take() else {
            return;
        };

        match receiver.try_recv() {
            Ok(result) => match result {
                Ok(download_result) => match download_result {
                    DicomWebDownloadResult::Single(paths) => {
                        let _ = self.load_selected_paths(paths, ctx);
                    }
                    DicomWebDownloadResult::Grouped { groups, open_group } => {
                        let prepared_groups = groups
                            .iter()
                            .map(|group| Self::prepare_load_paths(group.clone()))
                            .collect::<Vec<_>>();
                        let validated_open_group = if prepared_groups.is_empty() {
                            0
                        } else {
                            open_group.min(prepared_groups.len().saturating_sub(1))
                        };
                        let active_group_len = prepared_groups
                            .get(validated_open_group)
                            .map(|group| group.image_paths.len())
                            .unwrap_or(0);
                        let active_group_paths = prepared_groups
                            .get(validated_open_group)
                            .map(|group| group.image_paths.clone())
                            .unwrap_or_default();
                        let streamed_count = self.dicomweb_active_group_paths.len();
                        let streaming_started = streamed_count > 0;
                        let streamed_active_complete = streamed_count >= active_group_len
                            && (active_group_len == 1
                                || Self::is_supported_multi_view_group_size(active_group_len))
                            && self.dicomweb_active_pending_paths.is_empty();
                        let grouped_ready;

                        if !streamed_active_complete && !streaming_started {
                            self.load_local_groups(groups, validated_open_group, ctx);
                            grouped_ready =
                                self.displayed_study_matches_paths(active_group_paths.as_slice());
                        } else {
                            self.preload_non_active_groups_into_history(
                                &prepared_groups,
                                validated_open_group,
                            );
                            if Self::is_supported_multi_view_group_size(active_group_len)
                                && self.mammo_group_complete()
                                && !self.history_pushed_for_active_group
                            {
                                self.reorder_complete_mammo_group();
                                let loaded = self
                                    .mammo_group
                                    .iter()
                                    .filter_map(Option::as_ref)
                                    .cloned()
                                    .collect::<Vec<_>>();
                                self.push_group_history_entry(
                                    &loaded,
                                    self.mammo_selected_index,
                                    ctx,
                                );
                                self.history_pushed_for_active_group = true;
                            }
                            self.move_current_history_to_front();
                            grouped_ready =
                                self.displayed_study_matches_paths(active_group_paths.as_slice());
                        }

                        if streamed_active_complete || !streaming_started {
                            self.dicomweb_active_group_expected = None;
                            self.dicomweb_active_group_paths.clear();
                            self.dicomweb_active_pending_paths.clear();
                            self.dicomweb_active_path_receiver = None;
                            self.mammo_load_sender = None;
                            self.history_pushed_for_active_group = false;
                        }
                        if grouped_ready {
                            self.clear_load_error();
                            log::info!("Loaded grouped study from DICOMweb.");
                        }
                    }
                },
                Err(err) => {
                    self.set_load_error("DICOMweb request failed.");
                    log::error!("DICOMweb error: {err}");
                    self.dicomweb_active_group_expected = None;
                    self.dicomweb_active_group_paths.clear();
                    self.dicomweb_active_pending_paths.clear();
                    self.dicomweb_active_path_receiver = None;
                    self.mammo_load_sender = None;
                    self.history_pushed_for_active_group = false;
                }
            },
            Err(TryRecvError::Empty) => {
                self.dicomweb_receiver = Some(receiver);
                ctx.request_repaint_after(Duration::from_millis(16));
            }
            Err(TryRecvError::Disconnected) => {
                self.set_load_error("DICOMweb download worker disconnected.");
                log::error!("DICOMweb download worker disconnected.");
                self.dicomweb_active_group_expected = None;
                self.dicomweb_active_group_paths.clear();
                self.dicomweb_active_pending_paths.clear();
                self.dicomweb_active_path_receiver = None;
                self.mammo_load_sender = None;
                self.history_pushed_for_active_group = false;
            }
        }
    }

    fn poll_mammo_group_load(&mut self, ctx: &egui::Context) {
        let Some(receiver) = self.mammo_load_receiver.take() else {
            return;
        };

        let mut had_error = false;
        let mut should_continue = true;
        match receiver.try_recv() {
            Ok(result) => match result {
                Ok(pending) => {
                    if let Err(err) = self.insert_loaded_mammo(pending, ctx) {
                        self.set_load_error("Failed to load multi-view DICOM group.");
                        log::error!("{err}");
                        self.mammo_group.clear();
                        self.mammo_load_receiver = None;
                        self.mammo_load_sender = None;
                        self.history_pushed_for_active_group = false;
                        self.cine_mode = false;
                        if self.dicomweb_active_group_expected.is_some()
                            || self.dicomweb_active_path_receiver.is_some()
                            || !self.dicomweb_active_pending_paths.is_empty()
                        {
                            self.dicomweb_active_group_expected = None;
                            self.dicomweb_active_group_paths.clear();
                            self.dicomweb_active_pending_paths.clear();
                            self.dicomweb_active_path_receiver = None;
                        }
                        return;
                    }
                    self.reorder_complete_mammo_group();
                    self.clear_load_error();
                    if self.mammo_group_complete()
                        && (self
                            .dicomweb_active_group_expected
                            .is_some_and(Self::is_supported_multi_view_group_size)
                            || self.dicomweb_active_path_receiver.is_some())
                        && !self.history_pushed_for_active_group
                    {
                        let loaded = self
                            .mammo_group
                            .iter()
                            .filter_map(Option::as_ref)
                            .cloned()
                            .collect::<Vec<_>>();
                        self.push_group_history_entry(&loaded, self.mammo_selected_index, ctx);
                        self.move_current_history_to_front();
                        self.history_pushed_for_active_group = true;
                    }
                    ctx.request_repaint();
                }
                Err(err) => {
                    self.set_load_error("Failed to load multi-view DICOM group.");
                    log::error!("{err}");
                    self.mammo_group.clear();
                    self.history_pushed_for_active_group = false;
                    if self.dicomweb_active_group_expected.is_some()
                        || self.dicomweb_active_path_receiver.is_some()
                        || !self.dicomweb_active_pending_paths.is_empty()
                    {
                        self.dicomweb_active_group_expected = None;
                        self.dicomweb_active_group_paths.clear();
                        self.dicomweb_active_pending_paths.clear();
                        self.dicomweb_active_path_receiver = None;
                    }
                    had_error = true;
                    should_continue = false;
                }
            },
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                should_continue = false;
            }
        }

        if had_error {
            self.mammo_load_receiver = None;
            self.mammo_load_sender = None;
            self.cine_mode = false;
            return;
        }

        if should_continue {
            self.mammo_load_receiver = Some(receiver);
            ctx.request_repaint_after(Duration::from_millis(16));
            return;
        }

        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        if self.mammo_group_complete() {
            self.reorder_complete_mammo_group();
            self.clear_load_error();
            if !self.history_pushed_for_active_group {
                let loaded = self
                    .mammo_group
                    .iter()
                    .filter_map(Option::as_ref)
                    .cloned()
                    .collect::<Vec<_>>();
                self.push_group_history_entry(&loaded, self.mammo_selected_index, ctx);
            }
        } else {
            self.set_load_error(
                "Multi-view group load incomplete: worker exited before all images were received.",
            );
            log::warn!(
                "Multi-view group load incomplete: worker exited before all images were received."
            );
        }
        ctx.request_repaint();
    }

    fn poll_single_load(&mut self, ctx: &egui::Context) {
        let Some(receiver) = self.single_load_receiver.take() else {
            return;
        };

        match receiver.try_recv() {
            Ok(result) => {
                match result {
                    Ok(PendingSingleLoad::Image(pending)) => {
                        self.apply_loaded_single(pending.path, pending.image, ctx);
                        self.clear_load_error();
                    }
                    Ok(PendingSingleLoad::StructuredReport { path, report }) => {
                        self.apply_loaded_structured_report(path, report, ctx);
                        self.clear_load_error();
                    }
                    Err(err) => {
                        self.set_load_error("Failed to load selected DICOM.");
                        log::error!("{err}");
                    }
                }
                self.single_load_receiver = None;
                ctx.request_repaint();
            }
            Err(TryRecvError::Empty) => {
                self.single_load_receiver = Some(receiver);
                ctx.request_repaint_after(Duration::from_millis(16));
            }
            Err(TryRecvError::Disconnected) => {
                self.single_load_receiver = None;
                self.set_load_error("Selected DICOM load did not complete.");
                log::error!("Single-image load incomplete: worker exited before sending a result.");
                ctx.request_repaint();
            }
        }
    }

    fn open_dicoms(&mut self, ctx: &egui::Context) {
        let picked = rfd::FileDialog::new()
            .add_filter("DICOM", &["dcm"])
            .pick_files();

        if let Some(paths) = picked {
            self.queue_local_paths_open(paths);
            ctx.set_cursor_icon(egui::CursorIcon::Progress);
            ctx.request_repaint();
        }
    }

    fn show_file_drop_overlay(&self, ctx: &egui::Context, hovered_files: &[egui::HoveredFile]) {
        if hovered_files.is_empty() {
            return;
        }

        let overlay_rect = ctx.screen_rect();
        let painter = ctx.layer_painter(egui::LayerId::new(
            egui::Order::Foreground,
            egui::Id::new("file-drop-backdrop"),
        ));
        painter.rect_filled(overlay_rect, 0.0, egui::Color32::from_black_alpha(168));

        let heading = Self::file_drop_overlay_heading(hovered_files);
        egui::Area::new(egui::Id::new("file-drop-overlay"))
            .order(egui::Order::Foreground)
            .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
            .show(ctx, |ui| {
                egui::Frame::none()
                    .fill(egui::Color32::from_black_alpha(228))
                    .stroke(egui::Stroke::NONE)
                    .rounding(egui::Rounding::same(12.0))
                    .inner_margin(egui::Margin::symmetric(24.0, 20.0))
                    .show(ui, |ui| {
                        ui.set_min_width(FILE_DROP_OVERLAY_WIDTH);
                        ui.set_max_width(FILE_DROP_OVERLAY_WIDTH);
                        ui.vertical_centered(|ui| {
                            ui.label(egui::RichText::new(heading).strong().size(24.0));
                            ui.add_space(6.0);
                            ui.label(
                                egui::RichText::new("Drop DICOM files anywhere in the window.")
                                    .color(egui::Color32::from_gray(196)),
                            );
                        });
                    });
            });
    }

    fn apply_prepared_load_paths(
        &mut self,
        prepared: PreparedLoadPaths,
        ctx: &egui::Context,
    ) -> Result<(), ()> {
        let paths = prepared.image_paths;
        let structured_report_paths = prepared.structured_report_paths;
        if !paths.is_empty() || !structured_report_paths.is_empty() || prepared.gsps_files_found > 0
        {
            self.sync_current_state_to_history();
        }
        self.history_preload_receiver = None;
        self.pending_gsps_overlays = prepared.gsps_overlays;
        self.gsps_overlay_visible = false;

        if paths.is_empty() {
            if let Some((report_path, remaining_reports)) = structured_report_paths
                .split_first()
                .map(|(first, rest)| (first.clone(), rest))
            {
                self.stage_structured_report_history_entries(remaining_reports, ctx);
                self.load_structured_report_path(report_path, ctx);
                return Ok(());
            }
            if prepared.gsps_files_found > 0 {
                self.set_load_error("GSPS detected, but no displayable DICOM image was selected.");
                log::warn!("GSPS detected, but no displayable DICOM image was selected.");
                ctx.request_repaint();
                return Err(());
            }
            if prepared.other_files_found > 0 {
                self.set_load_error(
                    "Selected DICOM objects are not displayable images or structured reports.",
                );
                log::warn!(
                    "Selected DICOM objects are not displayable images or structured reports."
                );
                ctx.request_repaint();
                return Err(());
            }
            return Err(());
        }

        if !structured_report_paths.is_empty() {
            self.stage_structured_report_history_entries(&structured_report_paths, ctx);
            log::info!(
                "Opening {} image DICOM(s) and staging {} structured report object(s) as separate history entries.",
                paths.len(),
                structured_report_paths.len()
            );
        }

        match paths.len() {
            0 => Err(()),
            1 => {
                self.single_load_receiver = None;
                self.mammo_load_receiver = None;
                self.mammo_load_sender = None;
                self.history_pushed_for_active_group = false;
                if let Some(path) = paths.into_iter().next() {
                    self.load_path(path, ctx);
                }
                Ok(())
            }
            count if Self::is_supported_multi_view_group_size(count) => {
                self.load_mammo_group_paths(paths, ctx);
                Ok(())
            }
            other => {
                let err = Self::format_select_paths_count_error(other);
                self.set_load_error(err.clone());
                log::warn!("{err}");
                ctx.request_repaint();
                Err(())
            }
        }
    }

    fn load_selected_paths(&mut self, paths: Vec<PathBuf>, ctx: &egui::Context) -> Result<(), ()> {
        self.clear_load_error();
        let prepared = Self::prepare_load_paths(paths);
        self.apply_prepared_load_paths(prepared, ctx)
    }

    fn stage_structured_report_history_entries(
        &mut self,
        report_paths: &[PathBuf],
        ctx: &egui::Context,
    ) {
        for path in report_paths {
            match load_structured_report(path) {
                Ok(report) => self.push_report_history_entry(path.clone(), report, ctx),
                Err(err) => {
                    log::warn!(
                        "Could not stage Structured Report {} into history: {err:#}",
                        path.display()
                    );
                }
            }
        }
    }

    fn load_path(&mut self, path: PathBuf, ctx: &egui::Context) {
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.single_load_receiver = None;
        self.history_pushed_for_active_group = false;
        self.clear_load_error();
        log::info!("Loading selected DICOM...");
        log::info!(target: "perf", "single-open started");
        let (tx, rx) = mpsc::channel::<Result<PendingSingleLoad, String>>();
        thread::spawn(move || {
            let result = match load_dicom(&path) {
                Ok(image) => {
                    log::info!(target: "perf", "single-open dicom-load completed");
                    Ok(PendingSingleLoad::Image(PendingLoad { path, image }))
                }
                Err(err) => Err(format!("Error opening selected DICOM: {err:#}")),
            };
            let _ = tx.send(result);
        });
        self.single_load_receiver = Some(rx);
        ctx.request_repaint();
    }

    fn load_structured_report_path(&mut self, path: PathBuf, ctx: &egui::Context) {
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.single_load_receiver = None;
        self.history_pushed_for_active_group = false;
        self.clear_load_error();
        log::info!("Loading selected Structured Report...");
        let (tx, rx) = mpsc::channel::<Result<PendingSingleLoad, String>>();
        thread::spawn(move || {
            let result = match load_structured_report(&path) {
                Ok(report) => Ok(PendingSingleLoad::StructuredReport { path, report }),
                Err(err) => Err(format!("Error opening selected Structured Report: {err:#}")),
            };
            let _ = tx.send(result);
        });
        self.single_load_receiver = Some(rx);
        ctx.request_repaint();
    }

    fn apply_loaded_single(&mut self, path: PathBuf, image: DicomImage, ctx: &egui::Context) {
        let mut image = image;
        Self::attach_matching_gsps_overlay(&mut image, &self.pending_gsps_overlays);
        self.gsps_overlay_visible = false;
        self.clear_load_error();

        self.window_center = image.window_center;
        self.window_width = image.window_width;
        self.current_frame = 0;
        self.cine_mode = false;
        self.last_cine_advance = None;
        self.cine_fps = image
            .recommended_cine_fps
            .unwrap_or(DEFAULT_CINE_FPS)
            .clamp(1.0, 120.0);

        let history_image = image.clone();
        self.report = None;
        self.image = Some(image);
        self.current_single_path = Some(path.clone());
        self.mammo_group.clear();
        self.mammo_selected_index = 0;
        self.reset_single_view_transform();
        self.single_view_frame_scroll_accum = 0.0;
        self.rebuild_texture(ctx);
        log::info!(target: "perf", "single-open completed");
        let history_texture = self.texture.clone();
        if let Some(texture) = history_texture.as_ref() {
            self.push_single_history_entry(
                HistorySingleData {
                    path: path.clone(),
                    image: history_image,
                    texture: texture.clone(),
                    window_center: self.window_center,
                    window_width: self.window_width,
                    current_frame: self.current_frame,
                    cine_fps: self.cine_fps,
                },
                ctx,
            );
        }
        log::info!("Loaded selected DICOM.");
    }

    fn apply_loaded_structured_report(
        &mut self,
        path: PathBuf,
        report: StructuredReportDocument,
        ctx: &egui::Context,
    ) {
        self.clear_single_viewer();
        self.clear_load_error();
        self.push_report_history_entry(path.clone(), report.clone(), ctx);
        self.report = Some(report);
        self.current_single_path = Some(path);
        self.pending_gsps_overlays.clear();
        ctx.request_repaint();
        log::info!("Loaded selected Structured Report.");
    }

    fn load_mammo_group_paths(&mut self, paths: Vec<PathBuf>, ctx: &egui::Context) {
        if !Self::is_supported_multi_view_group_size(paths.len()) {
            let err = Self::format_multi_view_size_error(paths.len());
            self.set_load_error(err.clone());
            log::warn!("{err}");
            return;
        }

        let group_len = paths.len();
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.single_load_receiver = None;
        self.history_pushed_for_active_group = false;
        self.clear_single_viewer();
        self.clear_load_error();
        self.mammo_group = (0..group_len).map(|_| None).collect();
        self.mammo_selected_index = 0;
        self.cine_mode = false;
        self.last_cine_advance = None;
        log::info!(
            "Loading {} multi-view group...",
            Self::multi_view_layout_label(group_len)
        );

        let (tx, rx) = mpsc::channel::<Result<PendingLoad, String>>();
        thread::spawn(move || {
            for path in paths {
                match load_dicom(&path) {
                    Ok(image) => {
                        let _ = tx.send(Ok(PendingLoad { path, image }));
                    }
                    Err(err) => {
                        let _ = tx.send(Err(format!("Error opening DICOM in group: {err:#}")));
                        return;
                    }
                }
            }
        });
        self.mammo_load_receiver = Some(rx);
        ctx.request_repaint();
    }

    fn toggle_cine_mode(&mut self) {
        if let Some(image) = self.image.as_ref() {
            if image.frame_count() <= 1 {
                self.cine_mode = false;
                log::debug!("Cine mode requires a multi-frame DICOM.");
                return;
            }
            self.cine_mode = !self.cine_mode;
            self.last_cine_advance = Some(Instant::now());
            return;
        }

        if self.loaded_mammo_count() == 0 {
            self.cine_mode = false;
            return;
        }

        if !self.mammo_group_complete() {
            self.cine_mode = false;
            log::debug!("Multi-view cine mode requires all views to be loaded.");
            return;
        }

        let frame_count = self.mammo_group_common_frame_count();
        if frame_count <= 1 {
            self.cine_mode = false;
            log::debug!("Multi-view cine mode requires all views to be multi-frame.");
            return;
        }

        let enabling = !self.cine_mode;
        self.cine_mode = enabling;
        self.last_cine_advance = Some(Instant::now());
        if enabling {
            let start_frame = self
                .selected_mammo_frame_index()
                .min(frame_count.saturating_sub(1));
            let _ = self.set_mammo_group_frame(start_frame);
        }
    }

    fn advance_cine_if_needed(&mut self, ctx: &egui::Context) {
        if !self.cine_mode {
            return;
        }

        let frame_count = if let Some(image) = self.image.as_ref() {
            image.frame_count()
        } else {
            self.mammo_group_common_frame_count()
        };

        if frame_count <= 1 {
            self.cine_mode = false;
            return;
        }

        let fps = self.cine_fps.clamp(1.0, 120.0);
        let frame_interval = Duration::from_secs_f32(1.0 / fps);
        let now = Instant::now();
        let last = self.last_cine_advance.unwrap_or(now);
        let elapsed = now.duration_since(last);

        if elapsed >= frame_interval {
            let frames_to_advance = ((elapsed.as_secs_f32() * fps).floor() as usize).max(1);
            if self.image.is_some() {
                self.current_frame = (self.current_frame + frames_to_advance) % frame_count;
            } else {
                let next_frame =
                    (self.selected_mammo_frame_index() + frames_to_advance) % frame_count;
                let _ = self.set_mammo_group_frame(next_frame);
            }
            self.last_cine_advance = Some(now);
            if self.image.is_some() {
                self.rebuild_texture(ctx);
            }
        }

        ctx.request_repaint_after(Duration::from_millis(8));
    }

    fn render_image_frame(
        image: &DicomImage,
        frame_index: usize,
        window_center: f32,
        window_width: f32,
    ) -> Option<ColorImage> {
        if image.is_monochrome() {
            let frame_pixels = image.frame_mono_pixels(frame_index)?;
            Some(render_window_level(
                image.width,
                image.height,
                frame_pixels.as_ref(),
                image.invert,
                window_center,
                window_width,
            ))
        } else {
            let frame_pixels = image.frame_rgb_pixels(frame_index)?;
            Some(render_rgb(
                image.width,
                image.height,
                frame_pixels.as_ref(),
                image.samples_per_pixel,
            ))
        }
    }

    fn rebuild_texture(&mut self, ctx: &egui::Context) {
        let had_renderable_image = self
            .image
            .as_ref()
            .map(|image| image.frame_count() > 0)
            .unwrap_or(false);
        let prepared = self.image.as_ref().and_then(|image| {
            let frame_count = image.frame_count();
            if frame_count == 0 {
                return None;
            }

            let frame_index = self.current_frame.min(frame_count.saturating_sub(1));
            let color_image = Self::render_image_frame(
                image,
                frame_index,
                self.window_center,
                self.window_width,
            )?;
            Some((color_image, frame_index))
        });

        let Some((color_image, frame_index)) = prepared else {
            if had_renderable_image {
                self.frame_wait_pending = true;
                ctx.request_repaint_after(Duration::from_millis(16));
            } else {
                self.texture = None;
                self.frame_wait_pending = false;
            }
            return;
        };

        self.frame_wait_pending = false;
        self.current_frame = frame_index;
        if let Some(texture) = self.texture.as_mut() {
            texture.set(color_image, TextureOptions::LINEAR);
        } else {
            self.texture =
                Some(ctx.load_texture("dicom-image", color_image, TextureOptions::LINEAR));
        }
    }

    fn selected_mammo_viewport(&self) -> Option<&MammoViewport> {
        if self.loaded_mammo_count() == 0 {
            return None;
        }
        let selected = self
            .mammo_selected_index
            .min(self.mammo_group.len().saturating_sub(1));
        self.mammo_group
            .get(selected)
            .and_then(Option::as_ref)
            .or_else(|| self.loaded_mammo_viewports().next())
    }

    fn active_image(&self) -> Option<&DicomImage> {
        if let Some(image) = self.image.as_ref() {
            Some(image)
        } else {
            self.selected_mammo_viewport()
                .map(|viewport| &viewport.image)
        }
    }

    fn active_metadata(&self) -> Option<&[(String, String)]> {
        if let Some(image) = self.active_image() {
            Some(image.metadata.as_slice())
        } else {
            self.report
                .as_ref()
                .map(|report| report.metadata.as_slice())
        }
    }

    fn displayed_study_matches_paths(&self, image_paths: &[PathBuf]) -> bool {
        match image_paths {
            [path] => self
                .current_single_path
                .as_ref()
                .is_some_and(|current| current == path),
            paths if Self::is_supported_multi_view_group_size(paths.len()) => {
                if !self.mammo_group_complete() {
                    return false;
                }

                let loaded_paths = self
                    .loaded_mammo_viewports()
                    .map(|viewport| viewport.path.clone())
                    .collect::<HashSet<_>>();
                loaded_paths.len() == paths.len()
                    && paths.iter().all(|path| loaded_paths.contains(path))
            }
            _ => false,
        }
    }

    fn active_viewport_state(&self) -> Option<ActiveViewportState> {
        if let Some(image) = self.image.as_ref() {
            Some(ActiveViewportState {
                is_single: true,
                is_monochrome: image.is_monochrome(),
                min_value: image.min_value,
                max_value: image.max_value,
                frame_count: image.frame_count(),
                default_center: image.window_center,
                default_width: image.window_width,
                window_center: self.window_center,
                window_width: self.window_width,
                current_frame: self.current_frame,
            })
        } else {
            let group_frame_count = self.mammo_group_common_frame_count();
            self.selected_mammo_viewport().map(|viewport| {
                let current_frame = if group_frame_count == 0 {
                    0
                } else {
                    viewport
                        .current_frame
                        .min(group_frame_count.saturating_sub(1))
                };
                ActiveViewportState {
                    is_single: false,
                    is_monochrome: viewport.image.is_monochrome(),
                    min_value: viewport.image.min_value,
                    max_value: viewport.image.max_value,
                    frame_count: group_frame_count,
                    default_center: viewport.image.window_center,
                    default_width: viewport.image.window_width,
                    window_center: viewport.window_center,
                    window_width: viewport.window_width,
                    current_frame,
                }
            })
        }
    }

    fn apply_active_viewport_state(&mut self, state: &ActiveViewportState, ctx: &egui::Context) {
        if state.is_single {
            self.window_center = state.window_center;
            self.window_width = state.window_width.max(1.0);
            self.current_frame = state.current_frame;
            self.rebuild_texture(ctx);
        } else if self.cine_mode {
            let frame_index = if state.frame_count == 0 {
                0
            } else {
                state.current_frame.min(state.frame_count.saturating_sub(1))
            };
            let _ = self.set_mammo_group_frame(frame_index);
            self.last_cine_advance = Some(Instant::now());
        } else if let Some(viewport) = self.selected_mammo_viewport_mut() {
            viewport.window_center = state.window_center;
            viewport.window_width = state.window_width.max(1.0);
            if state.frame_count == 0 {
                viewport.current_frame = 0;
            } else {
                viewport.current_frame = state.current_frame.min(state.frame_count - 1);
            }
            if self.rebuild_selected_mammo_texture() {
                ctx.request_repaint_after(Duration::from_millis(16));
            }
        }
    }

    fn selected_mammo_viewport_mut(&mut self) -> Option<&mut MammoViewport> {
        if self.loaded_mammo_count() == 0 {
            return None;
        }
        let selected = self
            .mammo_selected_index
            .min(self.mammo_group.len().saturating_sub(1));
        if selected < self.mammo_group.len() && self.mammo_group[selected].is_some() {
            return self.mammo_group[selected].as_mut();
        }
        self.mammo_group.iter_mut().find_map(Option::as_mut)
    }

    fn rebuild_selected_mammo_texture(&mut self) -> bool {
        let Some(viewport) = self.selected_mammo_viewport_mut() else {
            return false;
        };
        let frame_count = viewport.image.frame_count();
        if frame_count == 0 {
            return false;
        }

        viewport.current_frame = viewport.current_frame.min(frame_count.saturating_sub(1));
        let Some(color_image) = Self::render_image_frame(
            &viewport.image,
            viewport.current_frame,
            viewport.window_center,
            viewport.window_width,
        ) else {
            self.frame_wait_pending = true;
            return true;
        };
        viewport.texture.set(color_image, TextureOptions::LINEAR);
        self.frame_wait_pending = false;
        false
    }

    fn mammo_base_center(viewport_rect: egui::Rect, draw_width: f32, index: usize) -> egui::Pos2 {
        let mut base_center = viewport_rect.center();
        if draw_width < viewport_rect.width() {
            let x_slack = (viewport_rect.width() - draw_width) * 0.5;
            match mammo_image_align(index) {
                egui::Align::Min => base_center.x -= x_slack,
                egui::Align::Center => {}
                egui::Align::Max => base_center.x += x_slack,
            }
        }
        base_center
    }

    fn gsps_point_to_screen(
        point: (f32, f32),
        units: GspsUnits,
        image_rect: egui::Rect,
        image_width: usize,
        image_height: usize,
    ) -> egui::Pos2 {
        let (x, y) = point;
        let (norm_x, norm_y) = match units {
            GspsUnits::Display => (x, y),
            GspsUnits::Pixel => {
                let width = image_width.max(1) as f32;
                let height = image_height.max(1) as f32;
                (x / width, y / height)
            }
        };
        egui::pos2(
            image_rect.left() + norm_x * image_rect.width(),
            image_rect.top() + norm_y * image_rect.height(),
        )
    }

    fn draw_gsps_overlay(
        painter: &egui::Painter,
        image_rect: egui::Rect,
        image: &DicomImage,
        frame_index: usize,
    ) {
        let Some(overlay) = image.gsps_overlay.as_ref() else {
            return;
        };
        if overlay.is_empty() {
            return;
        }

        let stroke = egui::Stroke::new(1.6, PERSPECTA_BRAND_BLUE);
        let marker_half = (image_rect.width().min(image_rect.height()) * 0.008).clamp(2.0, 5.0);

        for graphic in overlay.graphics_for_frame(frame_index) {
            match graphic {
                GspsGraphic::Point { x, y, units } => {
                    let center = Self::gsps_point_to_screen(
                        (*x, *y),
                        *units,
                        image_rect,
                        image.width,
                        image.height,
                    );
                    painter.line_segment(
                        [
                            egui::pos2(center.x - marker_half, center.y),
                            egui::pos2(center.x + marker_half, center.y),
                        ],
                        stroke,
                    );
                    painter.line_segment(
                        [
                            egui::pos2(center.x, center.y - marker_half),
                            egui::pos2(center.x, center.y + marker_half),
                        ],
                        stroke,
                    );
                }
                GspsGraphic::Polyline {
                    points,
                    units,
                    closed,
                } => {
                    if points.len() < 2 {
                        continue;
                    }
                    let screen_points = points
                        .iter()
                        .map(|point| {
                            Self::gsps_point_to_screen(
                                *point,
                                *units,
                                image_rect,
                                image.width,
                                image.height,
                            )
                        })
                        .collect::<Vec<_>>();
                    for pair in screen_points.windows(2) {
                        painter.line_segment([pair[0], pair[1]], stroke);
                    }
                    if *closed && screen_points.len() > 2 {
                        if let (Some(first), Some(last)) = (
                            screen_points.first().copied(),
                            screen_points.last().copied(),
                        ) {
                            painter.line_segment([last, first], stroke);
                        }
                    }
                }
            }
        }
    }

    fn show_structured_report_view(&self, ui: &mut egui::Ui, report: &StructuredReportDocument) {
        ui.add_space(8.0);
        ui.vertical_centered(|ui| {
            ui.label(egui::RichText::new(&report.title).strong().size(24.0));
            ui.add_space(4.0);
            let mut summary_fields = Vec::new();
            if let Some(modality) = report.modality.as_deref() {
                summary_fields.push(format!("Modality: {modality}"));
            }
            if let Some(completion_flag) = report.completion_flag.as_deref() {
                summary_fields.push(format!("Completion: {completion_flag}"));
            }
            if let Some(verification_flag) = report.verification_flag.as_deref() {
                summary_fields.push(format!("Verification: {verification_flag}"));
            }
            if !summary_fields.is_empty() {
                ui.add(
                    egui::Label::new(summary_fields.join("  |  "))
                        .wrap()
                        .halign(egui::Align::Center),
                );
            }
        });
        ui.add_space(12.0);

        egui::ScrollArea::vertical()
            .id_salt("structured-report-scroll")
            .show(ui, |ui| {
                ui.vertical_centered(|ui| {
                    ui.set_max_width(720.0);

                    if report.content.is_empty() {
                        ui.add(
                            egui::Label::new(
                                "This Structured Report does not contain a parsable Content Sequence.",
                            )
                            .wrap()
                            .halign(egui::Align::Center),
                        );
                        return;
                    }

                    for node in &report.content {
                        Self::show_structured_report_node(ui, node, 0);
                        ui.add_space(6.0);
                    }
                });
            });
    }

    fn show_structured_report_node(ui: &mut egui::Ui, node: &StructuredReportNode, depth: usize) {
        let header = match node.relationship_type.as_deref() {
            Some(relationship_type) => format!("{relationship_type}  {}", node.label),
            None => node.label.clone(),
        };

        if node.children.is_empty() {
            ui.vertical_centered(|ui| {
                ui.label(egui::RichText::new(header).strong());
                if let Some(value) = node.value.as_deref() {
                    ui.add(egui::Label::new(value).wrap().halign(egui::Align::Center));
                }
            });
            return;
        }

        egui::CollapsingHeader::new(header)
            .default_open(depth < 2)
            .show(ui, |ui| {
                ui.vertical_centered(|ui| {
                    if let Some(value) = node.value.as_deref() {
                        ui.add(egui::Label::new(value).wrap().halign(egui::Align::Center));
                        ui.add_space(4.0);
                    }
                    for child in &node.children {
                        Self::show_structured_report_node(ui, child, depth.saturating_add(1));
                        ui.add_space(4.0);
                    }
                });
            });
    }

    fn apply_window_level_drag(
        window_center: &mut f32,
        window_width: &mut f32,
        min_value: i32,
        max_value: i32,
        drag_delta: egui::Vec2,
    ) -> bool {
        if drag_delta == egui::Vec2::ZERO {
            return false;
        }

        let span = (max_value as i64 - min_value as i64).unsigned_abs() as f32;
        let sensitivity = (span / 512.0).clamp(0.25, 256.0);
        let old_center = *window_center;
        let old_width = *window_width;

        *window_center += -drag_delta.y * sensitivity;
        *window_width = (*window_width + drag_delta.x * sensitivity).max(1.0);

        (*window_center - old_center).abs() > f32::EPSILON
            || (*window_width - old_width).abs() > f32::EPSILON
    }

    fn add_value_control_no_border<'a>(
        ui: &mut egui::Ui,
        size: [f32; 2],
        drag_value: egui::DragValue<'a>,
    ) -> egui::Response {
        ui.scope(|ui| {
            Self::apply_no_border_visuals(ui.visuals_mut());
            ui.add_sized(size, drag_value)
        })
        .inner
    }

    fn add_action_control_button_no_border(
        ui: &mut egui::Ui,
        size: [f32; 2],
        text: impl Into<egui::WidgetText>,
    ) -> egui::Response {
        ui.scope(|ui| {
            Self::apply_no_border_visuals(ui.visuals_mut());
            ui.add_sized(size, egui::Button::new(text))
        })
        .inner
    }

    fn apply_no_border_visuals(visuals: &mut egui::Visuals) {
        let noninteractive_bg = visuals.widgets.noninteractive.weak_bg_fill;
        let inactive_bg = visuals.widgets.inactive.weak_bg_fill;
        let hovered_bg = visuals.widgets.hovered.weak_bg_fill;
        let active_bg = visuals.widgets.active.weak_bg_fill;

        visuals.widgets.noninteractive.bg_fill = noninteractive_bg;
        visuals.widgets.noninteractive.weak_bg_fill = noninteractive_bg;
        visuals.widgets.noninteractive.bg_stroke = egui::Stroke::NONE;

        visuals.widgets.inactive.bg_fill = inactive_bg;
        visuals.widgets.inactive.weak_bg_fill = inactive_bg;
        visuals.widgets.inactive.bg_stroke = egui::Stroke::NONE;

        visuals.widgets.hovered.bg_fill = hovered_bg;
        visuals.widgets.hovered.weak_bg_fill = hovered_bg;
        visuals.widgets.hovered.bg_stroke = egui::Stroke::NONE;

        visuals.widgets.active.bg_fill = active_bg;
        visuals.widgets.active.weak_bg_fill = active_bg;
        visuals.widgets.active.bg_stroke = egui::Stroke::NONE;
    }

    fn frame_step_from_scroll(scroll_accum: &mut f32, scroll: f32) -> i32 {
        const DEAD_ZONE: f32 = 0.5;
        const PIXELS_PER_FRAME_STEP: f32 = 30.0;

        if scroll.abs() <= DEAD_ZONE {
            return 0;
        }

        // Reset stale residuals when the user reverses scroll direction.
        if *scroll_accum != 0.0 && scroll.signum() != scroll_accum.signum() {
            *scroll_accum = 0.0;
        }
        *scroll_accum += scroll;

        let raw_steps = (*scroll_accum / PIXELS_PER_FRAME_STEP).trunc() as i32;
        if raw_steps == 0 {
            return 0;
        }

        *scroll_accum -= raw_steps as f32 * PIXELS_PER_FRAME_STEP;
        -raw_steps
    }

    fn dominant_scroll_axis(raw_scroll: egui::Vec2, smooth_scroll: egui::Vec2) -> f32 {
        let pick = |delta: egui::Vec2| {
            if delta.y.abs() >= delta.x.abs() {
                delta.y
            } else {
                delta.x
            }
        };

        if smooth_scroll != egui::Vec2::ZERO {
            pick(smooth_scroll)
        } else {
            pick(raw_scroll)
        }
    }

    fn is_frame_scroll_input(modifiers: egui::Modifiers) -> bool {
        modifiers.shift
    }

    fn show_mammo_grid(&mut self, ui: &mut egui::Ui) {
        const MAMMO_GRID_GAP: f32 = 2.0;
        const MAMMO_VIEW_INNER_MARGIN: f32 = 3.0;
        let show_gsps_overlay = self.gsps_overlay_visible;

        ui.scope(|ui| {
            ui.spacing_mut().item_spacing = egui::vec2(MAMMO_GRID_GAP, MAMMO_GRID_GAP);

            let slot_count = if self.mammo_group.is_empty() {
                self.dicomweb_active_group_expected
                    .filter(|count| Self::is_supported_multi_view_group_size(*count))
                    .unwrap_or(4)
            } else {
                self.mammo_group.len()
            };
            let (rows, columns) = Self::multi_view_grid_dimensions(slot_count).unwrap_or((2, 2));
            let available = ui.available_size();
            let total_gap_x = MAMMO_GRID_GAP * columns.saturating_sub(1) as f32;
            let total_gap_y = MAMMO_GRID_GAP * rows.saturating_sub(1) as f32;
            let cell_width = ((available.x - total_gap_x).max(2.0)) / columns as f32;
            let cell_height = ((available.y - total_gap_y).max(2.0)) / rows as f32;
            let cell_size = egui::vec2(cell_width, cell_height);
            let common_frame_count = self.mammo_group_common_frame_count();
            let mut clicked_index = None;
            let mut pending_frame_target: Option<(usize, usize)> = None;

            for row in 0..rows {
                ui.horizontal(|ui| {
                    for col in 0..columns {
                        let index = row * columns + col;
                        ui.allocate_ui_with_layout(
                            cell_size,
                            egui::Layout::top_down(egui::Align::Center),
                            |ui| {
                                let has_loaded_image = self
                                    .mammo_group
                                    .get(index)
                                    .and_then(Option::as_ref)
                                    .is_some();
                                let stroke_color =
                                    if index == self.mammo_selected_index && has_loaded_image {
                                        PERSPECTA_BRAND_BLUE
                                    } else {
                                        egui::Color32::BLACK
                                    };
                                let frame = egui::Frame::none()
                                    .stroke(egui::Stroke::new(1.0, stroke_color))
                                    .inner_margin(egui::Margin::same(MAMMO_VIEW_INNER_MARGIN));
                                frame.show(ui, |ui| {
                                    let remaining = ui.available_size();
                                    let (viewport_rect, response) =
                                        ui.allocate_exact_size(remaining, Sense::click_and_drag());
                                    if response.clicked() {
                                        clicked_index = Some(index);
                                    }
                                    if let Some(viewport) =
                                        self.mammo_group.get_mut(index).and_then(Option::as_mut)
                                    {
                                        let texture_size = viewport.texture.size_vec2();
                                        if texture_size.x > 0.0
                                            && texture_size.y > 0.0
                                            && viewport_rect.is_positive()
                                        {
                                            let fit_scale = (viewport_rect.width()
                                                / texture_size.x)
                                                .min(viewport_rect.height() / texture_size.y)
                                                .max(0.01);
                                            let draw_size_before =
                                                texture_size * fit_scale * viewport.zoom;
                                            let base_center_before = Self::mammo_base_center(
                                                viewport_rect,
                                                draw_size_before.x,
                                                index,
                                            );
                                            if response.double_clicked() {
                                                viewport.zoom = 1.0;
                                                viewport.pan = egui::Vec2::ZERO;
                                            }
                                            if response.dragged() {
                                                let (frame_drag_delta, shift_held) =
                                                    ui.input(|input| {
                                                        (
                                                            input.pointer.delta(),
                                                            input.modifiers.shift,
                                                        )
                                                    });
                                                if shift_held && viewport.image.is_monochrome() {
                                                    if Self::apply_window_level_drag(
                                                        &mut viewport.window_center,
                                                        &mut viewport.window_width,
                                                        viewport.image.min_value,
                                                        viewport.image.max_value,
                                                        frame_drag_delta,
                                                    ) {
                                                        if let Some(color_image) =
                                                            Self::render_image_frame(
                                                                &viewport.image,
                                                                viewport.current_frame,
                                                                viewport.window_center,
                                                                viewport.window_width,
                                                            )
                                                        {
                                                            viewport.texture.set(
                                                                color_image,
                                                                TextureOptions::LINEAR,
                                                            );
                                                        }
                                                    }
                                                } else if viewport.zoom > 1.0 {
                                                    viewport.pan += frame_drag_delta;
                                                }
                                            }
                                            if response.hovered() {
                                                let (
                                                    modifiers,
                                                    raw_scroll,
                                                    smooth_scroll,
                                                    zoom_delta,
                                                ) = ui.input(|input| {
                                                    (
                                                        input.modifiers,
                                                        input.raw_scroll_delta,
                                                        input.smooth_scroll_delta,
                                                        input.zoom_delta(),
                                                    )
                                                });
                                                let frame_scroll_mode =
                                                    Self::is_frame_scroll_input(modifiers);
                                                let scroll = Self::dominant_scroll_axis(
                                                    raw_scroll,
                                                    smooth_scroll,
                                                );

                                                if frame_scroll_mode {
                                                    let frame_count = common_frame_count;
                                                    if frame_count > 1 {
                                                        let step = Self::frame_step_from_scroll(
                                                            &mut viewport.frame_scroll_accum,
                                                            scroll,
                                                        );
                                                        if step != 0 {
                                                            let next_frame = (viewport.current_frame
                                                                as i32
                                                                + step)
                                                                .clamp(0, frame_count as i32 - 1)
                                                                as usize;
                                                            pending_frame_target =
                                                                Some((index, next_frame));
                                                        }
                                                    }
                                                } else {
                                                    let wheel_zoom = (scroll * 0.0015).exp();
                                                    let mut next_zoom = viewport.zoom;
                                                    if (zoom_delta - 1.0).abs() > f32::EPSILON {
                                                        next_zoom *= zoom_delta;
                                                    } else if (wheel_zoom - 1.0).abs()
                                                        > f32::EPSILON
                                                    {
                                                        next_zoom *= wheel_zoom;
                                                    }
                                                    next_zoom = next_zoom.clamp(1.0, 12.0);
                                                    if (next_zoom - viewport.zoom).abs()
                                                        > f32::EPSILON
                                                    {
                                                        let old_zoom = viewport.zoom;
                                                        viewport.zoom = next_zoom;
                                                        if let Some(pointer_pos) =
                                                            response.hover_pos()
                                                        {
                                                            let old_center =
                                                                base_center_before + viewport.pan;
                                                            let pointer_offset =
                                                                pointer_pos - old_center;
                                                            let zoom_ratio =
                                                                viewport.zoom / old_zoom;
                                                            viewport.pan +=
                                                                pointer_offset * (1.0 - zoom_ratio);
                                                        }
                                                    }
                                                }
                                            }

                                            let draw_size =
                                                texture_size * fit_scale * viewport.zoom;
                                            let max_pan_x = ((draw_size.x - viewport_rect.width())
                                                * 0.5)
                                                .max(0.0);
                                            let max_pan_y =
                                                ((draw_size.y - viewport_rect.height()) * 0.5)
                                                    .max(0.0);
                                            viewport.pan.x =
                                                viewport.pan.x.clamp(-max_pan_x, max_pan_x);
                                            viewport.pan.y =
                                                viewport.pan.y.clamp(-max_pan_y, max_pan_y);
                                            if viewport.zoom <= 1.0 {
                                                viewport.pan = egui::Vec2::ZERO;
                                            }

                                            let base_center = Self::mammo_base_center(
                                                viewport_rect,
                                                draw_size.x,
                                                index,
                                            );
                                            let image_rect = egui::Rect::from_center_size(
                                                base_center + viewport.pan,
                                                draw_size,
                                            );
                                            let painter =
                                                ui.painter().with_clip_rect(viewport_rect);
                                            painter.image(
                                                viewport.texture.id(),
                                                image_rect,
                                                egui::Rect::from_min_max(
                                                    egui::Pos2::ZERO,
                                                    egui::pos2(1.0, 1.0),
                                                ),
                                                egui::Color32::WHITE,
                                            );
                                            if show_gsps_overlay {
                                                Self::draw_gsps_overlay(
                                                    &painter,
                                                    image_rect,
                                                    &viewport.image,
                                                    viewport.current_frame,
                                                );
                                            }
                                        }
                                    } else {
                                        ui.allocate_ui_with_layout(
                                            remaining,
                                            egui::Layout::centered_and_justified(
                                                egui::Direction::TopDown,
                                            ),
                                            |_ui| {},
                                        );
                                    }
                                });
                            },
                        );
                    }
                });
            }

            if let Some(index) = clicked_index {
                self.mammo_selected_index = index;
            }
            if let Some((index, frame_target)) = pending_frame_target {
                self.mammo_selected_index = index;
                if self.set_mammo_group_frame(frame_target) {
                    ui.ctx().request_repaint_after(Duration::from_millis(16));
                }
                self.last_cine_advance = Some(Instant::now());
            }
        });
    }

    fn show_history_list(
        &self,
        ui: &mut egui::Ui,
        current_history_id: Option<&str>,
    ) -> Option<usize> {
        if self.history_entries.is_empty() {
            ui.label("No previous images.");
            return None;
        }

        let mut clicked_index = None;
        egui::ScrollArea::vertical()
            .id_salt("history-thumbnails")
            .show(ui, |ui| {
                ui.with_layout(egui::Layout::top_down(egui::Align::Max), |ui| {
                    for (index, entry) in self.history_entries.iter().enumerate() {
                        let is_current = current_history_id == Some(entry.id.as_str());
                        let stroke_color = if is_current {
                            PERSPECTA_BRAND_BLUE
                        } else {
                            egui::Color32::from_gray(35)
                        };

                        egui::Frame::none()
                            .fill(egui::Color32::TRANSPARENT)
                            .stroke(egui::Stroke::new(1.0, stroke_color))
                            .inner_margin(egui::Margin::same(6.0))
                            .show(ui, |ui| {
                                ui.horizontal_wrapped(|ui| {
                                    for thumb in &entry.thumbs {
                                        let texture_size = thumb.texture.size_vec2();
                                        let max_side = texture_size.x.max(texture_size.y).max(1.0);
                                        let scale = (HISTORY_LIST_THUMB_MAX_DIM / max_side)
                                            .clamp(0.01, 1.0);
                                        let draw_size = texture_size * scale;
                                        let response = ui.add(
                                            egui::Image::new((thumb.texture.id(), draw_size))
                                                .sense(Sense::click()),
                                        );
                                        if response.clicked() {
                                            clicked_index = Some(index);
                                        }
                                    }
                                });
                            });
                        ui.add_space(4.0);
                    }
                });
            });

        clicked_index
    }

    fn show_resize_grip(&self, ctx: &egui::Context) {
        const GRIP_SIZE: f32 = 18.0;
        const MARGIN: f32 = 1.0;

        egui::Area::new(egui::Id::new("window-resize-grip"))
            .order(egui::Order::Foreground)
            .anchor(egui::Align2::RIGHT_BOTTOM, egui::vec2(-MARGIN, -MARGIN))
            .show(ctx, |ui| {
                let (rect, response) = ui
                    .allocate_exact_size(egui::vec2(GRIP_SIZE, GRIP_SIZE), Sense::click_and_drag());

                if response.drag_started() {
                    ui.ctx().send_viewport_cmd(ViewportCommand::BeginResize(
                        ResizeDirection::SouthEast,
                    ));
                }
                if response.hovered() && !response.dragged() {
                    ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeSouthEast);
                }

                let stroke = egui::Stroke::new(1.0, egui::Color32::from_gray(72));
                let r = rect.shrink(3.0);
                for offset in [0.0_f32, 4.0, 8.0] {
                    ui.painter().line_segment(
                        [
                            egui::pos2(r.right() - 4.0 - offset, r.bottom()),
                            egui::pos2(r.right(), r.bottom() - 4.0 - offset),
                        ],
                        stroke,
                    );
                }
            });
    }
}

impl eframe::App for DicomViewerApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        Self::apply_black_background(ctx);
        if self.is_loading() || self.frame_wait_pending {
            ctx.set_cursor_icon(egui::CursorIcon::Progress);
        } else {
            ctx.set_cursor_icon(egui::CursorIcon::Default);
        }
        let dropped_files = ctx.input(|input| input.raw.dropped_files.clone());
        self.apply_dropped_files(&dropped_files, ctx);
        self.process_pending_history_open(ctx);
        self.process_pending_local_open(ctx);

        if let Some(request) = self.pending_launch_request.take() {
            self.handle_launch_request(request, ctx);
        }

        self.poll_dicomweb_active_paths(ctx);
        self.poll_dicomweb_download(ctx);
        self.poll_history_preload(ctx);
        self.poll_single_load(ctx);
        self.poll_mammo_group_load(ctx);
        if self.frame_wait_pending && !self.cine_mode {
            if self.image.is_some() {
                self.rebuild_texture(ctx);
            } else if self.loaded_mammo_count() > 0 {
                let pending = self.set_mammo_group_frame(self.selected_mammo_frame_index());
                self.frame_wait_pending = pending;
                if pending {
                    ctx.request_repaint_after(Duration::from_millis(16));
                }
            } else {
                self.frame_wait_pending = false;
            }
        }
        self.advance_cine_if_needed(ctx);

        let mut history_cycle_direction = None;
        let mut close_app_requested = false;
        let mut close_group_requested = false;
        let mut c_pressed = false;
        let mut g_pressed = false;
        let mut n_pressed = false;
        ctx.input_mut(|input| {
            if input.consume_key(
                egui::Modifiers::COMMAND | egui::Modifiers::SHIFT,
                egui::Key::W,
            ) {
                close_app_requested = true;
            } else if input.consume_key(egui::Modifiers::COMMAND, egui::Key::W) {
                close_group_requested = true;
            } else if input.consume_key(egui::Modifiers::SHIFT, egui::Key::Tab) {
                history_cycle_direction = Some(-1);
            } else if input.consume_key(egui::Modifiers::NONE, egui::Key::Tab) {
                history_cycle_direction = Some(1);
            }
            c_pressed = input.consume_key(egui::Modifiers::NONE, egui::Key::C);
            g_pressed = input.consume_key(egui::Modifiers::NONE, egui::Key::G);
            n_pressed = input.consume_key(egui::Modifiers::NONE, egui::Key::N);
        });
        if close_app_requested {
            ctx.send_viewport_cmd(ViewportCommand::Close);
            return;
        }
        if let Some(direction) = history_cycle_direction {
            self.cycle_history_entry(direction);
        }
        let history_transition_pending = self.pending_history_open_index.is_some();
        if close_group_requested
            && !history_transition_pending
            && self.handle_close_group_shortcut(ctx)
        {
            ctx.send_viewport_cmd(ViewportCommand::Close);
            return;
        }
        if c_pressed && !history_transition_pending {
            self.toggle_cine_mode();
        }
        if g_pressed && !history_transition_pending {
            self.toggle_gsps_overlay();
        }
        if n_pressed && !history_transition_pending {
            self.jump_to_next_gsps_overlay(ctx);
        }

        let mut open_dicoms_clicked = false;
        let hovered_files = ctx.input(|input| input.raw.hovered_files.clone());

        let is_maximized = ctx.input(|input| input.viewport().maximized.unwrap_or(false));
        let title_text = format!("{APP_TITLE} v{APP_VERSION}");
        let bar_fill = ctx.style().visuals.panel_fill;
        egui::TopBottomPanel::top("titlebar")
            .show_separator_line(false)
            .frame(egui::Frame::none().fill(bar_fill))
            .exact_height(30.0)
            .show(ctx, |ui| {
                let button_size = egui::vec2(28.0, 22.0);
                let right_controls_min_width =
                    button_size.x * 3.0 + ui.spacing().item_spacing.x * 2.0;
                let left_controls_width = right_controls_min_width;
                let total_width = ui.available_width();
                let titlebar_rect = ui.max_rect();
                let center_width = (total_width
                    - left_controls_width
                    - right_controls_min_width
                    - ui.spacing().item_spacing.x * 2.0)
                    .max(0.0);

                ui.add_space(2.0);
                ui.horizontal(|ui| {
                    ui.allocate_ui_with_layout(
                        egui::vec2(left_controls_width, button_size.y),
                        egui::Layout::left_to_right(egui::Align::Center),
                        |ui| {
                            ui.add_space(4.0);
                            let menu_button = egui::Button::new("")
                                .fill(bar_fill)
                                .stroke(egui::Stroke::NONE)
                                .min_size(egui::vec2(20.0, 18.0));
                            let menu_response =
                                egui::menu::menu_custom_button(ui, menu_button, |ui| {
                                    if ui.button("Open DICOM(s)").clicked() {
                                        open_dicoms_clicked = true;
                                        ui.close_menu();
                                    }
                                    ui.menu_button("Select Metadata Fields", |ui| {
                                        self.show_metadata_field_options_menu(ui);
                                    });
                                });

                            let icon_rect =
                                menu_response.response.rect.shrink2(egui::vec2(5.0, 5.0));
                            let line_color = ui.visuals().widgets.inactive.fg_stroke.color;
                            let line_stroke = egui::Stroke::new(1.0, line_color);
                            let y_top = icon_rect.top() + 1.0;
                            let y_mid = icon_rect.center().y;
                            let y_bottom = icon_rect.bottom() - 1.0;
                            for y in [y_top, y_mid, y_bottom] {
                                ui.painter().line_segment(
                                    [
                                        egui::pos2(icon_rect.left(), y),
                                        egui::pos2(icon_rect.right(), y),
                                    ],
                                    line_stroke,
                                );
                            }
                        },
                    );

                    let (title_rect, drag_response) = ui.allocate_exact_size(
                        egui::vec2(center_width, button_size.y),
                        Sense::click_and_drag(),
                    );
                    let window_centered_title_pos =
                        egui::pos2(titlebar_rect.center().x, title_rect.center().y);
                    ui.painter().text(
                        window_centered_title_pos,
                        egui::Align2::CENTER_CENTER,
                        &title_text,
                        egui::FontId::proportional(14.0),
                        ui.visuals().text_color(),
                    );

                    if drag_response.is_pointer_button_down_on() {
                        ctx.send_viewport_cmd(ViewportCommand::StartDrag);
                    }
                    if drag_response.double_clicked() {
                        ctx.send_viewport_cmd(ViewportCommand::Maximized(!is_maximized));
                    }

                    ui.allocate_ui_with_layout(
                        egui::vec2(ui.available_width(), button_size.y),
                        egui::Layout::right_to_left(egui::Align::Center),
                        |ui| {
                            if ui
                                .add_sized(
                                    button_size,
                                    egui::Button::new("X")
                                        .fill(bar_fill)
                                        .stroke(egui::Stroke::NONE),
                                )
                                .clicked()
                            {
                                ctx.send_viewport_cmd(ViewportCommand::Close);
                            }

                            if ui
                                .add_sized(
                                    button_size,
                                    egui::Button::new("□")
                                        .fill(bar_fill)
                                        .stroke(egui::Stroke::NONE),
                                )
                                .clicked()
                            {
                                ctx.send_viewport_cmd(ViewportCommand::Maximized(!is_maximized));
                            }

                            if ui
                                .add_sized(
                                    button_size,
                                    egui::Button::new("_")
                                        .fill(bar_fill)
                                        .stroke(egui::Stroke::NONE),
                                )
                                .clicked()
                            {
                                ctx.send_viewport_cmd(ViewportCommand::Minimized(true));
                            }
                        },
                    );
                });
            });

        if open_dicoms_clicked {
            self.open_dicoms(ctx);
        }

        let has_mammo_group = self.has_mammo_group();

        let has_history = !self.history_entries.is_empty();
        let current_history_id = self.current_history_id();
        let mut open_history_index = None;

        let mut active_state = self.active_viewport_state();
        let mut toggle_cine_clicked = false;
        let mut toggle_gsps_clicked = false;
        let mut next_gsps_clicked = false;
        let mut request_rebuild = false;
        let has_active_gsps_overlay = self.has_available_gsps_overlay();
        let has_gsps_navigation_target = self.next_gsps_navigation_target().is_some();

        if let Some(state) = active_state.as_mut() {
            let overlay_width = (ctx.screen_rect().width() * 0.5).clamp(340.0, 760.0);
            egui::Area::new(egui::Id::new("wl-overlay-right-bottom"))
                .order(egui::Order::Foreground)
                .anchor(egui::Align2::RIGHT_BOTTOM, egui::vec2(-10.0, -10.0))
                .show(ctx, |ui| {
                    ui.set_min_width(overlay_width);
                    ui.set_max_width(overlay_width);
                    let available_width = ui.available_width();
                    let controls_width = (available_width * 0.92).clamp(260.0, available_width);
                    let slider_width = (controls_width * 0.84).clamp(220.0, controls_width);

                    ui.add_enabled_ui(!history_transition_pending, |ui| {
                        ui.with_layout(egui::Layout::top_down(egui::Align::Max), |ui| {
                            let item_spacing = ui.spacing().item_spacing;
                            ui.spacing_mut().item_spacing =
                                egui::vec2(item_spacing.x, item_spacing.y + 4.0);

                            if state.is_monochrome {
                                let center_range = (state.min_value as f32 - 2000.0)
                                    ..=(state.max_value as f32 + 2000.0);
                                let max_width = ((state.max_value - state.min_value).abs() as f32
                                    * 2.0)
                                    .max(1.0);
                                let width_range = 1.0..=max_width;
                                let refresh_button_size = ui.spacing().interact_size.y;
                                let row_height = ui.spacing().interact_size.y;
                                let slider_with_refresh_width = (slider_width
                                    - refresh_button_size
                                    - CONTROL_VALUE_WIDTH
                                    - 2.0 * ui.spacing().item_spacing.x)
                                    .max(120.0);

                                ui.allocate_ui_with_layout(
                                    egui::vec2(controls_width, row_height),
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        if ui
                                            .add_sized(
                                                [refresh_button_size, row_height],
                                                egui::Button::new(
                                                    egui::RichText::new("↺").size(14.0),
                                                )
                                                .fill(egui::Color32::BLACK)
                                                .stroke(egui::Stroke::NONE),
                                            )
                                            .on_hover_text("Reset Center")
                                            .clicked()
                                        {
                                            state.window_center = state.default_center;
                                            request_rebuild = true;
                                        }

                                        request_rebuild |= Self::add_value_control_no_border(
                                            ui,
                                            [CONTROL_VALUE_WIDTH, row_height],
                                            egui::DragValue::new(&mut state.window_center)
                                                .range(center_range.clone())
                                                .speed(1.0)
                                                .max_decimals(1),
                                        )
                                        .changed();

                                        request_rebuild |= ui
                                            .add_sized(
                                                [slider_with_refresh_width, row_height],
                                                egui::Slider::new(
                                                    &mut state.window_center,
                                                    center_range.clone(),
                                                )
                                                .show_value(false)
                                                .text("Center"),
                                            )
                                            .changed();
                                    },
                                );
                                ui.allocate_ui_with_layout(
                                    egui::vec2(controls_width, row_height),
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        if ui
                                            .add_sized(
                                                [refresh_button_size, row_height],
                                                egui::Button::new(
                                                    egui::RichText::new("↺").size(14.0),
                                                )
                                                .fill(egui::Color32::BLACK)
                                                .stroke(egui::Stroke::NONE),
                                            )
                                            .on_hover_text("Reset Width")
                                            .clicked()
                                        {
                                            state.window_width = state.default_width;
                                            request_rebuild = true;
                                        }

                                        request_rebuild |= Self::add_value_control_no_border(
                                            ui,
                                            [CONTROL_VALUE_WIDTH, row_height],
                                            egui::DragValue::new(&mut state.window_width)
                                                .range(width_range.clone())
                                                .speed(1.0)
                                                .max_decimals(1),
                                        )
                                        .changed();

                                        request_rebuild |= ui
                                            .add_sized(
                                                [slider_with_refresh_width, row_height],
                                                egui::Slider::new(
                                                    &mut state.window_width,
                                                    width_range.clone(),
                                                )
                                                .show_value(false)
                                                .text("Width"),
                                            )
                                            .changed();
                                    },
                                );
                            }

                            if state.frame_count > 1 {
                                let mut frame_index = state.current_frame as u32;
                                let max_frame = state.frame_count.saturating_sub(1) as u32;
                                let refresh_button_size = ui.spacing().interact_size.y;
                                let row_height = ui.spacing().interact_size.y;
                                let slider_with_refresh_width = (slider_width
                                    - refresh_button_size
                                    - CONTROL_VALUE_WIDTH
                                    - 2.0 * ui.spacing().item_spacing.x)
                                    .max(120.0);

                                ui.allocate_ui_with_layout(
                                    egui::vec2(controls_width, row_height),
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        if ui
                                            .add_sized(
                                                [refresh_button_size, row_height],
                                                egui::Button::new(
                                                    egui::RichText::new("↺").size(14.0),
                                                )
                                                .fill(egui::Color32::BLACK)
                                                .stroke(egui::Stroke::NONE),
                                            )
                                            .on_hover_text("Reset Frame")
                                            .clicked()
                                        {
                                            frame_index = 0;
                                            state.current_frame = 0;
                                            self.last_cine_advance = Some(Instant::now());
                                            request_rebuild = true;
                                        }

                                        if Self::add_value_control_no_border(
                                            ui,
                                            [CONTROL_VALUE_WIDTH, row_height],
                                            egui::DragValue::new(&mut frame_index)
                                                .range(0..=max_frame)
                                                .speed(1.0),
                                        )
                                        .changed()
                                        {
                                            state.current_frame = frame_index as usize;
                                            self.last_cine_advance = Some(Instant::now());
                                            request_rebuild = true;
                                        }

                                        if ui
                                            .add_sized(
                                                [slider_with_refresh_width, row_height],
                                                egui::Slider::new(&mut frame_index, 0..=max_frame)
                                                    .show_value(false)
                                                    .text("Frame"),
                                            )
                                            .changed()
                                        {
                                            state.current_frame = frame_index as usize;
                                            self.last_cine_advance = Some(Instant::now());
                                            request_rebuild = true;
                                        }
                                    },
                                );

                                let refresh_button_size = ui.spacing().interact_size.y;
                                let row_height = ui.spacing().interact_size.y;

                                ui.allocate_ui_with_layout(
                                    egui::vec2(controls_width, row_height),
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        if ui
                                            .add(
                                                egui::Button::new(
                                                    egui::RichText::new("↺").size(14.0),
                                                )
                                                .fill(egui::Color32::BLACK)
                                                .stroke(egui::Stroke::NONE)
                                                .min_size(egui::vec2(
                                                    refresh_button_size,
                                                    row_height,
                                                )),
                                            )
                                            .on_hover_text("Reset Cine FPS")
                                            .clicked()
                                        {
                                            self.cine_fps =
                                                self.default_cine_fps_for_active_image();
                                            self.last_cine_advance = Some(Instant::now());
                                        }

                                        if Self::add_value_control_no_border(
                                            ui,
                                            [CONTROL_VALUE_WIDTH, row_height],
                                            egui::DragValue::new(&mut self.cine_fps)
                                                .range(1.0..=120.0)
                                                .speed(0.5)
                                                .max_decimals(1),
                                        )
                                        .changed()
                                        {
                                            self.cine_fps = self.cine_fps.clamp(1.0, 120.0);
                                            self.last_cine_advance = Some(Instant::now());
                                        }

                                        if ui
                                            .add_sized(
                                                [slider_with_refresh_width, row_height],
                                                egui::Slider::new(&mut self.cine_fps, 1.0..=120.0)
                                                    .show_value(false)
                                                    .text("Cine FPS"),
                                            )
                                            .changed()
                                        {
                                            self.cine_fps = self.cine_fps.clamp(1.0, 120.0);
                                            self.last_cine_advance = Some(Instant::now());
                                        }
                                    },
                                );

                                ui.allocate_ui_with_layout(
                                    egui::vec2(controls_width, 0.0),
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        if Self::add_action_control_button_no_border(
                                            ui,
                                            [
                                                CONTROL_ACTION_BUTTON_WIDTH,
                                                ui.spacing().interact_size.y,
                                            ],
                                            if self.cine_mode {
                                                "Stop Cine (C)"
                                            } else {
                                                "Start Cine (C)"
                                            },
                                        )
                                        .clicked()
                                        {
                                            toggle_cine_clicked = true;
                                        }
                                    },
                                );
                            }

                            if has_active_gsps_overlay {
                                ui.allocate_ui_with_layout(
                                    egui::vec2(controls_width, 0.0),
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        if Self::add_action_control_button_no_border(
                                            ui,
                                            [
                                                CONTROL_ACTION_BUTTON_WIDTH,
                                                ui.spacing().interact_size.y,
                                            ],
                                            if self.gsps_overlay_visible {
                                                "Hide GSPS (G)"
                                            } else {
                                                "Show GSPS (G)"
                                            },
                                        )
                                        .clicked()
                                        {
                                            toggle_gsps_clicked = true;
                                        }
                                    },
                                );

                                if has_gsps_navigation_target {
                                    ui.allocate_ui_with_layout(
                                        egui::vec2(controls_width, 0.0),
                                        egui::Layout::right_to_left(egui::Align::Center),
                                        |ui| {
                                            if Self::add_action_control_button_no_border(
                                                ui,
                                                [
                                                    CONTROL_ACTION_BUTTON_WIDTH,
                                                    ui.spacing().interact_size.y,
                                                ],
                                                "Next GSPS (N)",
                                            )
                                            .on_hover_text(
                                                "Jump to the next GSPS overlay and corresponding frame.",
                                            )
                                            .clicked()
                                            {
                                                next_gsps_clicked = true;
                                            }
                                        },
                                    );
                                }
                            }
                        });
                    });
                });
        }

        if toggle_cine_clicked {
            self.toggle_cine_mode();
        }
        if toggle_gsps_clicked {
            self.toggle_gsps_overlay();
        }
        if next_gsps_clicked {
            self.jump_to_next_gsps_overlay(ctx);
        }

        // Avoid applying stale W/L UI state while cycling history quickly with Tab.
        if request_rebuild && !next_gsps_clicked {
            if let Some(state) = active_state.as_ref() {
                self.apply_active_viewport_state(state, ctx);
            }
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            if has_mammo_group {
                self.show_mammo_grid(ui);
            } else if let Some(texture) = self.texture.clone() {
                let available = ui.available_size();
                let (canvas_rect, response) =
                    ui.allocate_exact_size(available, Sense::click_and_drag());
                let image_size = texture.size_vec2();
                if image_size.x > 0.0 && image_size.y > 0.0 && canvas_rect.is_positive() {
                    if response.double_clicked() {
                        self.reset_single_view_transform();
                    }

                    if response.dragged() {
                        let (frame_drag_delta, shift_held) =
                            ui.input(|input| (input.pointer.delta(), input.modifiers.shift));
                        let wl_meta = self
                            .image
                            .as_ref()
                            .map(|image| (image.is_monochrome(), image.min_value, image.max_value));
                        let mut handled_wl_drag = false;
                        if shift_held {
                            if let Some((true, min_value, max_value)) = wl_meta {
                                handled_wl_drag = true;
                                if Self::apply_window_level_drag(
                                    &mut self.window_center,
                                    &mut self.window_width,
                                    min_value,
                                    max_value,
                                    frame_drag_delta,
                                ) {
                                    self.rebuild_texture(ctx);
                                }
                            }
                        }
                        if !handled_wl_drag && self.single_view_zoom > 1.0 {
                            self.single_view_pan += frame_drag_delta;
                        }
                    }

                    if response.hovered() {
                        let (modifiers, zoom_delta, raw_scroll, smooth_scroll) =
                            ui.input(|input| {
                                (
                                    input.modifiers,
                                    input.zoom_delta(),
                                    input.raw_scroll_delta,
                                    input.smooth_scroll_delta,
                                )
                            });
                        let frame_scroll_mode = Self::is_frame_scroll_input(modifiers);
                        let scroll = Self::dominant_scroll_axis(raw_scroll, smooth_scroll);

                        if frame_scroll_mode {
                            if let Some(image) = self.image.as_ref() {
                                let frame_count = image.frame_count();
                                if frame_count > 1 {
                                    let step = Self::frame_step_from_scroll(
                                        &mut self.single_view_frame_scroll_accum,
                                        scroll,
                                    );
                                    if step != 0 {
                                        self.current_frame = (self.current_frame as i32 + step)
                                            .clamp(0, frame_count as i32 - 1)
                                            as usize;
                                        self.last_cine_advance = Some(Instant::now());
                                        self.rebuild_texture(ctx);
                                    }
                                }
                            }
                        } else {
                            let wheel_zoom = (scroll * 0.0015).exp();
                            let mut next_zoom = self.single_view_zoom;
                            if (zoom_delta - 1.0).abs() > f32::EPSILON {
                                next_zoom *= zoom_delta;
                            } else if (wheel_zoom - 1.0).abs() > f32::EPSILON {
                                next_zoom *= wheel_zoom;
                            }
                            next_zoom = next_zoom.clamp(1.0, 12.0);

                            if (next_zoom - self.single_view_zoom).abs() > f32::EPSILON {
                                let old_zoom = self.single_view_zoom;
                                self.single_view_zoom = next_zoom;
                                if let Some(pointer_pos) = response.hover_pos() {
                                    let old_center = canvas_rect.center() + self.single_view_pan;
                                    let pointer_offset = pointer_pos - old_center;
                                    let zoom_ratio = self.single_view_zoom / old_zoom;
                                    self.single_view_pan += pointer_offset * (1.0 - zoom_ratio);
                                }
                            }
                        }
                    }

                    let fit_scale = (canvas_rect.width() / image_size.x)
                        .min(canvas_rect.height() / image_size.y)
                        .max(0.01);
                    let draw_size = image_size * fit_scale * self.single_view_zoom;
                    let max_pan_x = ((draw_size.x - canvas_rect.width()) * 0.5).max(0.0);
                    let max_pan_y = ((draw_size.y - canvas_rect.height()) * 0.5).max(0.0);
                    self.single_view_pan.x = self.single_view_pan.x.clamp(-max_pan_x, max_pan_x);
                    self.single_view_pan.y = self.single_view_pan.y.clamp(-max_pan_y, max_pan_y);
                    if self.single_view_zoom <= 1.0 {
                        self.single_view_pan = egui::Vec2::ZERO;
                    }

                    let image_rect = egui::Rect::from_center_size(
                        canvas_rect.center() + self.single_view_pan,
                        draw_size,
                    );
                    let painter = ui.painter().with_clip_rect(canvas_rect);
                    painter.image(
                        texture.id(),
                        image_rect,
                        egui::Rect::from_min_max(egui::Pos2::ZERO, egui::pos2(1.0, 1.0)),
                        egui::Color32::WHITE,
                    );
                    if self.gsps_overlay_visible {
                        if let Some(image) = self.image.as_ref() {
                            Self::draw_gsps_overlay(
                                &painter,
                                image_rect,
                                image,
                                self.current_frame,
                            );
                        }
                    }
                }
            } else if let Some(report) = self.report.as_ref() {
                self.show_structured_report_view(ui, report);
            } else {
                let is_loading = self.is_loading();
                ui.allocate_ui_with_layout(
                    ui.available_size(),
                    egui::Layout::centered_and_justified(egui::Direction::TopDown),
                    |ui| {
                        if is_loading {
                            ui.label("Loading DICOM(s)...");
                        } else {
                            ui.label("Open DICOM(s) to start.");
                        }
                    },
                );
            }
        });

        if let Some(metadata) = self.active_metadata() {
            let overlay_height = (ctx.screen_rect().height() * 0.62).max(180.0);
            egui::Area::new(egui::Id::new("metadata-overlay-left"))
                .order(egui::Order::Foreground)
                .anchor(egui::Align2::LEFT_TOP, egui::vec2(10.0, 36.0))
                .show(ctx, |ui| {
                    ui.set_min_width(300.0);
                    ui.set_max_width(300.0);
                    ui.set_max_height(overlay_height);
                    egui::ScrollArea::vertical()
                        .id_salt("metadata-overlay-scroll")
                        .show(ui, |ui| {
                            let mut shown_count = 0usize;
                            for (key, value) in metadata {
                                if !self.visible_metadata_fields.contains(key.as_str()) {
                                    continue;
                                }
                                shown_count = shown_count.saturating_add(1);
                                ui.horizontal_wrapped(|ui| {
                                    ui.monospace(key);
                                    ui.label(value);
                                });
                            }
                            if shown_count == 0 {
                                ui.label("No metadata fields selected.");
                            }
                        });
                });
        }

        if has_history {
            let overlay_height = (ctx.screen_rect().height() * 0.62).max(160.0);
            egui::Area::new(egui::Id::new("history-overlay-right"))
                .order(egui::Order::Foreground)
                .anchor(egui::Align2::RIGHT_TOP, egui::vec2(-10.0, 36.0))
                .show(ctx, |ui| {
                    ui.set_min_width(170.0);
                    ui.set_max_width(170.0);
                    ui.set_max_height(overlay_height);
                    if let Some(index) = self.show_history_list(ui, current_history_id.as_deref()) {
                        open_history_index = Some(index);
                    }
                });
        }

        if let Some(message) = self.load_error_message.clone() {
            let mut dismiss_error = false;
            egui::Area::new(egui::Id::new("load-error-overlay"))
                .order(egui::Order::Foreground)
                .anchor(egui::Align2::CENTER_TOP, egui::vec2(0.0, 36.0))
                .show(ctx, |ui| {
                    egui::Frame::none()
                        .fill(egui::Color32::from_black_alpha(220))
                        .stroke(egui::Stroke::new(1.0, egui::Color32::from_gray(72)))
                        .rounding(egui::Rounding::same(6.0))
                        .inner_margin(egui::Margin::symmetric(10.0, 8.0))
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(
                                    egui::RichText::new(message)
                                        .color(egui::Color32::from_rgb(224, 96, 96)),
                                );
                                if ui
                                    .add(
                                        egui::Button::new(
                                            egui::RichText::new("×")
                                                .color(egui::Color32::from_gray(190)),
                                        )
                                        .small()
                                        .fill(egui::Color32::TRANSPARENT)
                                        .stroke(egui::Stroke::NONE),
                                    )
                                    .clicked()
                                {
                                    dismiss_error = true;
                                }
                            });
                        });
                });
            if dismiss_error {
                self.clear_load_error();
                ctx.request_repaint();
            }
        }

        if let Some(index) = open_history_index {
            self.queue_history_open(index);
        }

        self.show_file_drop_overlay(ctx, &hovered_files);
        self.show_resize_grip(ctx);

        if self.is_loading() {
            ctx.set_cursor_icon(egui::CursorIcon::Progress);
        }
    }
}

fn downsample_color_image(source: &ColorImage, max_dim: usize) -> ColorImage {
    let source_width = source.size[0];
    let source_height = source.size[1];
    if source_width == 0 || source_height == 0 || max_dim == 0 {
        return source.clone();
    }

    let longest_edge = source_width.max(source_height);
    if longest_edge <= max_dim {
        return source.clone();
    }

    let scale = max_dim as f32 / longest_edge as f32;
    let target_width = ((source_width as f32 * scale).round() as usize).max(1);
    let target_height = ((source_height as f32 * scale).round() as usize).max(1);

    let mut pixels = Vec::with_capacity(target_width * target_height);
    for target_y in 0..target_height {
        let source_y = ((target_y * source_height) / target_height).min(source_height - 1);
        for target_x in 0..target_width {
            let source_x = ((target_x * source_width) / target_width).min(source_width - 1);
            let source_index = source_y * source_width + source_x;
            pixels.push(source.pixels[source_index]);
        }
    }

    ColorImage {
        size: [target_width, target_height],
        pixels,
    }
}

fn compose_grid_thumb(images: &[ColorImage], max_dim: usize) -> ColorImage {
    if images.is_empty() || max_dim == 0 {
        return ColorImage {
            size: [1, 1],
            pixels: vec![egui::Color32::BLACK],
        };
    }
    if images.len() == 1 {
        return downsample_color_image(&images[0], max_dim);
    }

    let (rows, columns) =
        DicomViewerApp::multi_view_grid_dimensions(images.len()).unwrap_or((1, images.len()));
    let cell_width = (max_dim / columns).max(1);
    let cell_height = (max_dim / rows).max(1);
    let target_width = cell_width * columns;
    let target_height = cell_height * rows;
    let mut pixels = vec![egui::Color32::BLACK; target_width * target_height];

    let align_mammo = matches!(images.len(), 4 | 8);

    for (index, image) in images.iter().enumerate() {
        let source_width = image.size[0].max(1);
        let source_height = image.size[1].max(1);

        let scale = (cell_width as f32 / source_width as f32)
            .min(cell_height as f32 / source_height as f32);
        let draw_width = ((source_width as f32 * scale).round() as usize).clamp(1, cell_width);
        let draw_height = ((source_height as f32 * scale).round() as usize).clamp(1, cell_height);

        let col = index % columns;
        let row = index / columns;
        let base_x = if align_mammo {
            match mammo_image_align(index) {
                egui::Align::Max => col * cell_width + (cell_width - draw_width),
                egui::Align::Min => col * cell_width,
                _ => col * cell_width + (cell_width - draw_width) / 2,
            }
        } else {
            col * cell_width + (cell_width - draw_width) / 2
        };
        let base_y = row * cell_height + (cell_height - draw_height) / 2;

        for y in 0..draw_height {
            let source_y = ((y * source_height) / draw_height).min(source_height - 1);
            for x in 0..draw_width {
                let source_x = ((x * source_width) / draw_width).min(source_width - 1);
                let source_index = source_y * source_width + source_x;
                let target_index = (base_y + y) * target_width + (base_x + x);
                pixels[target_index] = image.pixels[source_index];
            }
        }
    }

    ColorImage {
        size: [target_width, target_height],
        pixels,
    }
}

fn default_visible_metadata_fields() -> HashSet<String> {
    METADATA_FIELD_NAMES
        .iter()
        .map(|field| (*field).to_string())
        .collect()
}

fn ordered_visible_metadata_fields(visible: &HashSet<String>) -> Vec<String> {
    METADATA_FIELD_NAMES
        .iter()
        .filter(|field| visible.contains(**field))
        .map(|field| (*field).to_string())
        .collect()
}

fn metadata_settings_file_path() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        return env::var_os("APPDATA")
            .map(PathBuf::from)
            .map(|base| base.join("perspecta").join("settings.toml"));
    }

    #[cfg(target_os = "macos")]
    {
        return env::var_os("HOME").map(PathBuf::from).map(|home| {
            home.join("Library")
                .join("Application Support")
                .join("perspecta")
                .join("settings.toml")
        });
    }

    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
            return Some(PathBuf::from(xdg).join("perspecta").join("settings.toml"));
        }
        env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join(".config").join("perspecta").join("settings.toml"))
    }
}

fn load_visible_metadata_fields(path: &Path) -> Option<HashSet<String>> {
    let text = fs::read_to_string(path).ok()?;
    let parsed = parse_visible_metadata_fields_from_toml(&text)?;
    let filtered = parsed
        .iter()
        .filter(|field| METADATA_FIELD_NAMES.contains(&field.as_str()))
        .cloned()
        .collect::<HashSet<_>>();

    if parsed.is_empty() {
        return Some(filtered);
    }
    if filtered.is_empty() {
        return None;
    }
    Some(filtered)
}

fn render_settings_toml(fields: &[String]) -> String {
    let mut text = String::from("visible_metadata_fields = [\n");
    for field in fields {
        text.push_str("  \"");
        text.push_str(&escape_toml_string(field));
        text.push_str("\",\n");
    }
    text.push_str("]\n");
    text
}

fn parse_visible_metadata_fields_from_toml(text: &str) -> Option<Vec<String>> {
    let key_pos = text.find("visible_metadata_fields")?;
    let after_key = &text[key_pos..];
    let open_bracket = after_key.find('[')?;
    let array_start = key_pos + open_bracket + 1;
    let array_tail = &text[array_start..];
    let close_bracket_rel = array_tail.find(']')?;
    let array_body = &array_tail[..close_bracket_rel];

    let mut fields = Vec::new();
    for chunk in array_body.split(',') {
        let token = chunk.trim();
        if token.is_empty() {
            continue;
        }
        if token.starts_with('\"') && token.ends_with('\"') && token.len() >= 2 {
            let inner = &token[1..token.len() - 1];
            fields.push(unescape_toml_string(inner));
        }
    }
    Some(fields)
}

fn escape_toml_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\"', "\\\"")
}

fn unescape_toml_string(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        let Some(next) = chars.next() else {
            break;
        };
        match next {
            '\\' => output.push('\\'),
            '\"' => output.push('\"'),
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            't' => output.push('\t'),
            other => output.push(other),
        }
    }
    output
}

fn history_id_from_paths(paths: &[PathBuf]) -> String {
    let mut normalized = paths
        .iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    normalized.sort();
    format!("{}:{}", normalized.len(), normalized.join("|"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_texture(ctx: &egui::Context, name: &str) -> TextureHandle {
        ctx.load_texture(
            name,
            ColorImage {
                size: [1, 1],
                pixels: vec![egui::Color32::BLACK],
            },
            TextureOptions::LINEAR,
        )
    }

    fn single_history_entry(ctx: &egui::Context, path: &str, texture_name: &str) -> HistoryEntry {
        let path_buf = PathBuf::from(path);
        HistoryEntry {
            id: history_id_from_paths(std::slice::from_ref(&path_buf)),
            kind: HistoryKind::Single(Box::new(HistorySingleData {
                path: path_buf,
                image: DicomImage::test_stub(None),
                texture: test_texture(ctx, texture_name),
                window_center: 0.0,
                window_width: 1.0,
                current_frame: 0,
                cine_fps: DEFAULT_CINE_FPS,
            })),
            thumbs: Vec::new(),
        }
    }

    fn report_history_entry(ctx: &egui::Context, path: &str, texture_name: &str) -> HistoryEntry {
        let path_buf = PathBuf::from(path);
        HistoryEntry {
            id: history_id_from_paths(std::slice::from_ref(&path_buf)),
            kind: HistoryKind::Report(Box::new(HistoryReportData {
                path: path_buf,
                report: StructuredReportDocument::test_stub(),
            })),
            thumbs: vec![HistoryThumb {
                texture: test_texture(ctx, texture_name),
            }],
        }
    }

    #[test]
    fn metadata_settings_toml_roundtrip() {
        let selected = vec![
            "PatientName".to_string(),
            "StudyDescription".to_string(),
            "Modality".to_string(),
        ];
        let toml = render_settings_toml(&selected);
        let parsed = parse_visible_metadata_fields_from_toml(&toml).expect("TOML should parse");
        assert_eq!(parsed, selected);
    }

    #[test]
    fn load_visible_metadata_fields_filters_unknown_values() {
        let path = std::env::temp_dir().join(format!(
            "perspecta-settings-test-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let toml = "visible_metadata_fields = [\"PatientName\", \"UnknownField\"]\n";
        fs::write(&path, toml).expect("should write temp settings");

        let loaded = load_visible_metadata_fields(&path).expect("settings should load");
        assert!(loaded.contains("PatientName"));
        assert!(!loaded.contains("UnknownField"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn local_paths_from_dropped_files_ignores_entries_without_paths() {
        let dropped_files = vec![
            egui::DroppedFile {
                path: Some(PathBuf::from("first.dcm")),
                ..Default::default()
            },
            egui::DroppedFile {
                name: "browser-upload.dcm".to_string(),
                ..Default::default()
            },
            egui::DroppedFile {
                path: Some(PathBuf::from("second.dcm")),
                ..Default::default()
            },
        ];

        assert_eq!(
            DicomViewerApp::local_paths_from_dropped_files(&dropped_files),
            vec![PathBuf::from("first.dcm"), PathBuf::from("second.dcm")]
        );
    }

    #[test]
    fn apply_dropped_files_queues_local_paths_for_open() {
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp::default();
        let dropped_files = vec![egui::DroppedFile {
            path: Some(PathBuf::from("dropped.dcm")),
            ..Default::default()
        }];

        app.apply_dropped_files(&dropped_files, &ctx);

        assert_eq!(
            app.pending_local_open_paths,
            Some(vec![PathBuf::from("dropped.dcm")])
        );
        assert!(!app.pending_local_open_armed);
        assert!(app.load_error_message.is_none());
    }

    #[test]
    fn apply_dropped_files_without_paths_sets_user_visible_error() {
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp::default();
        let dropped_files = vec![egui::DroppedFile {
            name: "web-upload.dcm".to_string(),
            ..Default::default()
        }];

        app.apply_dropped_files(&dropped_files, &ctx);

        assert_eq!(
            app.load_error_message.as_deref(),
            Some("Dropped items did not include readable local file paths.")
        );
        assert!(app.pending_local_open_paths.is_none());
    }

    #[test]
    fn file_drop_overlay_heading_matches_hovered_file_count() {
        assert_eq!(
            DicomViewerApp::file_drop_overlay_heading(&[]),
            "Drop DICOM files to open them"
        );

        let single = vec![egui::HoveredFile {
            path: Some(PathBuf::from("single.dcm")),
            ..Default::default()
        }];
        assert_eq!(
            DicomViewerApp::file_drop_overlay_heading(&single),
            "Drop 1 file to open it"
        );

        let portal_drag = vec![egui::HoveredFile {
            path: None,
            ..Default::default()
        }];
        assert_eq!(
            DicomViewerApp::file_drop_overlay_heading(&portal_drag),
            "Only local files can be dropped here"
        );

        let mixed_drag = vec![
            egui::HoveredFile {
                path: Some(PathBuf::from("local.dcm")),
                ..Default::default()
            },
            egui::HoveredFile {
                path: None,
                ..Default::default()
            },
        ];
        assert_eq!(
            DicomViewerApp::file_drop_overlay_heading(&mixed_drag),
            "Drop 1 file to open it"
        );

        let multi = vec![
            egui::HoveredFile {
                path: Some(PathBuf::from("one.dcm")),
                ..Default::default()
            },
            egui::HoveredFile {
                path: Some(PathBuf::from("two.dcm")),
                ..Default::default()
            },
            egui::HoveredFile {
                path: Some(PathBuf::from("three.dcm")),
                ..Default::default()
            },
        ];
        assert_eq!(
            DicomViewerApp::file_drop_overlay_heading(&multi),
            "Drop 3 files to open them"
        );
    }

    #[test]
    fn streaming_mammo_group_counts_as_group() {
        let mut app = DicomViewerApp::default();
        assert!(!app.has_mammo_group());

        let (_tx, rx) = mpsc::channel::<Result<PendingLoad, String>>();
        app.mammo_load_receiver = Some(rx);
        assert!(app.has_mammo_group());

        app.mammo_load_receiver = None;
        let (_tx, rx) = mpsc::channel::<DicomWebGroupStreamUpdate>();
        app.dicomweb_active_group_expected = Some(2);
        app.dicomweb_active_path_receiver = Some(rx);
        assert!(app.has_mammo_group());

        app.dicomweb_active_path_receiver = None;
        let (_tx, rx) = mpsc::channel::<DicomWebGroupStreamUpdate>();
        app.dicomweb_active_group_expected = Some(3);
        app.dicomweb_active_path_receiver = Some(rx);
        assert!(app.has_mammo_group());

        app.dicomweb_active_path_receiver = None;
        let (_tx, rx) = mpsc::channel::<DicomWebGroupStreamUpdate>();
        app.dicomweb_active_group_expected = Some(4);
        app.dicomweb_active_path_receiver = Some(rx);
        assert!(app.has_mammo_group());

        app.dicomweb_active_path_receiver = None;
        app.dicomweb_active_group_expected = Some(4);
        app.dicomweb_active_pending_paths
            .push_back(PathBuf::from("streamed.dcm"));
        assert!(app.has_mammo_group());

        app.dicomweb_active_path_receiver = None;
        app.dicomweb_active_pending_paths.clear();
        app.dicomweb_active_group_expected = Some(8);
        assert!(DicomViewerApp::is_supported_multi_view_group_size(8));
        assert_eq!(DicomViewerApp::multi_view_grid_dimensions(8), Some((2, 4)));
        assert_eq!(DicomViewerApp::multi_view_layout_label(8), "2x4");
        let (_tx, rx) = mpsc::channel::<DicomWebGroupStreamUpdate>();
        app.dicomweb_active_path_receiver = Some(rx);
        assert!(app.has_mammo_group());

        app.dicomweb_active_path_receiver = None;
        app.dicomweb_active_pending_paths
            .push_back(PathBuf::from("streamed-8-up.dcm"));
        assert!(app.has_mammo_group());
    }

    #[test]
    fn three_up_multi_view_layout_is_supported() {
        assert!(DicomViewerApp::is_supported_multi_view_group_size(3));
        assert_eq!(DicomViewerApp::multi_view_grid_dimensions(3), Some((1, 3)));
        assert_eq!(DicomViewerApp::multi_view_layout_label(3), "1x3");
        assert!(DicomViewerApp::is_supported_multi_view_group_size(8));
        assert_eq!(DicomViewerApp::multi_view_grid_dimensions(8), Some((2, 4)));
        assert_eq!(DicomViewerApp::multi_view_layout_label(8), "2x4");
    }

    #[test]
    fn reorder_items_by_indices_reorders_valid_permutation() {
        let items = vec![10, 20, 30, 40];
        assert_eq!(
            DicomViewerApp::reorder_items_by_indices(items, vec![2, 0, 3, 1]),
            vec![30, 10, 40, 20]
        );
    }

    #[test]
    fn restore_ordered_items_or_log_rejects_invalid_inputs() {
        let items = vec![10, 20, 30, 40];
        for invalid_indices in [vec![0, 1, 2], vec![0, 1, 1, 3], vec![0, 1, 2, 4]] {
            let (ordered, selected, reordered) = DicomViewerApp::restore_ordered_items_or_log(
                items.clone(),
                invalid_indices,
                Some(2),
                "test invalid reorder",
            );
            assert!(!reordered);
            assert_eq!(ordered, items);
            assert_eq!(selected, Some(2));
        }
    }

    #[test]
    fn restore_ordered_items_or_log_consistent_with_group_presence_states() {
        let items = vec![10, 20, 30, 40];
        let valid_indices = vec![1, 0, 3, 2];
        let invalid_indices = vec![0, 1, 1, 3];
        let expected = vec![20, 10, 40, 30];

        let mut app = DicomViewerApp::default();
        assert!(!app.has_mammo_group());
        let (ordered, selected, reordered) = DicomViewerApp::restore_ordered_items_or_log(
            items.clone(),
            valid_indices.clone(),
            Some(2),
            "test valid reorder",
        );
        assert!(reordered);
        assert_eq!(ordered, expected);
        assert_eq!(selected, Some(3));

        let (ordered, selected, reordered) = DicomViewerApp::restore_ordered_items_or_log(
            items.clone(),
            invalid_indices.clone(),
            Some(2),
            "test invalid reorder",
        );
        assert!(!reordered);
        assert_eq!(ordered, items.clone());
        assert_eq!(selected, Some(2));

        let (_tx, rx) = mpsc::channel::<DicomWebGroupStreamUpdate>();
        app.dicomweb_active_group_expected = Some(4);
        app.dicomweb_active_path_receiver = Some(rx);
        assert!(app.has_mammo_group());
        assert!(DicomViewerApp::is_supported_multi_view_group_size(4));
        assert_eq!(DicomViewerApp::multi_view_grid_dimensions(4), Some((2, 2)));
        assert_eq!(DicomViewerApp::multi_view_layout_label(4), "2x2");
        let (ordered, selected, reordered) = DicomViewerApp::restore_ordered_items_or_log(
            items.clone(),
            valid_indices.clone(),
            Some(2),
            "test valid reorder",
        );
        assert!(reordered);
        assert_eq!(ordered, expected);
        assert_eq!(selected, Some(3));
        let (ordered, selected, reordered) = DicomViewerApp::restore_ordered_items_or_log(
            items.clone(),
            invalid_indices.clone(),
            Some(2),
            "test invalid reorder",
        );
        assert!(!reordered);
        assert_eq!(ordered, items.clone());
        assert_eq!(selected, Some(2));

        app.dicomweb_active_path_receiver = None;
        app.dicomweb_active_pending_paths
            .push_back(PathBuf::from("pending-stream.dcm"));
        assert!(app.has_mammo_group());
        assert!(DicomViewerApp::is_supported_multi_view_group_size(8));
        assert_eq!(DicomViewerApp::multi_view_grid_dimensions(8), Some((2, 4)));
        assert_eq!(DicomViewerApp::multi_view_layout_label(8), "2x4");
        let (ordered, selected, reordered) = DicomViewerApp::restore_ordered_items_or_log(
            items.clone(),
            valid_indices.clone(),
            Some(2),
            "test valid reorder",
        );
        assert!(reordered);
        assert_eq!(ordered, expected);
        assert_eq!(selected, Some(3));
        let (ordered, selected, reordered) = DicomViewerApp::restore_ordered_items_or_log(
            items,
            invalid_indices,
            Some(2),
            "test invalid reorder",
        );
        assert!(!reordered);
        assert_eq!(ordered, vec![10, 20, 30, 40]);
        assert_eq!(selected, Some(2));

        app.dicomweb_active_pending_paths.clear();
        let (_tx, rx) = mpsc::channel::<DicomWebGroupStreamUpdate>();
        app.dicomweb_active_group_expected = Some(5);
        app.dicomweb_active_path_receiver = Some(rx);
        assert!(!app.has_mammo_group());
    }

    #[test]
    fn merge_gsps_overlays_appends_to_existing_uid() {
        let mut destination = HashMap::new();
        destination.insert(
            "1.2.3".to_string(),
            GspsOverlay::from_graphics(vec![GspsGraphic::Point {
                x: 1.0,
                y: 2.0,
                units: GspsUnits::Pixel,
            }]),
        );

        let mut source = HashMap::new();
        source.insert(
            "1.2.3".to_string(),
            GspsOverlay::from_graphics(vec![GspsGraphic::Polyline {
                points: vec![(0.0, 0.0), (1.0, 1.0)],
                units: GspsUnits::Display,
                closed: false,
            }]),
        );
        source.insert(
            "9.9.9".to_string(),
            GspsOverlay::from_graphics(vec![GspsGraphic::Point {
                x: 9.0,
                y: 9.0,
                units: GspsUnits::Pixel,
            }]),
        );

        DicomViewerApp::merge_gsps_overlays(&mut destination, source);
        assert_eq!(
            destination
                .get("1.2.3")
                .map(|overlay| overlay.graphics.len()),
            Some(2)
        );
        assert_eq!(
            destination
                .get("9.9.9")
                .map(|overlay| overlay.graphics.len()),
            Some(1)
        );
    }

    #[test]
    fn toggle_gsps_overlay_without_active_overlay_resets_to_off() {
        let mut app = DicomViewerApp {
            gsps_overlay_visible: true,
            ..Default::default()
        };
        app.toggle_gsps_overlay();
        assert!(!app.gsps_overlay_visible);
    }

    #[test]
    fn toggle_gsps_overlay_allows_group_overlay_when_other_viewport_is_selected() {
        let overlay = GspsOverlay::from_graphics(vec![GspsGraphic::Point {
            x: 1.0,
            y: 1.0,
            units: GspsUnits::Pixel,
        }]);
        let texture_image = ColorImage {
            size: [1, 1],
            pixels: vec![egui::Color32::BLACK],
        };
        let ctx = egui::Context::default();
        let texture_a = ctx.load_texture(
            "test-gsps-toggle-a",
            texture_image.clone(),
            TextureOptions::LINEAR,
        );
        let texture_b =
            ctx.load_texture("test-gsps-toggle-b", texture_image, TextureOptions::LINEAR);

        let mut app = DicomViewerApp {
            mammo_group: vec![
                Some(MammoViewport {
                    path: PathBuf::from("a.dcm"),
                    image: DicomImage::test_stub(None),
                    texture: texture_a,
                    label: "A".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
                Some(MammoViewport {
                    path: PathBuf::from("b.dcm"),
                    image: DicomImage::test_stub(Some(overlay)),
                    texture: texture_b,
                    label: "B".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
            ],
            mammo_selected_index: 0,
            ..Default::default()
        };

        app.toggle_gsps_overlay();
        assert!(app.gsps_overlay_visible);
    }

    #[test]
    fn jump_to_next_gsps_overlay_cycles_single_view_frames() {
        let overlay = GspsOverlay {
            graphics: vec![crate::dicom::GspsOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 1.0,
                    y: 1.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: Some(vec![2, 4]),
            }],
        };
        let mut app = DicomViewerApp {
            image: Some(DicomImage::test_stub_with_mono_frames(Some(overlay), 4)),
            ..Default::default()
        };
        let ctx = egui::Context::default();

        app.jump_to_next_gsps_overlay(&ctx);
        assert!(app.gsps_overlay_visible);
        assert_eq!(app.current_frame, 1);

        app.jump_to_next_gsps_overlay(&ctx);
        assert_eq!(app.current_frame, 3);

        app.jump_to_next_gsps_overlay(&ctx);
        assert_eq!(app.current_frame, 1);
    }

    #[test]
    fn jump_to_next_gsps_overlay_advances_when_current_target_is_hidden() {
        let overlay = GspsOverlay {
            graphics: vec![crate::dicom::GspsOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 1.0,
                    y: 1.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: Some(vec![2, 4]),
            }],
        };
        let mut app = DicomViewerApp {
            image: Some(DicomImage::test_stub_with_mono_frames(Some(overlay), 4)),
            current_frame: 1,
            ..Default::default()
        };
        let ctx = egui::Context::default();

        app.jump_to_next_gsps_overlay(&ctx);

        assert!(app.gsps_overlay_visible);
        assert_eq!(app.current_frame, 3);
    }

    #[test]
    fn jump_to_next_gsps_overlay_cycles_group_viewports_and_frames() {
        let overlay_a = GspsOverlay {
            graphics: vec![crate::dicom::GspsOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 1.0,
                    y: 1.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: Some(vec![2]),
            }],
        };
        let overlay_b = GspsOverlay {
            graphics: vec![crate::dicom::GspsOverlayGraphic {
                graphic: GspsGraphic::Polyline {
                    points: vec![(0.0, 0.0), (1.0, 1.0)],
                    units: GspsUnits::Display,
                    closed: false,
                },
                referenced_frames: Some(vec![1]),
            }],
        };
        let texture_image = ColorImage {
            size: [1, 1],
            pixels: vec![egui::Color32::BLACK],
        };
        let ctx = egui::Context::default();
        let texture_a = ctx.load_texture(
            "test-gsps-next-a",
            texture_image.clone(),
            TextureOptions::LINEAR,
        );
        let texture_b = ctx.load_texture("test-gsps-next-b", texture_image, TextureOptions::LINEAR);
        let mut app = DicomViewerApp {
            mammo_group: vec![
                Some(MammoViewport {
                    path: PathBuf::from("a.dcm"),
                    image: DicomImage::test_stub_with_mono_frames(Some(overlay_a), 3),
                    texture: texture_a,
                    label: "A".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
                Some(MammoViewport {
                    path: PathBuf::from("b.dcm"),
                    image: DicomImage::test_stub_with_mono_frames(Some(overlay_b), 3),
                    texture: texture_b,
                    label: "B".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
            ],
            mammo_selected_index: 0,
            ..Default::default()
        };

        app.jump_to_next_gsps_overlay(&ctx);
        assert!(app.gsps_overlay_visible);
        assert_eq!(app.mammo_selected_index, 0);
        assert_eq!(app.selected_mammo_frame_index(), 1);

        app.jump_to_next_gsps_overlay(&ctx);
        assert_eq!(app.mammo_selected_index, 1);
        assert_eq!(app.selected_mammo_frame_index(), 0);

        app.jump_to_next_gsps_overlay(&ctx);
        assert_eq!(app.mammo_selected_index, 0);
        assert_eq!(app.selected_mammo_frame_index(), 1);
    }

    #[test]
    fn open_history_entry_single_clears_load_error() {
        let ctx = egui::Context::default();
        let texture = ctx.load_texture(
            "history-single-error-clear",
            ColorImage {
                size: [1, 1],
                pixels: vec![egui::Color32::BLACK],
            },
            TextureOptions::LINEAR,
        );
        let mut app = DicomViewerApp {
            load_error_message: Some("Previous load failed.".to_string()),
            history_entries: vec![HistoryEntry {
                id: "single".to_string(),
                kind: HistoryKind::Single(Box::new(HistorySingleData {
                    path: PathBuf::from("cached-single.dcm"),
                    image: DicomImage::test_stub(None),
                    texture,
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    cine_fps: DEFAULT_CINE_FPS,
                })),
                thumbs: Vec::new(),
            }],
            ..Default::default()
        };

        app.open_history_entry(0, &ctx);

        assert!(app.load_error_message.is_none());
    }

    #[test]
    fn open_history_entry_group_clears_load_error() {
        let ctx = egui::Context::default();
        let texture_image = ColorImage {
            size: [1, 1],
            pixels: vec![egui::Color32::BLACK],
        };
        let texture_a = ctx.load_texture(
            "history-group-error-clear-a",
            texture_image.clone(),
            TextureOptions::LINEAR,
        );
        let texture_b = ctx.load_texture(
            "history-group-error-clear-b",
            texture_image,
            TextureOptions::LINEAR,
        );
        let mut app = DicomViewerApp {
            load_error_message: Some("Previous load failed.".to_string()),
            history_entries: vec![HistoryEntry {
                id: "group".to_string(),
                kind: HistoryKind::Group(HistoryGroupData {
                    viewports: vec![
                        HistoryGroupViewportData {
                            path: PathBuf::from("cached-a.dcm"),
                            image: DicomImage::test_stub(None),
                            texture: texture_a,
                            label: "A".to_string(),
                            window_center: 0.0,
                            window_width: 1.0,
                            current_frame: 0,
                        },
                        HistoryGroupViewportData {
                            path: PathBuf::from("cached-b.dcm"),
                            image: DicomImage::test_stub(None),
                            texture: texture_b,
                            label: "B".to_string(),
                            window_center: 0.0,
                            window_width: 1.0,
                            current_frame: 0,
                        },
                    ],
                    selected_index: 0,
                }),
                thumbs: Vec::new(),
            }],
            ..Default::default()
        };

        app.open_history_entry(0, &ctx);

        assert!(app.load_error_message.is_none());
    }

    #[test]
    fn open_history_entry_report_clears_load_error() {
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            load_error_message: Some("Previous load failed.".to_string()),
            history_entries: vec![report_history_entry(
                &ctx,
                "cached-report.dcm",
                "history-report-error-clear",
            )],
            ..Default::default()
        };

        app.open_history_entry(0, &ctx);

        assert!(app.load_error_message.is_none());
        assert_eq!(
            app.report.as_ref().map(|report| report.title.as_str()),
            Some("Structured Report")
        );
        assert_eq!(
            app.current_single_path,
            Some(PathBuf::from("cached-report.dcm"))
        );
    }

    #[test]
    fn open_history_entry_report_clears_active_group_view() {
        let ctx = egui::Context::default();
        let texture = test_texture(&ctx, "active-group-texture");
        let mut app = DicomViewerApp {
            mammo_group: vec![
                Some(MammoViewport {
                    path: PathBuf::from("group-a.dcm"),
                    image: DicomImage::test_stub(None),
                    texture: texture.clone(),
                    label: "A".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
                Some(MammoViewport {
                    path: PathBuf::from("group-b.dcm"),
                    image: DicomImage::test_stub(None),
                    texture,
                    label: "B".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
            ],
            history_entries: vec![report_history_entry(
                &ctx,
                "cached-report.dcm",
                "history-report-clears-group",
            )],
            ..Default::default()
        };

        app.open_history_entry(0, &ctx);

        assert!(app.mammo_group.is_empty());
        assert_eq!(
            app.report.as_ref().map(|report| report.title.as_str()),
            Some("Structured Report")
        );
    }

    #[test]
    fn poll_history_preload_can_enqueue_report_history_entry() {
        let (tx, rx) = mpsc::channel::<Result<HistoryPreloadResult, String>>();
        tx.send(Ok(HistoryPreloadResult::Report {
            path: PathBuf::from("preloaded-report.dcm"),
            report: Box::new(StructuredReportDocument::test_stub()),
        }))
        .expect("report preload should send");

        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            history_preload_receiver: Some(rx),
            ..Default::default()
        };

        app.poll_history_preload(&ctx);

        assert_eq!(app.history_entries.len(), 1);
        assert_eq!(
            app.history_entries[0].id,
            history_id_from_paths(&[PathBuf::from("preloaded-report.dcm")])
        );
    }

    #[test]
    fn close_current_group_removes_active_entry_and_opens_next_history_item() {
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            image: Some(DicomImage::test_stub(None)),
            current_single_path: Some(PathBuf::from("current.dcm")),
            texture: Some(test_texture(&ctx, "active-current")),
            history_entries: vec![
                single_history_entry(&ctx, "current.dcm", "history-current"),
                single_history_entry(&ctx, "next.dcm", "history-next"),
            ],
            ..Default::default()
        };

        app.close_current_group(&ctx);

        assert_eq!(app.current_single_path, Some(PathBuf::from("next.dcm")));
        assert_eq!(app.history_entries.len(), 1);
        assert_eq!(
            app.history_entries[0].id,
            history_id_from_paths(&[PathBuf::from("next.dcm")])
        );
    }

    #[test]
    fn close_current_group_without_history_clears_viewer_state() {
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            image: Some(DicomImage::test_stub(None)),
            current_single_path: Some(PathBuf::from("lonely.dcm")),
            texture: Some(test_texture(&ctx, "active-lonely")),
            ..Default::default()
        };

        app.close_current_group(&ctx);

        assert!(app.image.is_none());
        assert!(app.current_single_path.is_none());
        assert!(app.texture.is_none());
        assert!(!app.has_open_study());
    }

    #[test]
    fn handle_close_group_shortcut_requests_window_close_when_viewer_is_empty() {
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp::default();

        assert!(app.handle_close_group_shortcut(&ctx));
    }

    #[test]
    fn poll_dicomweb_grouped_keeps_load_error_until_active_group_is_ready() {
        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        tx.send(Ok(DicomWebDownloadResult::Grouped {
            groups: vec![vec![
                PathBuf::from("group-a.dcm"),
                PathBuf::from("group-b.dcm"),
            ]],
            open_group: 0,
        }))
        .expect("grouped result should send");

        let mut app = DicomViewerApp {
            dicomweb_receiver: Some(rx),
            dicomweb_active_group_paths: vec![PathBuf::from("group-a.dcm")],
            load_error_message: Some(
                "Streaming multi-view load channel was not available.".to_string(),
            ),
            ..Default::default()
        };

        let ctx = egui::Context::default();
        app.poll_dicomweb_download(&ctx);

        assert_eq!(
            app.load_error_message.as_deref(),
            Some("Streaming multi-view load channel was not available.")
        );
    }

    #[test]
    fn poll_single_load_sets_user_visible_error_on_failure() {
        let (tx, rx) = mpsc::channel::<Result<PendingSingleLoad, String>>();
        tx.send(Err(
            "Error opening selected DICOM: decode failed".to_string()
        ))
        .expect("failure should send");

        let mut app = DicomViewerApp {
            single_load_receiver: Some(rx),
            ..Default::default()
        };

        let ctx = egui::Context::default();
        app.poll_single_load(&ctx);

        assert_eq!(
            app.load_error_message.as_deref(),
            Some("Failed to load selected DICOM.")
        );
    }

    #[test]
    fn poll_single_load_clears_user_visible_error_on_success() {
        let (tx, rx) = mpsc::channel::<Result<PendingSingleLoad, String>>();
        tx.send(Ok(PendingSingleLoad::Image(PendingLoad {
            path: PathBuf::from("selected.dcm"),
            image: DicomImage::test_stub(None),
        })))
        .expect("success should send");

        let mut app = DicomViewerApp {
            single_load_receiver: Some(rx),
            load_error_message: Some("Previous load failed.".to_string()),
            ..Default::default()
        };

        let ctx = egui::Context::default();
        app.poll_single_load(&ctx);

        assert!(app.load_error_message.is_none());
    }

    #[test]
    fn poll_single_load_can_activate_structured_report() {
        let (tx, rx) = mpsc::channel::<Result<PendingSingleLoad, String>>();
        tx.send(Ok(PendingSingleLoad::StructuredReport {
            path: PathBuf::from("report.dcm"),
            report: StructuredReportDocument::test_stub(),
        }))
        .expect("report should send");

        let mut app = DicomViewerApp {
            single_load_receiver: Some(rx),
            load_error_message: Some("Previous load failed.".to_string()),
            ..Default::default()
        };

        let ctx = egui::Context::default();
        app.poll_single_load(&ctx);

        assert!(app.load_error_message.is_none());
        assert!(app.image.is_none());
        assert!(app.texture.is_none());
        assert_eq!(app.current_single_path, Some(PathBuf::from("report.dcm")));
        assert_eq!(
            app.report.as_ref().map(|report| report.title.as_str()),
            Some("Structured Report")
        );
        assert_eq!(app.history_entries.len(), 1);
        assert_eq!(
            app.history_entries[0].id,
            history_id_from_paths(&[PathBuf::from("report.dcm")])
        );
    }

    #[test]
    fn load_selected_paths_with_invalid_count_sets_user_visible_error() {
        let mut app = DicomViewerApp::default();
        let ctx = egui::Context::default();
        let paths = (0..5)
            .map(|index| PathBuf::from(format!("invalid-{index}.dcm")))
            .collect::<Vec<_>>();
        let expected = DicomViewerApp::format_select_paths_count_error(5);

        let result = app.load_selected_paths(paths, &ctx);

        assert!(result.is_err());
        assert_eq!(app.load_error_message.as_deref(), Some(expected.as_str()));
    }

    #[test]
    fn apply_prepared_load_paths_with_other_only_sets_user_visible_error() {
        let mut app = DicomViewerApp::default();
        let ctx = egui::Context::default();

        let result = app.apply_prepared_load_paths(
            PreparedLoadPaths {
                other_files_found: 1,
                ..Default::default()
            },
            &ctx,
        );

        assert!(result.is_err());
        assert_eq!(
            app.load_error_message.as_deref(),
            Some("Selected DICOM objects are not displayable images or structured reports.")
        );
    }

    #[test]
    fn poll_dicomweb_single_preserves_error_on_sync_rejection() {
        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        tx.send(Ok(DicomWebDownloadResult::Single(
            (0..5)
                .map(|index| PathBuf::from(format!("invalid-{index}.dcm")))
                .collect::<Vec<_>>(),
        )))
        .expect("single result should send");

        let mut app = DicomViewerApp {
            dicomweb_receiver: Some(rx),
            dicomweb_active_group_expected: Some(3),
            dicomweb_active_group_paths: vec![PathBuf::from("active-a.dcm")],
            dicomweb_active_pending_paths: VecDeque::from(vec![PathBuf::from("pending.dcm")]),
            history_pushed_for_active_group: true,
            load_error_message: Some("Previous load failed.".to_string()),
            ..Default::default()
        };
        let expected = DicomViewerApp::format_select_paths_count_error(5);

        let ctx = egui::Context::default();
        app.poll_dicomweb_download(&ctx);

        assert_eq!(app.load_error_message.as_deref(), Some(expected.as_str()));
        assert_eq!(app.dicomweb_active_group_expected, Some(3));
        assert_eq!(
            app.dicomweb_active_group_paths,
            vec![PathBuf::from("active-a.dcm")]
        );
        assert_eq!(
            app.dicomweb_active_pending_paths,
            VecDeque::from(vec![PathBuf::from("pending.dcm")])
        );
        assert!(app.history_pushed_for_active_group);
    }
}
