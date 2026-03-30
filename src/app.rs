use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use eframe::egui::{
    self, ColorImage, ResizeDirection, Sense, TextureHandle, TextureOptions, ViewportCommand,
};

use crate::dicom::{
    classify_dicom_path, load_dicom, load_gsps_overlays, load_mammography_cad_sr_overlays,
    load_parametric_map, load_parametric_map_overlays, load_structured_report,
    read_sop_instance_uid, DicomImage, DicomPathKind, DicomSource, DicomSourceMeta,
    FullMetadataField, GspsGraphic, GspsOverlay, GspsUnits, ParametricMapOverlay, SrOverlay,
    StructuredReportDocument, StructuredReportNode, METADATA_FIELD_NAMES,
};
use crate::dicomweb::{
    download_dicomweb_group_request, download_dicomweb_request, DicomWebDownloadResult,
    DicomWebGroupStreamUpdate,
};
use crate::launch::{DicomWebGroupedLaunchRequest, DicomWebLaunchRequest, LaunchRequest};
use crate::mammo::{mammo_image_align, mammo_label, order_mammo_indices, preferred_mammo_slot};
use crate::renderer::{blend_rgba_overlay, render_rgb, render_window_level};

mod history;
mod load;
mod metadata;
mod overlay;

#[cfg(test)]
use self::history::{
    history_id_from_paths, HistoryGroupData, HistoryGroupViewportData, HistoryReportData,
    HistoryThumb,
};
use self::history::{
    HistoryEntry, HistoryKind, HistoryPreloadJob, HistoryPreloadJobKey, HistoryPreloadResult,
    HistorySingleData,
};
use self::load::{PendingLoad, PendingSingleLoad, PreparedLoadPaths};

const APP_TITLE: &str = "Perspecta Viewer";
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const TITLE_TEXT_SIZE: f32 = 14.0;
const HISTORY_MAX_ENTRIES: usize = 24;
const HISTORY_THUMB_MAX_DIM: usize = 96;
const HISTORY_LIST_THUMB_MAX_DIM: f32 = 56.0;
const DEFAULT_CINE_FPS: f32 = 24.0;
const VALID_GROUP_SIZES: &[usize] = &[1, 2, 3, 4, 8];
const PERSPECTA_BRAND_BLUE: egui::Color32 = egui::Color32::from_rgb(14, 165, 233);
const PERSPECTA_OVERLAY_ORANGE: egui::Color32 = egui::Color32::from_rgb(249, 115, 22);
const ICON_STROKE_WIDTH: f32 = 1.25;
const CLOSE_ICON_SIZE_FACTOR: f32 = 0.36;
const TITLEBAR_MINIMIZE_ICON_HORIZONTAL_PADDING: f32 = 10.0;
const TITLEBAR_MINIMIZE_ICON_VERTICAL_PADDING: f32 = 9.0;
const TITLEBAR_MAXIMIZE_ICON_MARGIN: f32 = 15.0;
const TITLEBAR_MAXIMIZE_ICON_MIN_SIDE: f32 = 1.0;
const ERROR_OVERLAY_CLOSE_BUTTON_SIZE: f32 = 18.0;
const CONTROL_VALUE_WIDTH: f32 = 64.0;
const CONTROL_ACTION_BUTTON_WIDTH: f32 = 110.0;
const FILE_DROP_OVERLAY_WIDTH: f32 = 420.0;
const DICOMWEB_ACTIVE_PENDING_BATCH_SIZE: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq)]
struct WlOverlayLayout {
    slider_row_width: f32,
    slider_widget_width: f32,
    action_row_width: f32,
    area_width: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WlOverlayRow {
    Center,
    Width,
    Frame,
    CineFps,
    ToggleCine,
    ToggleOverlay,
    NextOverlay,
}

#[derive(Clone)]
struct MammoViewport {
    path: DicomSourceMeta,
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

enum FullMetadataLoadResult {
    Loaded {
        source: DicomSource,
        metadata: Arc<[FullMetadataField]>,
    },
    Failed {
        source: DicomSource,
    },
}

pub struct DicomViewerApp {
    image: Option<DicomImage>,
    report: Option<StructuredReportDocument>,
    current_single_path: Option<DicomSourceMeta>,
    texture: Option<TextureHandle>,
    mammo_group: Vec<Option<MammoViewport>>,
    mammo_selected_index: usize,
    history_entries: Vec<HistoryEntry>,
    visible_metadata_fields: HashSet<String>,
    full_metadata_popup_open: bool,
    settings_path: Option<PathBuf>,
    history_nonce: u64,
    pending_history_open_id: Option<String>,
    pending_history_open_armed: bool,
    pending_local_open_paths: Option<Vec<PathBuf>>,
    pending_local_open_armed: bool,
    pending_launch_request: Option<LaunchRequest>,
    dicomweb_receiver: Option<Receiver<Result<DicomWebDownloadResult, String>>>,
    dicomweb_active_path_receiver: Option<Receiver<DicomWebGroupStreamUpdate>>,
    dicomweb_active_group_expected: Option<usize>,
    dicomweb_active_group_paths: Vec<DicomSourceMeta>,
    dicomweb_completed_background_groups: HashSet<usize>,
    dicomweb_active_pending_paths: VecDeque<DicomSource>,
    full_metadata_receiver: Option<Receiver<FullMetadataLoadResult>>,
    full_metadata_sender: Option<Sender<FullMetadataLoadResult>>,
    single_load_receiver: Option<Receiver<Result<PendingSingleLoad, String>>>,
    mammo_load_receiver: Option<Receiver<Result<PendingLoad, String>>>,
    mammo_load_sender: Option<Sender<Result<PendingLoad, String>>>,
    history_pushed_for_active_group: bool,
    history_preload_receiver: Option<Receiver<Result<HistoryPreloadResult, String>>>,
    history_preload_queue: VecDeque<HistoryPreloadJob>,
    history_preload_active_key: Option<HistoryPreloadJobKey>,
    window_center: f32,
    window_width: f32,
    pending_gsps_overlays: HashMap<String, GspsOverlay>,
    authoritative_gsps_overlay_keys: HashSet<String>,
    pending_sr_overlays: HashMap<String, SrOverlay>,
    authoritative_sr_overlay_keys: HashSet<String>,
    pending_pm_overlays: HashMap<String, ParametricMapOverlay>,
    authoritative_pm_overlay_keys: HashSet<String>,
    overlay_visible: bool,
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
        let (full_metadata_sender, full_metadata_receiver) = mpsc::channel();
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
            full_metadata_popup_open: false,
            settings_path,
            history_nonce: 0,
            pending_history_open_id: None,
            pending_history_open_armed: false,
            pending_local_open_paths: None,
            pending_local_open_armed: false,
            pending_launch_request: initial_request,
            dicomweb_receiver: None,
            dicomweb_active_path_receiver: None,
            dicomweb_active_group_expected: None,
            dicomweb_active_group_paths: Vec::new(),
            dicomweb_completed_background_groups: HashSet::new(),
            dicomweb_active_pending_paths: VecDeque::new(),
            full_metadata_receiver: Some(full_metadata_receiver),
            full_metadata_sender: Some(full_metadata_sender),
            single_load_receiver: None,
            mammo_load_receiver: None,
            mammo_load_sender: None,
            history_pushed_for_active_group: false,
            history_preload_receiver: None,
            history_preload_queue: VecDeque::new(),
            history_preload_active_key: None,
            window_center: 0.0,
            window_width: 1.0,
            pending_gsps_overlays: HashMap::new(),
            authoritative_gsps_overlay_keys: HashSet::new(),
            pending_sr_overlays: HashMap::new(),
            authoritative_sr_overlay_keys: HashSet::new(),
            pending_pm_overlays: HashMap::new(),
            authoritative_pm_overlay_keys: HashSet::new(),
            overlay_visible: false,
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
            || !self.history_preload_queue.is_empty()
            || self.pending_history_open_id.is_some()
            || self.pending_local_open_paths.is_some()
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
            || (prepared.image_paths.is_empty()
                && (!prepared.structured_report_paths.is_empty()
                    || !prepared.parametric_map_paths.is_empty()))
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
        let showing_non_group_study = self.current_single_path.is_some() || self.report.is_some();
        !self.mammo_group.is_empty()
            || self.mammo_load_receiver.is_some()
            || (!showing_non_group_study
                && self
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

        let overlay_visible = self.overlay_visible;
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
                            Self::render_image_frame(
                                image,
                                *safe_frame,
                                *center,
                                *width,
                                overlay_visible,
                            )
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
        let Some(entry_id) = self
            .history_entries
            .get(index)
            .map(|entry| entry.id.clone())
        else {
            return;
        };
        if self.pending_history_open_id.is_none() {
            self.pending_history_open_armed = false;
        }
        self.pending_history_open_id = Some(entry_id);
    }

    fn process_pending_history_open(&mut self, ctx: &egui::Context) {
        let Some(entry_id) = self.pending_history_open_id.clone() else {
            return;
        };

        if !self.pending_history_open_armed {
            self.pending_history_open_armed = true;
            ctx.request_repaint();
            return;
        }

        self.pending_history_open_id = None;
        self.pending_history_open_armed = false;
        let Some(index) = self
            .history_entries
            .iter()
            .position(|entry| entry.id == entry_id)
        else {
            return;
        };
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
        self.overlay_visible = false;
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
        show_overlay: bool,
    ) -> Option<ColorImage> {
        let mut color_image = if image.is_monochrome() {
            let frame_pixels = image.frame_mono_pixels(frame_index)?;
            render_window_level(
                image.width,
                image.height,
                frame_pixels.as_ref(),
                image.invert,
                window_center,
                window_width,
            )
        } else {
            let frame_pixels = image.frame_rgb_pixels(frame_index)?;
            render_rgb(
                image.width,
                image.height,
                frame_pixels.as_ref(),
                image.samples_per_pixel,
            )
        };

        if show_overlay {
            Self::blend_parametric_map_overlay(&mut color_image, image, frame_index);
        }

        Some(color_image)
    }

    fn blend_parametric_map_overlay(
        color_image: &mut ColorImage,
        image: &DicomImage,
        frame_index: usize,
    ) {
        let Some(overlay) = image.pm_overlay.as_ref() else {
            return;
        };
        let Some(stored_frame_index) = image.display_frame_index_to_stored(frame_index) else {
            return;
        };

        for overlay_rgba in
            overlay.rgba_frames_for_source_frame(stored_frame_index, image.frame_count())
        {
            blend_rgba_overlay(color_image, overlay_rgba.as_ref());
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
                self.overlay_visible,
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

    fn active_image_mut(&mut self) -> Option<&mut DicomImage> {
        if self.image.is_some() {
            self.image.as_mut()
        } else {
            self.selected_mammo_viewport_mut()
                .map(|viewport| &mut viewport.image)
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

    fn displayed_study_matches_paths<T>(&self, image_paths: &[T]) -> bool
    where
        T: Clone + Into<DicomSourceMeta>,
    {
        let image_paths = image_paths
            .iter()
            .cloned()
            .map(Into::into)
            .collect::<Vec<_>>();
        match image_paths.as_slice() {
            [ref path] => self
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
        let overlay_visible = self.overlay_visible;
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
            overlay_visible,
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
        let Some(stored_frame_index) = image.display_frame_index_to_stored(frame_index) else {
            return;
        };

        let stroke = egui::Stroke::new(1.6, PERSPECTA_BRAND_BLUE);
        let marker_half = (image_rect.width().min(image_rect.height()) * 0.008).clamp(2.0, 5.0);

        for graphic in overlay.graphics_for_frame(stored_frame_index) {
            Self::draw_overlay_graphic(painter, image_rect, image, graphic, stroke, marker_half);
        }
    }

    fn draw_sr_overlay(
        painter: &egui::Painter,
        image_rect: egui::Rect,
        image: &DicomImage,
        frame_index: usize,
    ) {
        let Some(overlay) = image.sr_overlay.as_ref() else {
            return;
        };
        if overlay.is_empty() {
            return;
        }
        let Some(stored_frame_index) = image.display_frame_index_to_stored(frame_index) else {
            return;
        };

        let stroke = egui::Stroke::new(1.6, PERSPECTA_OVERLAY_ORANGE);
        let marker_half = (image_rect.width().min(image_rect.height()) * 0.008).clamp(2.0, 5.0);

        for graphic in overlay.visible_graphics_for_frame(stored_frame_index) {
            Self::draw_overlay_graphic(painter, image_rect, image, graphic, stroke, marker_half);
        }
    }

    fn draw_overlay_graphic(
        painter: &egui::Painter,
        image_rect: egui::Rect,
        image: &DicomImage,
        graphic: &GspsGraphic,
        stroke: egui::Stroke,
        marker_half: f32,
    ) {
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
                    return;
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

    fn icon_stroke(ui: &egui::Ui, response: &egui::Response) -> egui::Stroke {
        egui::Stroke::new(
            ICON_STROKE_WIDTH,
            ui.style().interact(response).fg_stroke.color,
        )
    }

    fn register_icon_button_accessibility(response: &egui::Response, label: &'static str) {
        let enabled = response.enabled();
        response.widget_info(move || {
            egui::WidgetInfo::labeled(egui::WidgetType::Button, enabled, label)
        });
    }

    fn paint_close_icon(painter: &egui::Painter, button_rect: egui::Rect, stroke: egui::Stroke) {
        let icon_side = button_rect.width().min(button_rect.height()) * CLOSE_ICON_SIZE_FACTOR;
        let icon_rect =
            egui::Rect::from_center_size(button_rect.center(), egui::vec2(icon_side, icon_side));
        painter.line_segment([icon_rect.left_top(), icon_rect.right_bottom()], stroke);
        painter.line_segment([icon_rect.right_top(), icon_rect.left_bottom()], stroke);
    }

    fn paint_titlebar_minimize_icon(
        painter: &egui::Painter,
        button_rect: egui::Rect,
        stroke: egui::Stroke,
    ) {
        let icon_rect = button_rect.shrink2(egui::vec2(
            TITLEBAR_MINIMIZE_ICON_HORIZONTAL_PADDING,
            TITLEBAR_MINIMIZE_ICON_VERTICAL_PADDING,
        ));
        let y = icon_rect.center().y;
        painter.line_segment(
            [
                egui::pos2(icon_rect.left(), y),
                egui::pos2(icon_rect.right(), y),
            ],
            stroke,
        );
    }

    fn paint_titlebar_maximize_icon(
        painter: &egui::Painter,
        button_rect: egui::Rect,
        stroke: egui::Stroke,
    ) {
        let icon_side = (button_rect.height() - TITLEBAR_MAXIMIZE_ICON_MARGIN)
            .min(button_rect.width() - TITLEBAR_MAXIMIZE_ICON_MARGIN)
            .max(TITLEBAR_MAXIMIZE_ICON_MIN_SIDE);
        let icon_rect =
            egui::Rect::from_center_size(button_rect.center(), egui::vec2(icon_side, icon_side));
        let top_left = icon_rect.left_top();
        let top_right = icon_rect.right_top();
        let bottom_left = icon_rect.left_bottom();
        let bottom_right = icon_rect.right_bottom();
        painter.line_segment([top_left, top_right], stroke);
        painter.line_segment([top_right, bottom_right], stroke);
        painter.line_segment([bottom_right, bottom_left], stroke);
        painter.line_segment([bottom_left, top_left], stroke);
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

                    for (index, node) in report.content.iter().enumerate() {
                        Self::show_structured_report_node(ui, node, vec![index], 0);
                        ui.add_space(6.0);
                    }
                });
            });
    }

    fn show_structured_report_node(
        ui: &mut egui::Ui,
        node: &StructuredReportNode,
        path: Vec<usize>,
        depth: usize,
    ) {
        let header = match node.relationship_type.as_deref() {
            Some(relationship_type) => format!("{relationship_type}  {}", node.label),
            None => node.label.clone(),
        };

        if node.children.is_empty() {
            ui.push_id(&path, |ui| {
                ui.vertical_centered(|ui| {
                    ui.label(egui::RichText::new(header).strong());
                    if let Some(value) = node.value.as_deref() {
                        ui.add(egui::Label::new(value).wrap().halign(egui::Align::Center));
                    }
                });
            });
            return;
        }

        ui.push_id(&path, |ui| {
            egui::CollapsingHeader::new(header)
                .default_open(depth < 2)
                .show(ui, |ui| {
                    ui.vertical_centered(|ui| {
                        if let Some(value) = node.value.as_deref() {
                            ui.add(egui::Label::new(value).wrap().halign(egui::Align::Center));
                            ui.add_space(4.0);
                        }
                        for (index, child) in node.children.iter().enumerate() {
                            let mut child_path = path.clone();
                            child_path.push(index);
                            Self::show_structured_report_node(
                                ui,
                                child,
                                child_path,
                                depth.saturating_add(1),
                            );
                            ui.add_space(4.0);
                        }
                    });
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

    fn wl_overlay_layout(
        screen_width: f32,
        refresh_button_size: f32,
        item_spacing_x: f32,
        has_slider_rows: bool,
        has_action_rows: bool,
    ) -> WlOverlayLayout {
        let base_overlay_width = (screen_width * 0.5).clamp(340.0, 760.0);
        let slider_widget_width = (base_overlay_width * 0.18).clamp(80.0, 132.0);
        let slider_row_width =
            slider_widget_width + refresh_button_size + CONTROL_VALUE_WIDTH + 2.0 * item_spacing_x;
        let action_row_width = CONTROL_ACTION_BUTTON_WIDTH;

        let mut area_width = 0.0;
        if has_slider_rows {
            area_width = slider_row_width;
        }
        if has_action_rows {
            area_width = area_width.max(action_row_width);
        }

        WlOverlayLayout {
            slider_row_width,
            slider_widget_width,
            action_row_width,
            area_width,
        }
    }

    fn show_wl_overlay_row(
        ctx: &egui::Context,
        id: &'static str,
        width: f32,
        row_height: f32,
        bottom_offset_y: f32,
        enabled: bool,
        add_contents: impl FnOnce(&mut egui::Ui),
    ) {
        egui::Area::new(egui::Id::new(id))
            .movable(false)
            .interactable(enabled)
            .default_width(width)
            .default_height(row_height)
            .order(egui::Order::Foreground)
            .anchor(
                egui::Align2::RIGHT_BOTTOM,
                egui::vec2(-10.0, -bottom_offset_y),
            )
            .show(ctx, |ui| {
                ui.add_enabled_ui(enabled, |ui| add_contents(ui));
            });
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
        let show_overlay = self.overlay_visible;

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
                                                                self.overlay_visible,
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
                                            if show_overlay {
                                                Self::draw_gsps_overlay(
                                                    &painter,
                                                    image_rect,
                                                    &viewport.image,
                                                    viewport.current_frame,
                                                );
                                                Self::draw_sr_overlay(
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
        self.poll_full_metadata_load(ctx);
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
        let mut v_pressed = false;
        let mut escape_pressed = false;
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
            if self.can_toggle_full_metadata_popup() {
                v_pressed = input.consume_key(egui::Modifiers::NONE, egui::Key::V);
            }
            if self.full_metadata_popup_open {
                escape_pressed = input.consume_key(egui::Modifiers::NONE, egui::Key::Escape);
            }
        });
        if close_app_requested {
            ctx.send_viewport_cmd(ViewportCommand::Close);
            return;
        }
        if let Some(direction) = history_cycle_direction {
            self.cycle_history_entry(direction);
        }
        let history_transition_pending = self.pending_history_open_id.is_some();
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
        if g_pressed && !history_transition_pending && self.toggle_overlay() {
            self.refresh_active_textures(ctx);
        }
        if n_pressed && !history_transition_pending {
            self.jump_to_next_overlay(ctx);
        }
        if v_pressed {
            self.toggle_full_metadata_popup();
        }
        if escape_pressed {
            self.close_full_metadata_popup();
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
                            let line_stroke = Self::icon_stroke(ui, &menu_response.response);
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
                        egui::FontId::proportional(TITLE_TEXT_SIZE),
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
                            let close_response = ui.add_sized(
                                button_size,
                                egui::Button::new("")
                                    .fill(bar_fill)
                                    .stroke(egui::Stroke::NONE),
                            );
                            Self::paint_close_icon(
                                ui.painter(),
                                close_response.rect,
                                Self::icon_stroke(ui, &close_response),
                            );
                            if close_response.clicked() {
                                ctx.send_viewport_cmd(ViewportCommand::Close);
                            }
                            Self::register_icon_button_accessibility(&close_response, "Close");

                            let maximize_response = ui.add_sized(
                                button_size,
                                egui::Button::new("")
                                    .fill(bar_fill)
                                    .stroke(egui::Stroke::NONE),
                            );
                            Self::paint_titlebar_maximize_icon(
                                ui.painter(),
                                maximize_response.rect,
                                Self::icon_stroke(ui, &maximize_response),
                            );
                            if maximize_response.clicked() {
                                ctx.send_viewport_cmd(ViewportCommand::Maximized(!is_maximized));
                            }
                            Self::register_icon_button_accessibility(
                                &maximize_response,
                                "Maximize",
                            );

                            let minimize_response = ui.add_sized(
                                button_size,
                                egui::Button::new("")
                                    .fill(bar_fill)
                                    .stroke(egui::Stroke::NONE),
                            );
                            Self::paint_titlebar_minimize_icon(
                                ui.painter(),
                                minimize_response.rect,
                                Self::icon_stroke(ui, &minimize_response),
                            );
                            if minimize_response.clicked() {
                                ctx.send_viewport_cmd(ViewportCommand::Minimized(true));
                            }
                            Self::register_icon_button_accessibility(
                                &minimize_response,
                                "Minimize",
                            );
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
        let mut toggle_overlay_clicked = false;
        let mut next_overlay_clicked = false;
        let mut request_rebuild = false;
        let has_active_overlay = self.has_available_overlay();
        let has_overlay_navigation_target = self.next_overlay_navigation_target().is_some();

        if let Some(state) = active_state.as_mut() {
            let spacing = ctx.style().spacing.clone();
            let has_slider_rows = state.is_monochrome || state.frame_count > 1;
            let has_action_rows = state.frame_count > 1 || has_active_overlay;
            let wl_layout = Self::wl_overlay_layout(
                ctx.screen_rect().width(),
                spacing.interact_size.y,
                spacing.item_spacing.x,
                has_slider_rows,
                has_action_rows,
            );

            if wl_layout.area_width > 0.0 {
                let row_height = spacing.interact_size.y;
                let row_spacing_y = spacing.item_spacing.y + 4.0;
                let mut overlay_rows = Vec::new();
                if state.is_monochrome {
                    overlay_rows.push(WlOverlayRow::Center);
                    overlay_rows.push(WlOverlayRow::Width);
                }
                if state.frame_count > 1 {
                    overlay_rows.push(WlOverlayRow::Frame);
                    overlay_rows.push(WlOverlayRow::CineFps);
                    overlay_rows.push(WlOverlayRow::ToggleCine);
                }
                if has_active_overlay {
                    overlay_rows.push(WlOverlayRow::ToggleOverlay);
                    if has_overlay_navigation_target {
                        overlay_rows.push(WlOverlayRow::NextOverlay);
                    }
                }

                let mut bottom_offset_y = 10.0;
                for row in overlay_rows.into_iter().rev() {
                    let (row_id, row_width) = match row {
                        WlOverlayRow::Center => ("wl-overlay-center", wl_layout.slider_row_width),
                        WlOverlayRow::Width => ("wl-overlay-width", wl_layout.slider_row_width),
                        WlOverlayRow::Frame => ("wl-overlay-frame", wl_layout.slider_row_width),
                        WlOverlayRow::CineFps => {
                            ("wl-overlay-cine-fps", wl_layout.slider_row_width)
                        }
                        WlOverlayRow::ToggleCine => {
                            ("wl-overlay-toggle-cine", wl_layout.action_row_width)
                        }
                        WlOverlayRow::ToggleOverlay => {
                            ("wl-overlay-toggle-overlay", wl_layout.action_row_width)
                        }
                        WlOverlayRow::NextOverlay => {
                            ("wl-overlay-next-overlay", wl_layout.action_row_width)
                        }
                    };

                    Self::show_wl_overlay_row(
                        ctx,
                        row_id,
                        row_width,
                        row_height,
                        bottom_offset_y,
                        !history_transition_pending,
                        |ui| match row {
                            WlOverlayRow::Center => {
                                let center_range = (state.min_value as f32 - 2000.0)
                                    ..=(state.max_value as f32 + 2000.0);
                                let refresh_button_size = ui.spacing().interact_size.y;
                                ui.with_layout(
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
                                            .scope(|ui| {
                                                ui.spacing_mut().slider_width =
                                                    wl_layout.slider_widget_width;
                                                ui.add(
                                                    egui::Slider::new(
                                                        &mut state.window_center,
                                                        center_range.clone(),
                                                    )
                                                    .show_value(false)
                                                    .text("Center"),
                                                )
                                            })
                                            .inner
                                            .changed();
                                    },
                                );
                            }
                            WlOverlayRow::Width => {
                                let max_width = ((state.max_value - state.min_value).abs() as f32
                                    * 2.0)
                                    .max(1.0);
                                let width_range = 1.0..=max_width;
                                let refresh_button_size = ui.spacing().interact_size.y;
                                ui.with_layout(
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
                                            .scope(|ui| {
                                                ui.spacing_mut().slider_width =
                                                    wl_layout.slider_widget_width;
                                                ui.add(
                                                    egui::Slider::new(
                                                        &mut state.window_width,
                                                        width_range.clone(),
                                                    )
                                                    .show_value(false)
                                                    .text("Width"),
                                                )
                                            })
                                            .inner
                                            .changed();
                                    },
                                );
                            }
                            WlOverlayRow::Frame => {
                                let mut frame_index = state.current_frame as u32;
                                let max_frame = state.frame_count.saturating_sub(1) as u32;
                                let refresh_button_size = ui.spacing().interact_size.y;
                                ui.with_layout(
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
                                            .scope(|ui| {
                                                ui.spacing_mut().slider_width =
                                                    wl_layout.slider_widget_width;
                                                ui.add(
                                                    egui::Slider::new(
                                                        &mut frame_index,
                                                        0..=max_frame,
                                                    )
                                                    .show_value(false)
                                                    .text("Frame"),
                                                )
                                            })
                                            .inner
                                            .changed()
                                        {
                                            state.current_frame = frame_index as usize;
                                            self.last_cine_advance = Some(Instant::now());
                                            request_rebuild = true;
                                        }
                                    },
                                );
                            }
                            WlOverlayRow::CineFps => {
                                let refresh_button_size = ui.spacing().interact_size.y;
                                ui.with_layout(
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
                                            .scope(|ui| {
                                                ui.spacing_mut().slider_width =
                                                    wl_layout.slider_widget_width;
                                                ui.add(
                                                    egui::Slider::new(
                                                        &mut self.cine_fps,
                                                        1.0..=120.0,
                                                    )
                                                    .show_value(false)
                                                    .text("Cine FPS"),
                                                )
                                            })
                                            .inner
                                            .changed()
                                        {
                                            self.cine_fps = self.cine_fps.clamp(1.0, 120.0);
                                            self.last_cine_advance = Some(Instant::now());
                                        }
                                    },
                                );
                            }
                            WlOverlayRow::ToggleCine => {
                                ui.with_layout(
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
                            WlOverlayRow::ToggleOverlay => {
                                ui.with_layout(
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        if Self::add_action_control_button_no_border(
                                            ui,
                                            [
                                                CONTROL_ACTION_BUTTON_WIDTH,
                                                ui.spacing().interact_size.y,
                                            ],
                                            if self.overlay_visible {
                                                "Hide Overlay (G)"
                                            } else {
                                                "Show Overlay (G)"
                                            },
                                        )
                                        .clicked()
                                        {
                                            toggle_overlay_clicked = true;
                                        }
                                    },
                                );
                            }
                            WlOverlayRow::NextOverlay => {
                                ui.with_layout(
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        if Self::add_action_control_button_no_border(
                                            ui,
                                            [
                                                CONTROL_ACTION_BUTTON_WIDTH,
                                                ui.spacing().interact_size.y,
                                            ],
                                            "Next Overlay (N)",
                                        )
                                        .on_hover_text(
                                            "Jump to the next overlay and corresponding frame.",
                                        )
                                        .clicked()
                                        {
                                            next_overlay_clicked = true;
                                        }
                                    },
                                );
                            }
                        },
                    );
                    bottom_offset_y += row_height + row_spacing_y;
                }
            }
        }

        if toggle_cine_clicked {
            self.toggle_cine_mode();
        }
        if toggle_overlay_clicked && self.toggle_overlay() {
            self.refresh_active_textures(ctx);
        }
        if next_overlay_clicked {
            self.jump_to_next_overlay(ctx);
        }

        // Avoid applying stale W/L UI state while cycling history quickly with Tab.
        if request_rebuild && !next_overlay_clicked {
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
                    if self.overlay_visible {
                        if let Some(image) = self.image.as_ref() {
                            Self::draw_gsps_overlay(
                                &painter,
                                image_rect,
                                image,
                                self.current_frame,
                            );
                            Self::draw_sr_overlay(&painter, image_rect, image, self.current_frame);
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

        self.show_metadata_ui(ctx);

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
                                let dismiss_response = ui.add_sized(
                                    egui::vec2(
                                        ERROR_OVERLAY_CLOSE_BUTTON_SIZE,
                                        ERROR_OVERLAY_CLOSE_BUTTON_SIZE,
                                    ),
                                    egui::Button::new("")
                                        .fill(egui::Color32::TRANSPARENT)
                                        .stroke(egui::Stroke::NONE),
                                );
                                Self::paint_close_icon(
                                    ui.painter(),
                                    dismiss_response.rect,
                                    Self::icon_stroke(ui, &dismiss_response),
                                );
                                if dismiss_response.clicked() {
                                    dismiss_error = true;
                                }
                                Self::register_icon_button_accessibility(
                                    &dismiss_response,
                                    "Dismiss",
                                );
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

#[cfg(test)]
mod tests {
    use super::*;
    use dicom_core::value::DataSetSequence;
    use dicom_core::{DataElement, PrimitiveValue, Tag, VR};
    use dicom_object::{FileMetaTableBuilder, InMemDicomObject};

    use crate::dicom::{
        load_parametric_map_overlays, SrOverlay, SrOverlayGraphic, SrRenderingIntent,
        BASIC_TEXT_SR_SOP_CLASS_UID, DIGITAL_MAMMOGRAPHY_XRAY_IMAGE_PRESENTATION_SOP_CLASS_UID,
        EXPLICIT_VR_LITTLE_ENDIAN_UID, GSPS_SOP_CLASS_UID, PARAMETRIC_MAP_SOP_CLASS_UID,
    };

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

    fn assert_approx_eq(actual: f32, expected: f32) {
        assert!(
            (actual - expected).abs() < 0.001,
            "expected {expected}, got {actual}"
        );
    }

    fn unique_test_file_path(prefix: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after UNIX_EPOCH")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "perspecta-app-{prefix}-{}-{nanos}.dcm",
            std::process::id()
        ))
    }

    fn write_test_structured_report_file(prefix: &str) -> PathBuf {
        let sr_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, BASIC_TEXT_SR_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "SR"),
            DataElement::new(Tag(0x0008, 0x103E), VR::LO, "Test Structured Report"),
        ]);

        let sr_obj = sr_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(BASIC_TEXT_SR_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid("4.3.2.200"),
            )
            .expect("SR test object should build file meta");

        let path = unique_test_file_path(prefix);
        sr_obj
            .write_to_file(&path)
            .expect("SR test object should write to disk");
        path
    }

    fn test_source(path: &str) -> DicomSource {
        PathBuf::from(path).into()
    }

    fn test_meta(path: &str) -> DicomSourceMeta {
        test_source(path).into()
    }

    fn test_memory_source(
        preferred_name: &str,
        study_uid: &str,
        series_uid: &str,
        instance_uid: &str,
    ) -> DicomSource {
        let sr_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, BASIC_TEXT_SR_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, instance_uid),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "SR"),
            DataElement::new(Tag(0x0020, 0x000D), VR::UI, study_uid),
            DataElement::new(Tag(0x0020, 0x000E), VR::UI, series_uid),
        ]);

        let sr_obj = sr_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(BASIC_TEXT_SR_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid(instance_uid),
            )
            .expect("memory SR test object should build file meta");

        let path = unique_test_file_path("memory-source");
        sr_obj
            .write_to_file(&path)
            .expect("memory SR test object should write to disk");
        let bytes = fs::read(&path).expect("memory SR test bytes should read from disk");
        let _ = fs::remove_file(&path);
        DicomSource::from_memory(preferred_name, bytes)
    }

    fn test_memory_image_source(
        preferred_name: &str,
        study_uid: &str,
        series_uid: &str,
        instance_uid: &str,
    ) -> DicomSource {
        let image_sop_class_uid = DIGITAL_MAMMOGRAPHY_XRAY_IMAGE_PRESENTATION_SOP_CLASS_UID;
        let image_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, image_sop_class_uid),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, instance_uid),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "MG"),
            DataElement::new(Tag(0x0020, 0x000D), VR::UI, study_uid),
            DataElement::new(Tag(0x0020, 0x000E), VR::UI, series_uid),
            DataElement::new(Tag(0x0028, 0x0002), VR::US, PrimitiveValue::from(1u16)),
            DataElement::new(Tag(0x0028, 0x0004), VR::CS, "MONOCHROME2"),
            DataElement::new(Tag(0x0028, 0x0010), VR::US, PrimitiveValue::from(1u16)),
            DataElement::new(Tag(0x0028, 0x0011), VR::US, PrimitiveValue::from(1u16)),
            DataElement::new(Tag(0x0028, 0x0100), VR::US, PrimitiveValue::from(8u16)),
            DataElement::new(Tag(0x0028, 0x0101), VR::US, PrimitiveValue::from(8u16)),
            DataElement::new(Tag(0x0028, 0x0102), VR::US, PrimitiveValue::from(7u16)),
            DataElement::new(Tag(0x0028, 0x0103), VR::US, PrimitiveValue::from(0u16)),
            DataElement::new(Tag(0x7FE0, 0x0010), VR::OB, PrimitiveValue::from(vec![0u8])),
        ]);

        let image_obj = image_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(image_sop_class_uid)
                    .media_storage_sop_instance_uid(instance_uid),
            )
            .expect("memory image test object should build file meta");

        let path = unique_test_file_path("memory-image-source");
        image_obj
            .write_to_file(&path)
            .expect("memory image test object should write to disk");
        let bytes = fs::read(&path).expect("memory image test bytes should read from disk");
        let _ = fs::remove_file(&path);
        DicomSource::from_memory(preferred_name, bytes)
    }

    fn test_memory_gsps_source(
        preferred_name: &str,
        study_uid: &str,
        series_uid: &str,
        instance_uid: &str,
        referenced_instance_uid: &str,
    ) -> DicomSource {
        let referenced_image = InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0008, 0x1155),
            VR::UI,
            referenced_instance_uid,
        )]);
        let graphic = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0070, 0x0005), VR::CS, "PIXEL"),
            DataElement::new(
                Tag(0x0070, 0x0022),
                VR::FL,
                PrimitiveValue::F32(vec![1.0, 1.0].into()),
            ),
            DataElement::new(Tag(0x0070, 0x0023), VR::CS, "POINT"),
        ]);
        let annotation = InMemDicomObject::from_element_iter([
            DataElement::new(
                Tag(0x0008, 0x1140),
                VR::SQ,
                DataSetSequence::from(vec![referenced_image]),
            ),
            DataElement::new(
                Tag(0x0070, 0x0009),
                VR::SQ,
                DataSetSequence::from(vec![graphic]),
            ),
        ]);
        let gsps_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, GSPS_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, instance_uid),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "PR"),
            DataElement::new(Tag(0x0020, 0x000D), VR::UI, study_uid),
            DataElement::new(Tag(0x0020, 0x000E), VR::UI, series_uid),
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
                    .media_storage_sop_instance_uid(instance_uid),
            )
            .expect("memory GSPS test object should build file meta");

        let path = unique_test_file_path("memory-gsps-source");
        gsps_obj
            .write_to_file(&path)
            .expect("memory GSPS test object should write to disk");
        let bytes = fs::read(&path).expect("memory GSPS test bytes should read from disk");
        let _ = fs::remove_file(&path);
        DicomSource::from_memory(preferred_name, bytes)
    }

    fn test_memory_parametric_map_source(
        preferred_name: &str,
        study_uid: &str,
        series_uid: &str,
        instance_uid: &str,
        referenced_instance_uid: &str,
    ) -> DicomSource {
        let referenced_image = InMemDicomObject::from_element_iter([
            DataElement::new(
                Tag(0x0008, 0x1150),
                VR::UI,
                DIGITAL_MAMMOGRAPHY_XRAY_IMAGE_PRESENTATION_SOP_CLASS_UID,
            ),
            DataElement::new(Tag(0x0008, 0x1155), VR::UI, referenced_instance_uid),
        ]);
        let pm_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, PARAMETRIC_MAP_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, instance_uid),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "MG"),
            DataElement::new(Tag(0x0020, 0x000D), VR::UI, study_uid),
            DataElement::new(Tag(0x0020, 0x000E), VR::UI, series_uid),
            DataElement::new(Tag(0x0028, 0x0002), VR::US, PrimitiveValue::from(1u16)),
            DataElement::new(Tag(0x0028, 0x0004), VR::CS, "MONOCHROME2"),
            DataElement::new(Tag(0x0028, 0x0010), VR::US, PrimitiveValue::from(1u16)),
            DataElement::new(Tag(0x0028, 0x0011), VR::US, PrimitiveValue::from(1u16)),
            DataElement::new(Tag(0x0028, 0x0008), VR::IS, "1"),
            DataElement::new(
                Tag(0x0008, 0x2112),
                VR::SQ,
                DataSetSequence::from(vec![referenced_image]),
            ),
            DataElement::new(
                Tag(0x7FE0, 0x0008),
                VR::OF,
                PrimitiveValue::F32(vec![1.0f32].into()),
            ),
        ]);

        let pm_obj = pm_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(PARAMETRIC_MAP_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid(instance_uid),
            )
            .expect("memory Parametric Map test object should build file meta");

        let path = unique_test_file_path("memory-parametric-map-source");
        pm_obj
            .write_to_file(&path)
            .expect("memory Parametric Map test object should write to disk");
        let bytes = fs::read(&path).expect("memory Parametric Map test bytes should read");
        let _ = fs::remove_file(&path);
        DicomSource::from_memory(preferred_name, bytes)
    }

    fn single_history_entry(ctx: &egui::Context, path: &str, texture_name: &str) -> HistoryEntry {
        let path_buf = PathBuf::from(path);
        HistoryEntry {
            id: history_id_from_paths(std::slice::from_ref(&path_buf)),
            kind: HistoryKind::Single(Box::new(HistorySingleData {
                path: path_buf.clone().into(),
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
                path: path_buf.into(),
                report: StructuredReportDocument::test_stub(),
            })),
            thumbs: vec![HistoryThumb {
                texture: test_texture(ctx, texture_name),
            }],
        }
    }

    #[test]
    fn memory_sources_use_semantic_identity_for_history_and_display_matching() {
        let reopened = test_memory_source(
            "reopened-report",
            "1.2.840.10008.1",
            "1.2.840.10008.1.1",
            "1.2.840.10008.1.1.1",
        );
        let reopened_again = test_memory_source(
            "same-report-different-handle",
            "1.2.840.10008.1",
            "1.2.840.10008.1.1",
            "1.2.840.10008.1.1.1",
        );
        let different = test_memory_source(
            "different-report",
            "1.2.840.10008.1",
            "1.2.840.10008.1.1",
            "1.2.840.10008.1.1.2",
        );

        let reopened_id = history_id_from_paths(std::slice::from_ref(&reopened));
        let reopened_again_id = history_id_from_paths(std::slice::from_ref(&reopened_again));
        let different_id = history_id_from_paths(std::slice::from_ref(&different));

        assert_eq!(reopened_id, reopened_again_id);
        assert_ne!(reopened_id, different_id);

        let app = DicomViewerApp {
            current_single_path: Some((&reopened).into()),
            ..Default::default()
        };
        assert!(app.displayed_study_matches_paths(&[reopened_again]));
        assert!(!app.displayed_study_matches_paths(&[different]));
    }

    #[test]
    fn history_id_from_paths_uses_collision_free_length_prefix_encoding() {
        let left = vec![PathBuf::from("a|b"), PathBuf::from("c")];
        let right = vec![PathBuf::from("a"), PathBuf::from("b|c")];

        assert_ne!(history_id_from_paths(&left), history_id_from_paths(&right));
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
    fn wl_overlay_layout_uses_minimum_width_clamp_for_small_screens() {
        let layout = DicomViewerApp::wl_overlay_layout(320.0, 20.0, 8.0, true, false);

        assert_approx_eq(layout.slider_row_width, 180.0);
        assert_approx_eq(layout.slider_widget_width, 80.0);
        assert_approx_eq(layout.area_width, layout.slider_row_width);
    }

    #[test]
    fn wl_overlay_layout_uses_maximum_width_clamp_for_large_screens() {
        let layout = DicomViewerApp::wl_overlay_layout(2000.0, 20.0, 8.0, true, false);

        assert_approx_eq(layout.slider_row_width, 232.0);
        assert_approx_eq(layout.slider_widget_width, 132.0);
        assert_approx_eq(layout.area_width, layout.slider_row_width);
    }

    #[test]
    fn wl_overlay_layout_keeps_slider_widget_floor() {
        let layout = DicomViewerApp::wl_overlay_layout(320.0, 80.0, 40.0, true, false);

        assert_approx_eq(layout.slider_widget_width, 80.0);
        assert_approx_eq(layout.slider_row_width, 304.0);
        assert_approx_eq(layout.area_width, layout.slider_row_width);
    }

    #[test]
    fn wl_overlay_layout_shrinks_to_action_width_when_no_slider_rows_exist() {
        let layout = DicomViewerApp::wl_overlay_layout(1400.0, 20.0, 8.0, false, true);

        assert_approx_eq(layout.action_row_width, CONTROL_ACTION_BUTTON_WIDTH);
        assert_approx_eq(layout.area_width, CONTROL_ACTION_BUTTON_WIDTH);
    }

    #[test]
    fn wl_overlay_layout_disables_overlay_when_no_rows_are_visible() {
        let layout = DicomViewerApp::wl_overlay_layout(1400.0, 20.0, 8.0, false, false);

        assert_approx_eq(layout.area_width, 0.0);
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
            .push_back(test_source("streamed.dcm"));
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
            .push_back(test_source("streamed-8-up.dcm"));
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
            .push_back(test_source("pending-stream.dcm"));
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

        DicomViewerApp::merge_gsps_overlays(&mut destination, &source);
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
    fn authoritative_pending_gsps_snapshot_replaces_and_locks_streamed_keys() {
        let mut app = DicomViewerApp::default();
        app.merge_pending_gsps_overlays(HashMap::from([(
            "1.2.3".to_string(),
            GspsOverlay::from_graphics(vec![GspsGraphic::Point {
                x: 1.0,
                y: 2.0,
                units: GspsUnits::Pixel,
            }]),
        )]));

        app.set_authoritative_pending_gsps_overlays(HashMap::from([(
            "1.2.3".to_string(),
            GspsOverlay::from_graphics(vec![GspsGraphic::Polyline {
                points: vec![(0.0, 0.0), (1.0, 1.0)],
                units: GspsUnits::Display,
                closed: false,
            }]),
        )]));

        let overlay = app
            .pending_gsps_overlays
            .get("1.2.3")
            .expect("authoritative snapshot should replace the streamed entry");
        assert_eq!(overlay.graphics.len(), 1);
        assert!(matches!(
            overlay.graphics[0].graphic,
            GspsGraphic::Polyline { .. }
        ));

        app.merge_pending_gsps_overlays(HashMap::from([(
            "1.2.3".to_string(),
            GspsOverlay::from_graphics(vec![GspsGraphic::Point {
                x: 9.0,
                y: 9.0,
                units: GspsUnits::Pixel,
            }]),
        )]));

        let overlay = app
            .pending_gsps_overlays
            .get("1.2.3")
            .expect("authoritative snapshot should keep the same overlay");
        assert_eq!(overlay.graphics.len(), 1);
        assert!(matches!(
            overlay.graphics[0].graphic,
            GspsGraphic::Polyline { .. }
        ));
    }

    #[test]
    fn authoritative_pending_gsps_snapshot_drops_empty_entries_before_locking() {
        let mut app = DicomViewerApp::default();

        app.set_authoritative_pending_gsps_overlays(HashMap::from([(
            "1.2.3".to_string(),
            GspsOverlay::default(),
        )]));

        assert!(app.pending_gsps_overlays.is_empty());
        assert!(app.authoritative_gsps_overlay_keys.is_empty());

        app.merge_pending_gsps_overlays(HashMap::from([(
            "1.2.3".to_string(),
            GspsOverlay::from_graphics(vec![GspsGraphic::Point {
                x: 5.0,
                y: 6.0,
                units: GspsUnits::Pixel,
            }]),
        )]));

        assert_eq!(
            app.pending_gsps_overlays
                .get("1.2.3")
                .map(|overlay| overlay.graphics.len()),
            Some(1)
        );
    }

    #[test]
    fn authoritative_pending_gsps_snapshot_detaches_removed_current_overlay() {
        let ctx = egui::Context::default();
        let stale_overlay = GspsOverlay::from_graphics(vec![GspsGraphic::Point {
            x: 1.0,
            y: 2.0,
            units: GspsUnits::Pixel,
        }]);
        let replacement_overlay = GspsOverlay::from_graphics(vec![GspsGraphic::Polyline {
            points: vec![(0.0, 0.0), (1.0, 1.0)],
            units: GspsUnits::Display,
            closed: false,
        }]);
        let path = test_meta("current-single.dcm");
        let mut live_image = DicomImage::test_stub(Some(stale_overlay.clone()));
        live_image.sop_instance_uid = Some("1.2.3".to_string());

        let mut app = DicomViewerApp {
            image: Some(live_image),
            current_single_path: Some(path.clone()),
            texture: Some(test_texture(&ctx, "authoritative-gsps-detach")),
            pending_gsps_overlays: HashMap::from([("1.2.3".to_string(), stale_overlay)]),
            history_entries: vec![HistoryEntry {
                id: history_id_from_paths(std::slice::from_ref(&path)),
                kind: HistoryKind::Single(Box::new(HistorySingleData {
                    path,
                    image: DicomImage::test_stub(None),
                    texture: test_texture(&ctx, "authoritative-gsps-detach-history"),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    cine_fps: DEFAULT_CINE_FPS,
                })),
                thumbs: Vec::new(),
            }],
            ..Default::default()
        };

        app.set_authoritative_pending_gsps_overlays(HashMap::from([(
            "9.9.9".to_string(),
            replacement_overlay,
        )]));

        assert!(
            app.image
                .as_ref()
                .and_then(|image| image.gsps_overlay.as_ref())
                .is_none(),
            "current study should detach overlays that are no longer authoritative"
        );
        assert!(!app.pending_gsps_overlays.contains_key("1.2.3"));
        assert!(app.pending_gsps_overlays.contains_key("9.9.9"));
        assert!(!app.authoritative_gsps_overlay_keys.contains("1.2.3"));
        assert!(app.authoritative_gsps_overlay_keys.contains("9.9.9"));

        let HistoryKind::Single(single) = &app.history_entries[0].kind else {
            panic!("expected single history entry");
        };
        assert!(
            single.image.gsps_overlay.is_none(),
            "history cache should persist removal of stale authoritative overlays"
        );
    }

    #[test]
    fn authoritative_pending_gsps_snapshot_detaches_removed_cached_history_overlays() {
        let ctx = egui::Context::default();
        let stale_overlay = GspsOverlay::from_graphics(vec![GspsGraphic::Point {
            x: 3.0,
            y: 4.0,
            units: GspsUnits::Pixel,
        }]);
        let unaffected_overlay = GspsOverlay::from_graphics(vec![GspsGraphic::Polyline {
            points: vec![(0.0, 0.0), (1.0, 1.0)],
            units: GspsUnits::Display,
            closed: false,
        }]);

        let mut single_image = DicomImage::test_stub(None);
        single_image.sop_instance_uid = Some("1.2.3".to_string());
        single_image.gsps_overlay = Some(stale_overlay.clone());

        let mut grouped_stale_image = DicomImage::test_stub(None);
        grouped_stale_image.sop_instance_uid = Some("1.2.3".to_string());
        grouped_stale_image.gsps_overlay = Some(stale_overlay.clone());

        let mut grouped_unaffected_image = DicomImage::test_stub(None);
        grouped_unaffected_image.sop_instance_uid = Some("9.9.9".to_string());
        grouped_unaffected_image.gsps_overlay = Some(unaffected_overlay.clone());

        let mut app = DicomViewerApp {
            pending_gsps_overlays: HashMap::from([("1.2.3".to_string(), stale_overlay)]),
            authoritative_gsps_overlay_keys: HashSet::from(["1.2.3".to_string()]),
            history_entries: vec![
                HistoryEntry {
                    id: history_id_from_paths(&[test_meta("cached-single-gsps.dcm")]),
                    kind: HistoryKind::Single(Box::new(HistorySingleData {
                        path: test_meta("cached-single-gsps.dcm"),
                        image: single_image,
                        texture: test_texture(&ctx, "authoritative-gsps-detach-history-single"),
                        window_center: 0.0,
                        window_width: 1.0,
                        current_frame: 0,
                        cine_fps: DEFAULT_CINE_FPS,
                    })),
                    thumbs: Vec::new(),
                },
                HistoryEntry {
                    id: history_id_from_paths(&[
                        test_meta("cached-group-gsps-a.dcm"),
                        test_meta("cached-group-gsps-b.dcm"),
                    ]),
                    kind: HistoryKind::Group(HistoryGroupData {
                        viewports: vec![
                            HistoryGroupViewportData {
                                path: test_meta("cached-group-gsps-a.dcm"),
                                image: grouped_stale_image,
                                texture: test_texture(
                                    &ctx,
                                    "authoritative-gsps-detach-history-group-a",
                                ),
                                label: "A".to_string(),
                                window_center: 0.0,
                                window_width: 1.0,
                                current_frame: 0,
                            },
                            HistoryGroupViewportData {
                                path: test_meta("cached-group-gsps-b.dcm"),
                                image: grouped_unaffected_image,
                                texture: test_texture(
                                    &ctx,
                                    "authoritative-gsps-detach-history-group-b",
                                ),
                                label: "B".to_string(),
                                window_center: 0.0,
                                window_width: 1.0,
                                current_frame: 0,
                            },
                        ],
                        selected_index: 0,
                    }),
                    thumbs: Vec::new(),
                },
            ],
            ..Default::default()
        };

        app.set_authoritative_pending_gsps_overlays(HashMap::new());

        let HistoryKind::Single(single) = &app.history_entries[0].kind else {
            panic!("expected single history entry");
        };
        assert!(
            single.image.gsps_overlay.is_none(),
            "single history cache should clear removed authoritative GSPS overlays"
        );

        let HistoryKind::Group(group) = &app.history_entries[1].kind else {
            panic!("expected group history entry");
        };
        assert!(
            group.viewports[0].image.gsps_overlay.is_none(),
            "group history cache should clear removed authoritative GSPS overlays"
        );
        assert!(
            group.viewports[1].image.gsps_overlay.is_some(),
            "group history cache should keep unrelated GSPS overlays"
        );
    }

    #[test]
    fn authoritative_pending_sr_snapshot_attaches_to_live_image_and_history() {
        let ctx = egui::Context::default();
        let path = test_meta("current-single-sr.dcm");
        let mut live_image = DicomImage::test_stub(None);
        live_image.sop_instance_uid = Some("1.2.3".to_string());
        let sr_overlay = SrOverlay {
            graphics: vec![SrOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 4.0,
                    y: 5.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: None,
                rendering_intent: SrRenderingIntent::PresentationRequired,
                cad_operating_point: Some(1.0),
            }],
        };

        let mut app = DicomViewerApp {
            image: Some(live_image),
            current_single_path: Some(path.clone()),
            texture: Some(test_texture(&ctx, "authoritative-sr-attach")),
            history_entries: vec![HistoryEntry {
                id: history_id_from_paths(std::slice::from_ref(&path)),
                kind: HistoryKind::Single(Box::new(HistorySingleData {
                    path,
                    image: DicomImage::test_stub(None),
                    texture: test_texture(&ctx, "authoritative-sr-attach-history"),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    cine_fps: DEFAULT_CINE_FPS,
                })),
                thumbs: Vec::new(),
            }],
            ..Default::default()
        };

        app.set_authoritative_pending_sr_overlays(HashMap::from([(
            "1.2.3".to_string(),
            sr_overlay,
        )]));

        assert!(
            app.image
                .as_ref()
                .and_then(|image| image.sr_overlay.as_ref())
                .is_some(),
            "current study should receive authoritative SR overlays"
        );
        let HistoryKind::Single(single) = &app.history_entries[0].kind else {
            panic!("expected single history entry");
        };
        assert!(
            single.image.sr_overlay.is_some(),
            "history cache should persist authoritative SR overlays"
        );
    }

    #[test]
    fn authoritative_pending_sr_snapshot_detaches_removed_cached_history_overlays() {
        let ctx = egui::Context::default();
        let stale_overlay = SrOverlay {
            graphics: vec![SrOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 7.0,
                    y: 8.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: None,
                rendering_intent: SrRenderingIntent::PresentationRequired,
                cad_operating_point: Some(1.0),
            }],
        };
        let unaffected_overlay = SrOverlay {
            graphics: vec![SrOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 9.0,
                    y: 10.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: None,
                rendering_intent: SrRenderingIntent::PresentationRequired,
                cad_operating_point: Some(2.0),
            }],
        };

        let mut single_image = DicomImage::test_stub(None);
        single_image.sop_instance_uid = Some("1.2.3".to_string());
        single_image.sr_overlay = Some(stale_overlay.clone());

        let mut grouped_stale_image = DicomImage::test_stub(None);
        grouped_stale_image.sop_instance_uid = Some("1.2.3".to_string());
        grouped_stale_image.sr_overlay = Some(stale_overlay.clone());

        let mut grouped_unaffected_image = DicomImage::test_stub(None);
        grouped_unaffected_image.sop_instance_uid = Some("9.9.9".to_string());
        grouped_unaffected_image.sr_overlay = Some(unaffected_overlay.clone());

        let mut app = DicomViewerApp {
            pending_sr_overlays: HashMap::from([("1.2.3".to_string(), stale_overlay)]),
            authoritative_sr_overlay_keys: HashSet::from(["1.2.3".to_string()]),
            history_entries: vec![
                HistoryEntry {
                    id: history_id_from_paths(&[test_meta("cached-single-sr.dcm")]),
                    kind: HistoryKind::Single(Box::new(HistorySingleData {
                        path: test_meta("cached-single-sr.dcm"),
                        image: single_image,
                        texture: test_texture(&ctx, "authoritative-sr-detach-history-single"),
                        window_center: 0.0,
                        window_width: 1.0,
                        current_frame: 0,
                        cine_fps: DEFAULT_CINE_FPS,
                    })),
                    thumbs: Vec::new(),
                },
                HistoryEntry {
                    id: history_id_from_paths(&[
                        test_meta("cached-group-a.dcm"),
                        test_meta("cached-group-b.dcm"),
                    ]),
                    kind: HistoryKind::Group(HistoryGroupData {
                        viewports: vec![
                            HistoryGroupViewportData {
                                path: test_meta("cached-group-a.dcm"),
                                image: grouped_stale_image,
                                texture: test_texture(
                                    &ctx,
                                    "authoritative-sr-detach-history-group-a",
                                ),
                                label: "A".to_string(),
                                window_center: 0.0,
                                window_width: 1.0,
                                current_frame: 0,
                            },
                            HistoryGroupViewportData {
                                path: test_meta("cached-group-b.dcm"),
                                image: grouped_unaffected_image,
                                texture: test_texture(
                                    &ctx,
                                    "authoritative-sr-detach-history-group-b",
                                ),
                                label: "B".to_string(),
                                window_center: 0.0,
                                window_width: 1.0,
                                current_frame: 0,
                            },
                        ],
                        selected_index: 0,
                    }),
                    thumbs: Vec::new(),
                },
            ],
            ..Default::default()
        };

        app.set_authoritative_pending_sr_overlays(HashMap::new());

        let HistoryKind::Single(single) = &app.history_entries[0].kind else {
            panic!("expected single history entry");
        };
        assert!(
            single.image.sr_overlay.is_none(),
            "single history cache should clear removed authoritative SR overlays"
        );

        let HistoryKind::Group(group) = &app.history_entries[1].kind else {
            panic!("expected group history entry");
        };
        assert!(
            group.viewports[0].image.sr_overlay.is_none(),
            "group history cache should clear removed authoritative SR overlays"
        );
        assert!(
            group.viewports[1].image.sr_overlay.is_some(),
            "group history cache should keep unrelated SR overlays"
        );
    }

    #[test]
    fn toggle_overlay_without_active_overlay_resets_to_off() {
        let mut app = DicomViewerApp {
            overlay_visible: true,
            ..Default::default()
        };
        app.toggle_overlay();
        assert!(!app.overlay_visible);
    }

    #[test]
    fn has_available_overlay_counts_required_sr_overlay() {
        let mut image = DicomImage::test_stub_with_mono_frames(None, 1);
        image.sr_overlay = Some(SrOverlay {
            graphics: vec![SrOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 2.0,
                    y: 3.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: None,
                rendering_intent: SrRenderingIntent::PresentationRequired,
                cad_operating_point: Some(2.0),
            }],
        });
        let app = DicomViewerApp {
            image: Some(image),
            ..Default::default()
        };

        assert!(app.has_available_overlay());
    }

    #[test]
    fn has_available_overlay_ignores_optional_sr_overlay() {
        let mut image = DicomImage::test_stub(None);
        image.sr_overlay = Some(SrOverlay {
            graphics: vec![SrOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 2.0,
                    y: 3.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: None,
                rendering_intent: SrRenderingIntent::PresentationOptional,
                cad_operating_point: Some(2.0),
            }],
        });
        let app = DicomViewerApp {
            image: Some(image),
            ..Default::default()
        };

        assert!(!app.has_available_overlay());
    }

    #[test]
    fn has_available_overlay_ignores_non_renderable_single_frame_overlay() {
        let overlay = GspsOverlay {
            graphics: vec![crate::dicom::GspsOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 2.0,
                    y: 3.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: Some(vec![9]),
            }],
        };
        let app = DicomViewerApp {
            image: Some(DicomImage::test_stub_with_mono_frames(Some(overlay), 4)),
            ..Default::default()
        };

        assert!(!app.has_available_overlay());
    }

    #[test]
    fn has_available_overlay_ignores_group_overlay_outside_common_frame_count() {
        let ctx = egui::Context::default();
        let texture_image = ColorImage {
            size: [1, 1],
            pixels: vec![egui::Color32::BLACK],
        };
        let overlay = GspsOverlay {
            graphics: vec![crate::dicom::GspsOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 4.0,
                    y: 5.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: Some(vec![4]),
            }],
        };
        let texture_a = ctx.load_texture(
            "test-non-renderable-group-a",
            texture_image.clone(),
            TextureOptions::LINEAR,
        );
        let texture_b = ctx.load_texture(
            "test-non-renderable-group-b",
            texture_image,
            TextureOptions::LINEAR,
        );
        let app = DicomViewerApp {
            mammo_group: vec![
                Some(MammoViewport {
                    path: test_meta("non-renderable-a.dcm"),
                    image: DicomImage::test_stub_with_mono_frames(Some(overlay), 4),
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
                    path: test_meta("non-renderable-b.dcm"),
                    image: DicomImage::test_stub_with_mono_frames(None, 3),
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
            ..Default::default()
        };

        assert!(!app.has_available_overlay());
    }

    #[test]
    fn toggle_overlay_allows_group_overlay_when_other_viewport_is_selected() {
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
                    path: test_meta("a.dcm"),
                    image: DicomImage::test_stub_with_mono_frames(None, 1),
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
                    path: test_meta("b.dcm"),
                    image: DicomImage::test_stub_with_mono_frames(Some(overlay), 1),
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

        app.toggle_overlay();
        assert!(app.overlay_visible);
    }

    #[test]
    fn jump_to_next_overlay_cycles_single_view_frames() {
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

        app.jump_to_next_overlay(&ctx);
        assert!(app.overlay_visible);
        assert_eq!(app.current_frame, 1);

        app.jump_to_next_overlay(&ctx);
        assert_eq!(app.current_frame, 3);

        app.jump_to_next_overlay(&ctx);
        assert_eq!(app.current_frame, 1);
    }

    #[test]
    fn jump_to_next_overlay_advances_when_current_target_is_hidden() {
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

        app.jump_to_next_overlay(&ctx);

        assert!(app.overlay_visible);
        assert_eq!(app.current_frame, 3);
    }

    #[test]
    fn overlay_target_frames_follow_reversed_display_order() {
        let overlay = GspsOverlay {
            graphics: vec![crate::dicom::GspsOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 1.0,
                    y: 1.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: Some(vec![1, 4]),
            }],
        };
        let image = DicomImage::test_stub_with_mono_frames_and_reverse(Some(overlay), 4, true);

        assert_eq!(DicomViewerApp::overlay_target_frames(&image, 4), vec![0, 3]);
    }

    #[test]
    fn overlay_target_frames_include_required_sr_overlay_and_follow_reverse_order() {
        let mut image = DicomImage::test_stub_with_mono_frames_and_reverse(None, 4, true);
        image.sr_overlay = Some(SrOverlay {
            graphics: vec![SrOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 1.0,
                    y: 1.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: Some(vec![1, 4]),
                rendering_intent: SrRenderingIntent::PresentationRequired,
                cad_operating_point: Some(1.0),
            }],
        });

        assert_eq!(DicomViewerApp::overlay_target_frames(&image, 4), vec![0, 3]);
    }

    #[test]
    fn reversed_display_frame_maps_back_to_stored_gsps_frame() {
        let overlay = GspsOverlay {
            graphics: vec![crate::dicom::GspsOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 1.0,
                    y: 1.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: Some(vec![1]),
            }],
        };
        let image = DicomImage::test_stub_with_mono_frames_and_reverse(Some(overlay), 4, true);
        let stored_frame_index = image
            .display_frame_index_to_stored(3)
            .expect("display frame should map to stored frame");

        let overlay = image
            .gsps_overlay
            .as_ref()
            .expect("test image should keep GSPS overlay");

        assert_eq!(stored_frame_index, 0);
        assert_eq!(overlay.graphics_for_frame(stored_frame_index).count(), 1);
        assert_eq!(overlay.graphics_for_frame(3).count(), 0);
    }

    #[test]
    fn jump_to_next_overlay_cycles_group_viewports_and_frames() {
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
                    path: test_meta("a.dcm"),
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
                    path: test_meta("b.dcm"),
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

        app.jump_to_next_overlay(&ctx);
        assert!(app.overlay_visible);
        assert_eq!(app.mammo_selected_index, 0);
        assert_eq!(app.selected_mammo_frame_index(), 1);

        app.jump_to_next_overlay(&ctx);
        assert_eq!(app.mammo_selected_index, 1);
        assert_eq!(app.selected_mammo_frame_index(), 0);

        app.jump_to_next_overlay(&ctx);
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
                    path: test_meta("cached-single.dcm"),
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
    fn open_history_entry_single_hides_streaming_group_placeholders() {
        let ctx = egui::Context::default();
        let (_tx, rx) = mpsc::channel::<DicomWebGroupStreamUpdate>();
        let (_single_tx, single_rx) = mpsc::channel::<Result<PendingSingleLoad, String>>();
        let (_mammo_tx, mammo_rx) = mpsc::channel::<Result<PendingLoad, String>>();
        let mut app = DicomViewerApp {
            dicomweb_active_group_expected: Some(2),
            dicomweb_active_path_receiver: Some(rx),
            single_load_receiver: Some(single_rx),
            mammo_load_receiver: Some(mammo_rx),
            history_entries: vec![single_history_entry(
                &ctx,
                "cached-single.dcm",
                "history-single-hides-stream",
            )],
            ..Default::default()
        };

        app.open_history_entry(0, &ctx);

        assert_eq!(
            app.current_single_path,
            Some(test_meta("cached-single.dcm"))
        );
        assert!(!app.has_mammo_group());
        assert!(app.single_load_receiver.is_none());
        assert!(app.mammo_load_receiver.is_none());
        assert!(app.mammo_load_sender.is_none());
    }

    #[test]
    fn sync_current_state_to_history_persists_single_view_gsps_backfill() {
        let ctx = egui::Context::default();
        let overlay = GspsOverlay::from_graphics(vec![GspsGraphic::Point {
            x: 1.0,
            y: 1.0,
            units: GspsUnits::Pixel,
        }]);
        let texture = test_texture(&ctx, "single-history-gsps-backfill");
        let path = test_meta("cached-single.dcm");
        let mut live_image = DicomImage::test_stub(Some(overlay));
        live_image.sop_instance_uid = Some("9.999.200.1".to_string());

        let mut app = DicomViewerApp {
            image: Some(live_image),
            current_single_path: Some(path.clone()),
            texture: Some(texture.clone()),
            history_entries: vec![HistoryEntry {
                id: history_id_from_paths(std::slice::from_ref(&path)),
                kind: HistoryKind::Single(Box::new(HistorySingleData {
                    path: path.clone(),
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

        app.sync_current_state_to_history();

        let HistoryKind::Single(single) = &app.history_entries[0].kind else {
            panic!("expected single history entry");
        };
        assert!(
            single.image.gsps_overlay.is_some(),
            "single-view history entry should keep GSPS backfills from the live image"
        );
    }

    #[test]
    fn sync_current_state_to_history_persists_group_view_gsps_removal() {
        let ctx = egui::Context::default();
        let stale_overlay = GspsOverlay::from_graphics(vec![GspsGraphic::Point {
            x: 2.0,
            y: 2.0,
            units: GspsUnits::Pixel,
        }]);
        let texture_image = ColorImage {
            size: [1, 1],
            pixels: vec![egui::Color32::BLACK],
        };
        let texture_a = ctx.load_texture(
            "group-history-gsps-removal-a",
            texture_image.clone(),
            TextureOptions::LINEAR,
        );
        let texture_b = ctx.load_texture(
            "group-history-gsps-removal-b",
            texture_image,
            TextureOptions::LINEAR,
        );
        let path_a = test_meta("group-a.dcm");
        let path_b = test_meta("group-b.dcm");

        let mut app = DicomViewerApp {
            mammo_group: vec![
                Some(MammoViewport {
                    path: path_a.clone(),
                    image: DicomImage::test_stub(None),
                    texture: texture_a.clone(),
                    label: "A".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
                Some(MammoViewport {
                    path: path_b.clone(),
                    image: DicomImage::test_stub(None),
                    texture: texture_b.clone(),
                    label: "B".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
            ],
            history_entries: vec![HistoryEntry {
                id: history_id_from_paths(&[path_a.clone(), path_b.clone()]),
                kind: HistoryKind::Group(HistoryGroupData {
                    viewports: vec![
                        HistoryGroupViewportData {
                            path: path_a,
                            image: DicomImage::test_stub(None),
                            texture: texture_a,
                            label: "A".to_string(),
                            window_center: 0.0,
                            window_width: 1.0,
                            current_frame: 0,
                        },
                        HistoryGroupViewportData {
                            path: path_b.clone(),
                            image: DicomImage::test_stub(Some(stale_overlay)),
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

        app.sync_current_state_to_history();

        let HistoryKind::Group(group) = &app.history_entries[0].kind else {
            panic!("expected group history entry");
        };
        let cached_viewport = group
            .viewports
            .iter()
            .find(|viewport| viewport.path == path_b)
            .expect("group history should keep the second viewport");
        assert!(
            cached_viewport.image.gsps_overlay.is_none(),
            "group history entry should persist GSPS removals from the live viewport"
        );
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
                            path: test_meta("cached-a.dcm"),
                            image: DicomImage::test_stub(None),
                            texture: texture_a,
                            label: "A".to_string(),
                            window_center: 0.0,
                            window_width: 1.0,
                            current_frame: 0,
                        },
                        HistoryGroupViewportData {
                            path: test_meta("cached-b.dcm"),
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
    fn open_history_entry_group_keeps_cached_gsps_when_pending_map_is_empty() {
        let ctx = egui::Context::default();
        let overlay = GspsOverlay::from_graphics(vec![GspsGraphic::Point {
            x: 1.0,
            y: 1.0,
            units: GspsUnits::Pixel,
        }]);
        let texture_image = ColorImage {
            size: [1, 1],
            pixels: vec![egui::Color32::BLACK],
        };
        let texture_a = ctx.load_texture(
            "history-group-gsps-keep-a",
            texture_image.clone(),
            TextureOptions::LINEAR,
        );
        let texture_b = ctx.load_texture(
            "history-group-gsps-keep-b",
            texture_image,
            TextureOptions::LINEAR,
        );
        let mut app = DicomViewerApp {
            history_entries: vec![HistoryEntry {
                id: history_id_from_paths(&[
                    test_source("cached-a.dcm"),
                    test_source("cached-b.dcm"),
                ]),
                kind: HistoryKind::Group(HistoryGroupData {
                    viewports: vec![
                        HistoryGroupViewportData {
                            path: test_meta("cached-a.dcm"),
                            image: DicomImage::test_stub(None),
                            texture: texture_a,
                            label: "A".to_string(),
                            window_center: 0.0,
                            window_width: 1.0,
                            current_frame: 0,
                        },
                        HistoryGroupViewportData {
                            path: test_meta("cached-b.dcm"),
                            image: DicomImage::test_stub(Some(overlay)),
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

        let displayed_viewport = app
            .loaded_mammo_viewports()
            .find(|viewport| viewport.path == test_meta("cached-b.dcm"))
            .expect("history group should open its second viewport");
        assert!(
            displayed_viewport.image.gsps_overlay.is_some(),
            "opening from history should not clear cached GSPS when no pending overlay map exists"
        );
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
            Some(test_meta("cached-report.dcm"))
        );
    }

    #[test]
    fn open_history_entry_report_clears_active_group_view() {
        let ctx = egui::Context::default();
        let texture = test_texture(&ctx, "active-group-texture");
        let mut app = DicomViewerApp {
            mammo_group: vec![
                Some(MammoViewport {
                    path: test_meta("group-a.dcm"),
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
                    path: test_meta("group-b.dcm"),
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
            path: test_source("preloaded-report.dcm"),
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
    fn preload_non_active_groups_into_history_only_applies_completed_filter_when_requested() {
        let (_tx, rx) = mpsc::channel::<Result<HistoryPreloadResult, String>>();
        let ctx = egui::Context::default();
        let groups = vec![
            PreparedLoadPaths {
                image_paths: vec![test_source("group-0.dcm")],
                ..Default::default()
            },
            PreparedLoadPaths {
                image_paths: vec![test_source("group-1.dcm")],
                ..Default::default()
            },
            PreparedLoadPaths {
                image_paths: vec![test_source("group-2.dcm")],
                ..Default::default()
            },
        ];
        let completed_background_groups = HashSet::from([1usize]);

        let mut local_app = DicomViewerApp {
            history_preload_receiver: Some(rx),
            dicomweb_completed_background_groups: completed_background_groups.clone(),
            ..Default::default()
        };
        local_app.preload_non_active_groups_into_history(&groups, 0, None, &ctx);

        assert_eq!(local_app.history_preload_queue.len(), 2);

        let (_tx, rx) = mpsc::channel::<Result<HistoryPreloadResult, String>>();
        let mut dicomweb_app = DicomViewerApp {
            history_preload_receiver: Some(rx),
            dicomweb_completed_background_groups: completed_background_groups.clone(),
            ..Default::default()
        };
        dicomweb_app.preload_non_active_groups_into_history(
            &groups,
            0,
            Some(&completed_background_groups),
            &ctx,
        );

        assert_eq!(dicomweb_app.history_preload_queue.len(), 1);
    }

    #[test]
    fn preload_non_active_groups_into_history_skips_duplicate_queued_groups() {
        let (_tx, rx) = mpsc::channel::<Result<HistoryPreloadResult, String>>();
        let ctx = egui::Context::default();
        let groups = vec![
            PreparedLoadPaths {
                image_paths: vec![test_source("group-0.dcm")],
                ..Default::default()
            },
            PreparedLoadPaths {
                image_paths: vec![test_source("group-1.dcm")],
                ..Default::default()
            },
            PreparedLoadPaths {
                image_paths: vec![test_source("group-2.dcm")],
                ..Default::default()
            },
        ];

        let mut app = DicomViewerApp {
            history_preload_receiver: Some(rx),
            ..Default::default()
        };

        app.preload_non_active_groups_into_history(&groups, 0, None, &ctx);
        app.preload_non_active_groups_into_history(&groups, 0, None, &ctx);

        assert_eq!(app.history_preload_queue.len(), 2);
    }

    #[test]
    fn stage_structured_report_history_entries_uses_background_preload() {
        let path = write_test_structured_report_file("history-stage-report");
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp::default();

        app.stage_structured_report_history_entries(std::slice::from_ref(&path), &ctx);

        assert!(app.history_entries.is_empty());

        let receiver = app
            .history_preload_receiver
            .take()
            .expect("report staging should start a history preload worker");
        let result = receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("report preload should complete")
            .expect("report preload should succeed");

        match result {
            HistoryPreloadResult::Report {
                path: result_path, ..
            } => {
                assert_eq!(result_path, path);
            }
            HistoryPreloadResult::Single { .. } | HistoryPreloadResult::Group { .. } => {
                panic!("expected a report history preload result");
            }
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn poll_dicomweb_active_paths_stages_streamed_structured_report_in_history() {
        let path = write_test_structured_report_file("streamed-active-report");
        let (tx, rx) = mpsc::channel::<DicomWebGroupStreamUpdate>();
        tx.send(DicomWebGroupStreamUpdate::ActiveGroupInstanceCount(2))
            .expect("streamed group count should send");
        tx.send(DicomWebGroupStreamUpdate::ActivePath(path.clone().into()))
            .expect("streamed report path should send");
        drop(tx);

        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            dicomweb_active_path_receiver: Some(rx),
            ..Default::default()
        };

        app.poll_dicomweb_active_paths(&ctx);

        assert_eq!(app.dicomweb_active_group_expected, Some(2));
        assert_eq!(app.mammo_group.len(), 2);
        assert!(app.dicomweb_active_group_paths.is_empty());

        let receiver = app
            .history_preload_receiver
            .take()
            .expect("streamed report should be staged into history preload");
        let result = receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("streamed report preload should complete")
            .expect("streamed report preload should succeed");

        match result {
            HistoryPreloadResult::Report {
                path: result_path, ..
            } => {
                assert_eq!(result_path, path);
            }
            HistoryPreloadResult::Single { .. } | HistoryPreloadResult::Group { .. } => {
                panic!("expected a report history preload result");
            }
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn poll_dicomweb_active_paths_merges_streamed_parametric_map_overlay() {
        let study_uid = "9.999.102.1";
        let pm_series_uid = "9.999.102.3";
        let image_uid = "9.999.102.10";
        let pm_uid = "9.999.102.20";
        let pm_source = test_memory_parametric_map_source(
            "streamed-pm",
            study_uid,
            pm_series_uid,
            pm_uid,
            image_uid,
        );

        let (tx, rx) = mpsc::channel::<DicomWebGroupStreamUpdate>();
        tx.send(DicomWebGroupStreamUpdate::ActiveGroupInstanceCount(1))
            .expect("streamed group count should send");
        tx.send(DicomWebGroupStreamUpdate::ActivePath(pm_source))
            .expect("streamed Parametric Map path should send");
        drop(tx);

        let ctx = egui::Context::default();
        let mut image = DicomImage::test_stub_with_mono_frames(None, 1);
        image.sop_instance_uid = Some(image_uid.to_string());
        let mut app = DicomViewerApp {
            image: Some(image),
            current_single_path: Some(test_meta("displayed-image.dcm")),
            texture: Some(test_texture(&ctx, "streamed-pm-active")),
            dicomweb_active_path_receiver: Some(rx),
            ..Default::default()
        };

        app.poll_dicomweb_active_paths(&ctx);

        assert!(app.pending_pm_overlays.contains_key(image_uid));
        assert!(
            app.image
                .as_ref()
                .and_then(|image| image.pm_overlay.as_ref())
                .is_some(),
            "streamed Parametric Map overlays should attach to the displayed image"
        );
        assert!(app.load_error_message.is_none());
    }

    #[test]
    fn attach_matching_pm_overlay_clears_stale_overlay_when_updated_layers_do_not_match() {
        let image_uid = "9.999.102.30";
        let pm_source = test_memory_parametric_map_source(
            "stale-pm",
            "9.999.102.21",
            "9.999.102.22",
            "9.999.102.23",
            image_uid,
        );
        let overlays = load_parametric_map_overlays(&pm_source)
            .expect("Parametric Map overlay source should parse");
        let stale_overlay = overlays
            .get(image_uid)
            .cloned()
            .expect("overlay should be keyed by the referenced image UID");

        let mut image = DicomImage::test_stub_with_mono_frames(None, 1);
        image.width = 2;
        image.sop_instance_uid = Some(image_uid.to_string());
        image.pm_overlay = Some(stale_overlay);

        DicomViewerApp::attach_matching_pm_overlay(&mut image, &overlays);

        assert!(
            image.pm_overlay.is_none(),
            "filtered-out authoritative Parametric Map overlays should clear stale heatmaps"
        );
    }

    #[test]
    fn poll_dicomweb_active_paths_keeps_mammo_sender_until_streamed_paths_are_dispatched() {
        let image_source = test_memory_image_source(
            "streamed-active-image",
            "9.999.103.1",
            "9.999.103.2",
            "9.999.103.10",
        );
        let (tx, rx) = mpsc::channel::<DicomWebGroupStreamUpdate>();
        tx.send(DicomWebGroupStreamUpdate::ActiveGroupInstanceCount(2))
            .expect("streamed group count should send");
        tx.send(DicomWebGroupStreamUpdate::ActivePath(image_source.clone()))
            .expect("streamed image path should send");
        drop(tx);

        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            dicomweb_active_path_receiver: Some(rx),
            ..Default::default()
        };

        app.poll_dicomweb_active_paths(&ctx);

        assert!(app.load_error_message.is_none());
        assert!(app.mammo_load_sender.is_some());
        assert!(app.mammo_load_receiver.is_some());
        assert_eq!(
            app.dicomweb_active_group_paths,
            vec![(&image_source).into()]
        );
    }

    #[test]
    fn poll_dicomweb_active_paths_preloads_completed_background_group_before_final_result() {
        let path = write_test_structured_report_file("streamed-background-report");
        let (tx, rx) = mpsc::channel::<DicomWebGroupStreamUpdate>();
        tx.send(DicomWebGroupStreamUpdate::BackgroundGroupReady {
            group_index: 1,
            paths: vec![path.clone().into()],
        })
        .expect("background group update should send");
        drop(tx);

        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            dicomweb_active_path_receiver: Some(rx),
            ..Default::default()
        };

        app.poll_dicomweb_active_paths(&ctx);

        assert!(app.dicomweb_completed_background_groups.contains(&1));
        let receiver = app
            .history_preload_receiver
            .take()
            .expect("background group should be staged into history preload");
        let result = receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("background group preload should complete")
            .expect("background group preload should succeed");

        match result {
            HistoryPreloadResult::Report {
                path: result_path, ..
            } => {
                assert_eq!(result_path, path);
            }
            HistoryPreloadResult::Single { .. } | HistoryPreloadResult::Group { .. } => {
                panic!("expected a report history preload result");
            }
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn poll_history_preload_drops_group_when_any_viewport_render_fails() {
        let (tx, rx) = mpsc::channel::<Result<HistoryPreloadResult, String>>();
        tx.send(Ok(HistoryPreloadResult::Group {
            viewports: vec![
                (
                    test_source("history-group-a.dcm"),
                    DicomImage::test_stub_with_mono_frames(None, 1),
                ),
                (
                    test_source("history-group-b.dcm"),
                    DicomImage::test_stub_with_mono_frames(None, 1),
                ),
                (
                    test_source("history-group-c.dcm"),
                    DicomImage::test_stub(None),
                ),
                (
                    test_source("history-group-d.dcm"),
                    DicomImage::test_stub_with_mono_frames(None, 1),
                ),
            ],
        }))
        .expect("group preload should send");
        drop(tx);

        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            history_preload_receiver: Some(rx),
            ..Default::default()
        };

        app.poll_history_preload(&ctx);

        assert!(app.history_entries.is_empty());
    }

    #[test]
    fn poll_dicomweb_active_paths_preserves_open_group_when_stream_sender_is_missing() {
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            mammo_group: vec![
                Some(MammoViewport {
                    path: test_meta("history-a.dcm"),
                    image: DicomImage::test_stub(None),
                    texture: test_texture(&ctx, "history-group-a"),
                    label: "A".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
                Some(MammoViewport {
                    path: test_meta("history-b.dcm"),
                    image: DicomImage::test_stub(None),
                    texture: test_texture(&ctx, "history-group-b"),
                    label: "B".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
            ],
            dicomweb_active_group_expected: Some(2),
            dicomweb_active_pending_paths: VecDeque::from(vec![test_source("active-a.dcm")]),
            ..Default::default()
        };
        let expected_history_id = app
            .current_history_id()
            .expect("complete history group should have a stable id");

        app.poll_dicomweb_active_paths(&ctx);

        assert!(app.load_error_message.is_none());
        assert!(app.mammo_group_complete());
        assert_eq!(app.current_history_id(), Some(expected_history_id));
        assert_eq!(app.dicomweb_active_group_expected, Some(2));
        assert_eq!(
            app.dicomweb_active_group_paths,
            vec![test_meta("active-a.dcm")]
        );
        assert!(app.dicomweb_active_pending_paths.is_empty());
    }

    #[test]
    fn poll_dicomweb_active_paths_skips_streamed_single_activation_when_other_study_is_open() {
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            image: Some(DicomImage::test_stub(None)),
            current_single_path: Some(test_meta("history-open.dcm")),
            texture: Some(test_texture(&ctx, "history-open-texture")),
            dicomweb_active_group_expected: Some(1),
            dicomweb_active_pending_paths: VecDeque::from(vec![test_source("active-single.dcm")]),
            ..Default::default()
        };

        app.poll_dicomweb_active_paths(&ctx);

        assert_eq!(app.current_single_path, Some(test_meta("history-open.dcm")));
        assert!(app.single_load_receiver.is_none());
        assert_eq!(
            app.dicomweb_active_group_paths,
            vec![test_meta("active-single.dcm")]
        );
        assert!(app.load_error_message.is_none());
    }

    #[test]
    fn poll_dicomweb_grouped_preloads_active_group_when_viewing_other_group() {
        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        tx.send(Ok(DicomWebDownloadResult::Grouped {
            groups: vec![vec![
                test_source("active-a.dcm"),
                test_source("active-b.dcm"),
            ]],
            open_group: 0,
        }))
        .expect("grouped result should send");

        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            dicomweb_receiver: Some(rx),
            dicomweb_active_group_expected: Some(2),
            dicomweb_active_group_paths: vec![test_meta("active-a.dcm"), test_meta("active-b.dcm")],
            mammo_group: vec![
                Some(MammoViewport {
                    path: test_meta("history-a.dcm"),
                    image: DicomImage::test_stub(None),
                    texture: test_texture(&ctx, "other-group-a"),
                    label: "A".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
                Some(MammoViewport {
                    path: test_meta("history-b.dcm"),
                    image: DicomImage::test_stub(None),
                    texture: test_texture(&ctx, "other-group-b"),
                    label: "B".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
            ],
            ..Default::default()
        };
        let displayed_history_id = app
            .current_history_id()
            .expect("current group should have a stable id");

        app.poll_dicomweb_download(&ctx);

        assert_eq!(app.current_history_id(), Some(displayed_history_id));
        assert!(app.history_preload_receiver.is_some());
        assert!(app.history_entries.is_empty());
    }

    #[test]
    fn poll_dicomweb_grouped_backfills_gsps_for_displayed_open_group() {
        let study_uid = "9.999.100.1";
        let series_uid = "9.999.100.2";
        let gsps_series_uid = "9.999.100.3";
        let image_a_uid = "9.999.100.10";
        let image_b_uid = "9.999.100.11";
        let gsps_uid = "9.999.100.20";

        let image_a_source =
            test_memory_image_source("active-a", study_uid, series_uid, image_a_uid);
        let image_b_source =
            test_memory_image_source("active-b", study_uid, series_uid, image_b_uid);
        let gsps_source = test_memory_gsps_source(
            "active-b-gsps",
            study_uid,
            gsps_series_uid,
            gsps_uid,
            image_b_uid,
        );

        let current_group_paths = vec![image_a_source.clone(), image_b_source.clone()];
        let expected_history_id = history_id_from_paths(current_group_paths.as_slice());

        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        tx.send(Ok(DicomWebDownloadResult::Grouped {
            groups: vec![vec![
                image_a_source.clone(),
                image_b_source.clone(),
                gsps_source,
            ]],
            open_group: 0,
        }))
        .expect("grouped result should send");

        let ctx = egui::Context::default();
        let mut image_a = DicomImage::test_stub_with_mono_frames(None, 1);
        image_a.sop_instance_uid = Some(image_a_uid.to_string());
        let mut image_b = DicomImage::test_stub_with_mono_frames(None, 1);
        image_b.sop_instance_uid = Some(image_b_uid.to_string());

        let mut app = DicomViewerApp {
            dicomweb_receiver: Some(rx),
            dicomweb_active_group_expected: Some(2),
            dicomweb_active_group_paths: vec![(&image_a_source).into(), (&image_b_source).into()],
            mammo_group: vec![
                Some(MammoViewport {
                    path: (&image_a_source).into(),
                    image: image_a,
                    texture: test_texture(&ctx, "displayed-active-a"),
                    label: "A".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
                Some(MammoViewport {
                    path: (&image_b_source).into(),
                    image: image_b,
                    texture: test_texture(&ctx, "displayed-active-b"),
                    label: "B".to_string(),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    zoom: 1.0,
                    pan: egui::Vec2::ZERO,
                    frame_scroll_accum: 0.0,
                }),
            ],
            ..Default::default()
        };

        app.poll_dicomweb_download(&ctx);

        let displayed_viewport = app
            .loaded_mammo_viewports()
            .find(|viewport| viewport.path == (&image_b_source).into())
            .expect("displayed group should keep second viewport");
        assert!(
            displayed_viewport.image.gsps_overlay.is_some(),
            "open group should receive GSPS without needing history cycling"
        );
        assert!(app.has_available_overlay());

        let history_entry = app
            .history_entries
            .iter()
            .find(|entry| entry.id == expected_history_id)
            .expect("displayed group should be cached in history");
        let HistoryKind::Group(group) = &history_entry.kind else {
            panic!("expected grouped history entry");
        };
        let cached_viewport = group
            .viewports
            .iter()
            .find(|viewport| viewport.path == (&image_b_source).into())
            .expect("history group should keep second viewport");
        assert!(
            cached_viewport.image.gsps_overlay.is_some(),
            "history cache should preserve the backfilled GSPS overlay"
        );
    }

    #[test]
    fn poll_dicomweb_grouped_backfills_gsps_on_first_open_of_background_history_group() {
        let study_uid = "9.999.101.1";
        let active_series_uid = "9.999.101.2";
        let background_series_a_uid = "9.999.101.3";
        let background_series_b_uid = "9.999.101.4";
        let background_gsps_series_uid = "9.999.101.5";
        let active_image_uid = "9.999.101.10";
        let background_image_a_uid = "9.999.101.11";
        let background_image_b_uid = "9.999.101.12";
        let background_gsps_uid = "9.999.101.20";

        let active_source = test_memory_image_source(
            "active-single",
            study_uid,
            active_series_uid,
            active_image_uid,
        );
        let background_image_a_source = test_memory_image_source(
            "background-a",
            study_uid,
            background_series_a_uid,
            background_image_a_uid,
        );
        let background_image_b_source = test_memory_image_source(
            "background-b",
            study_uid,
            background_series_b_uid,
            background_image_b_uid,
        );
        let background_gsps_source = test_memory_gsps_source(
            "background-b-gsps",
            study_uid,
            background_gsps_series_uid,
            background_gsps_uid,
            background_image_b_uid,
        );

        let mut background_image_a = DicomImage::test_stub_with_mono_frames(None, 1);
        background_image_a.sop_instance_uid = Some(background_image_a_uid.to_string());
        let mut background_image_b = DicomImage::test_stub_with_mono_frames(None, 1);
        background_image_b.sop_instance_uid = Some(background_image_b_uid.to_string());

        let background_group_paths = vec![
            background_image_a_source.clone(),
            background_image_b_source.clone(),
        ];
        let background_history_id = history_id_from_paths(background_group_paths.as_slice());

        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        tx.send(Ok(DicomWebDownloadResult::Grouped {
            groups: vec![
                vec![active_source.clone()],
                vec![
                    background_image_a_source.clone(),
                    background_image_b_source.clone(),
                    background_gsps_source,
                ],
            ],
            open_group: 0,
        }))
        .expect("grouped result should send");

        let ctx = egui::Context::default();
        let mut active_image = DicomImage::test_stub_with_mono_frames(None, 1);
        active_image.sop_instance_uid = Some(active_image_uid.to_string());

        let mut app = DicomViewerApp {
            dicomweb_receiver: Some(rx),
            dicomweb_active_group_expected: Some(1),
            dicomweb_active_group_paths: vec![(&active_source).into()],
            image: Some(active_image),
            current_single_path: Some((&active_source).into()),
            texture: Some(test_texture(&ctx, "active-single-history-open")),
            history_entries: vec![HistoryEntry {
                id: background_history_id.clone(),
                kind: HistoryKind::Group(HistoryGroupData {
                    viewports: vec![
                        HistoryGroupViewportData {
                            path: (&background_image_a_source).into(),
                            image: background_image_a,
                            texture: test_texture(&ctx, "background-history-a"),
                            label: "A".to_string(),
                            window_center: 0.0,
                            window_width: 1.0,
                            current_frame: 0,
                        },
                        HistoryGroupViewportData {
                            path: (&background_image_b_source).into(),
                            image: background_image_b,
                            texture: test_texture(&ctx, "background-history-b"),
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

        app.poll_dicomweb_download(&ctx);

        assert!(
            app.pending_gsps_overlays
                .contains_key(background_image_b_uid),
            "grouped download should retain GSPS from background groups"
        );

        app.open_history_entry(0, &ctx);

        let displayed_viewport = app
            .loaded_mammo_viewports()
            .find(|viewport| viewport.path == (&background_image_b_source).into())
            .expect("background group should open from history");
        assert!(
            displayed_viewport.image.gsps_overlay.is_some(),
            "background group should show GSPS on first open"
        );

        let history_entry = app
            .history_entries
            .iter()
            .find(|entry| entry.id == background_history_id)
            .expect("background history entry should remain available");
        let HistoryKind::Group(group) = &history_entry.kind else {
            panic!("expected grouped history entry");
        };
        let cached_viewport = group
            .viewports
            .iter()
            .find(|viewport| viewport.path == (&background_image_b_source).into())
            .expect("background history should keep second viewport");
        assert!(
            cached_viewport.image.gsps_overlay.is_some(),
            "background history cache should be repaired on first open"
        );
    }

    #[test]
    fn poll_dicomweb_grouped_backfills_parametric_map_for_displayed_open_group() {
        let study_uid = "9.999.104.1";
        let series_uid = "9.999.104.2";
        let pm_series_uid = "9.999.104.3";
        let image_uid = "9.999.104.10";
        let pm_uid = "9.999.104.20";

        let image_source =
            test_memory_image_source("active-image", study_uid, series_uid, image_uid);
        let pm_source = test_memory_parametric_map_source(
            "active-image-pm",
            study_uid,
            pm_series_uid,
            pm_uid,
            image_uid,
        );
        let expected_history_id = history_id_from_paths(std::slice::from_ref(&image_source));

        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        tx.send(Ok(DicomWebDownloadResult::Grouped {
            groups: vec![vec![image_source.clone(), pm_source]],
            open_group: 0,
        }))
        .expect("grouped result should send");

        let ctx = egui::Context::default();
        let mut image = DicomImage::test_stub_with_mono_frames(None, 1);
        image.sop_instance_uid = Some(image_uid.to_string());
        let mut app = DicomViewerApp {
            dicomweb_receiver: Some(rx),
            dicomweb_active_group_expected: Some(1),
            dicomweb_active_group_paths: vec![(&image_source).into()],
            image: Some(image),
            current_single_path: Some((&image_source).into()),
            texture: Some(test_texture(&ctx, "displayed-active-pm")),
            history_entries: vec![HistoryEntry {
                id: expected_history_id.clone(),
                kind: HistoryKind::Single(Box::new(HistorySingleData {
                    path: (&image_source).into(),
                    image: DicomImage::test_stub_with_mono_frames(None, 1),
                    texture: test_texture(&ctx, "displayed-active-pm-history"),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    cine_fps: DEFAULT_CINE_FPS,
                })),
                thumbs: Vec::new(),
            }],
            ..Default::default()
        };

        app.poll_dicomweb_download(&ctx);

        assert!(
            app.image
                .as_ref()
                .and_then(|image| image.pm_overlay.as_ref())
                .is_some(),
            "open group should receive Parametric Map overlays without needing history cycling"
        );
        assert!(app.has_available_overlay());

        let history_entry = app
            .history_entries
            .iter()
            .find(|entry| entry.id == expected_history_id)
            .expect("displayed image should be cached in history");
        let HistoryKind::Single(single) = &history_entry.kind else {
            panic!("expected single history entry");
        };
        assert!(
            single.image.pm_overlay.is_some(),
            "history cache should preserve the backfilled Parametric Map overlay"
        );
    }

    #[test]
    fn poll_dicomweb_grouped_preloads_active_single_when_viewing_other_study() {
        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        tx.send(Ok(DicomWebDownloadResult::Grouped {
            groups: vec![vec![test_source("active-single.dcm")]],
            open_group: 0,
        }))
        .expect("grouped result should send");

        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            dicomweb_receiver: Some(rx),
            dicomweb_active_group_expected: Some(1),
            dicomweb_active_group_paths: vec![test_meta("active-single.dcm")],
            image: Some(DicomImage::test_stub(None)),
            current_single_path: Some(test_meta("history-open.dcm")),
            texture: Some(test_texture(&ctx, "history-open-single")),
            ..Default::default()
        };

        app.poll_dicomweb_download(&ctx);

        assert_eq!(app.current_single_path, Some(test_meta("history-open.dcm")));
        assert!(app.history_preload_receiver.is_some());
    }

    #[test]
    fn close_current_group_removes_active_entry_and_opens_next_history_item() {
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            image: Some(DicomImage::test_stub(None)),
            current_single_path: Some(test_meta("current.dcm")),
            texture: Some(test_texture(&ctx, "active-current")),
            history_entries: vec![
                single_history_entry(&ctx, "current.dcm", "history-current"),
                single_history_entry(&ctx, "next.dcm", "history-next"),
            ],
            ..Default::default()
        };

        app.close_current_group(&ctx);

        assert_eq!(app.current_single_path, Some(test_meta("next.dcm")));
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
            current_single_path: Some(test_meta("lonely.dcm")),
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
    fn pending_history_open_uses_entry_id_across_history_reordering() {
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            image: Some(DicomImage::test_stub(None)),
            current_single_path: Some(test_meta("current.dcm")),
            texture: Some(test_texture(&ctx, "active-current")),
            history_entries: vec![
                single_history_entry(&ctx, "current.dcm", "history-current"),
                single_history_entry(&ctx, "target.dcm", "history-target"),
            ],
            ..Default::default()
        };

        app.queue_history_open(1);
        app.process_pending_history_open(&ctx);

        app.history_entries.insert(
            0,
            single_history_entry(&ctx, "new-front.dcm", "history-new-front"),
        );

        app.process_pending_history_open(&ctx);

        assert_eq!(app.current_single_path, Some(test_meta("target.dcm")));
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
            groups: vec![vec![test_source("group-a.dcm"), test_source("group-b.dcm")]],
            open_group: 0,
        }))
        .expect("grouped result should send");

        let mut app = DicomViewerApp {
            dicomweb_receiver: Some(rx),
            dicomweb_active_group_paths: vec![test_meta("group-a.dcm")],
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
    fn poll_dicomweb_grouped_defers_stream_teardown_until_mammo_worker_finishes() {
        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        tx.send(Ok(DicomWebDownloadResult::Grouped {
            groups: vec![vec![test_source("group-a.dcm"), test_source("group-b.dcm")]],
            open_group: 0,
        }))
        .expect("grouped result should send");

        let (mammo_tx, mammo_rx) = mpsc::channel::<Result<PendingLoad, String>>();
        let mut app = DicomViewerApp {
            dicomweb_receiver: Some(rx),
            dicomweb_active_group_expected: Some(2),
            dicomweb_active_group_paths: vec![test_meta("group-a.dcm"), test_meta("group-b.dcm")],
            mammo_load_receiver: Some(mammo_rx),
            mammo_load_sender: Some(mammo_tx),
            ..Default::default()
        };

        let ctx = egui::Context::default();
        app.poll_dicomweb_download(&ctx);

        assert_eq!(app.dicomweb_active_group_expected, Some(2));
        assert_eq!(
            app.dicomweb_active_group_paths,
            vec![test_meta("group-a.dcm"), test_meta("group-b.dcm")]
        );
        assert!(app.mammo_load_receiver.is_some());
        assert!(app.mammo_load_sender.is_some());
    }

    #[test]
    fn start_dicomweb_download_clears_inflight_single_load() {
        let (_tx, rx) = mpsc::channel::<Result<PendingSingleLoad, String>>();
        let mut app = DicomViewerApp {
            single_load_receiver: Some(rx),
            ..Default::default()
        };

        app.start_dicomweb_download(DicomWebLaunchRequest {
            base_url: String::new(),
            study_uid: String::new(),
            series_uid: None,
            instance_uid: Some("1.2.3".to_string()),
            username: None,
            password: None,
        });

        assert!(app.single_load_receiver.is_none());
        assert!(app.dicomweb_receiver.is_some());
    }

    #[test]
    fn start_dicomweb_group_download_clears_inflight_single_load() {
        let (_tx, rx) = mpsc::channel::<Result<PendingSingleLoad, String>>();
        let mut app = DicomViewerApp {
            single_load_receiver: Some(rx),
            ..Default::default()
        };

        app.start_dicomweb_group_download(DicomWebGroupedLaunchRequest {
            base_url: String::new(),
            study_uid: String::new(),
            groups: Vec::new(),
            open_group: 0,
            username: None,
            password: None,
        });

        assert!(app.single_load_receiver.is_none());
        assert!(app.dicomweb_receiver.is_some());
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
            Some("Failed to load selected item.")
        );
    }

    #[test]
    fn poll_single_load_clears_user_visible_error_on_success() {
        let (tx, rx) = mpsc::channel::<Result<PendingSingleLoad, String>>();
        tx.send(Ok(PendingSingleLoad::Image(Box::new(PendingLoad {
            path: test_source("selected.dcm"),
            image: DicomImage::test_stub(None),
        }))))
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
            path: test_source("report.dcm"),
            report: Box::new(StructuredReportDocument::test_stub()),
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
        assert_eq!(app.current_single_path, Some(test_meta("report.dcm")));
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
    fn apply_loaded_structured_report_clears_active_group_view() {
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            mammo_group: vec![None, None],
            ..Default::default()
        };

        app.apply_loaded_structured_report(
            test_source("report.dcm"),
            StructuredReportDocument::test_stub(),
            &ctx,
        );

        assert!(app.mammo_group.is_empty());
        assert_eq!(app.current_single_path, Some(test_meta("report.dcm")));
        assert_eq!(
            app.report.as_ref().map(|report| report.title.as_str()),
            Some("Structured Report")
        );
    }

    #[test]
    fn apply_loaded_structured_report_preserves_pending_sr_overlays_for_history_reopen() {
        let ctx = egui::Context::default();
        let mut cached_image = DicomImage::test_stub_with_mono_frames(None, 1);
        cached_image.sop_instance_uid = Some("1.2.3".to_string());
        let sr_overlay = SrOverlay {
            graphics: vec![SrOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 6.0,
                    y: 7.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: None,
                rendering_intent: SrRenderingIntent::PresentationRequired,
                cad_operating_point: Some(1.0),
            }],
        };
        let image_path = test_meta("cached-image.dcm");
        let mut app = DicomViewerApp {
            pending_sr_overlays: HashMap::from([("1.2.3".to_string(), sr_overlay)]),
            history_entries: vec![HistoryEntry {
                id: history_id_from_paths(std::slice::from_ref(&image_path)),
                kind: HistoryKind::Single(Box::new(HistorySingleData {
                    path: image_path.clone(),
                    image: cached_image,
                    texture: test_texture(&ctx, "report-preserves-pending-sr-image"),
                    window_center: 0.0,
                    window_width: 1.0,
                    current_frame: 0,
                    cine_fps: DEFAULT_CINE_FPS,
                })),
                thumbs: Vec::new(),
            }],
            ..Default::default()
        };

        app.apply_loaded_structured_report(
            test_source("report.dcm"),
            StructuredReportDocument::test_stub(),
            &ctx,
        );

        assert!(app.pending_sr_overlays.contains_key("1.2.3"));
        assert_eq!(app.history_entries.len(), 2);

        app.open_history_entry(1, &ctx);

        assert_eq!(app.current_single_path, Some(image_path));
        assert!(
            app.image
                .as_ref()
                .and_then(|image| image.sr_overlay.as_ref())
                .is_some(),
            "reopening a cached image after report-only open should reattach preserved SR overlays"
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
    fn is_supported_prepared_group_allows_sr_only_multi_report_groups() {
        let prepared = PreparedLoadPaths {
            structured_report_paths: vec![test_source("report-a.dcm"), test_source("report-b.dcm")],
            ..Default::default()
        };

        assert!(DicomViewerApp::is_supported_prepared_group(&prepared));
    }

    #[test]
    fn is_supported_prepared_group_allows_parametric_map_only_groups() {
        let prepared = PreparedLoadPaths {
            parametric_map_paths: vec![test_source("heatmap.dcm")],
            ..Default::default()
        };

        assert!(DicomViewerApp::is_supported_prepared_group(&prepared));
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
            Some("Selected DICOM objects are not displayable images, parametric maps, or structured reports.")
        );
    }

    #[test]
    fn apply_prepared_load_paths_rejection_preserves_overlay_state() {
        let (_tx, rx) = mpsc::channel::<Result<HistoryPreloadResult, String>>();
        let ctx = egui::Context::default();
        let existing_gsps = GspsOverlay::from_graphics(vec![GspsGraphic::Point {
            x: 1.0,
            y: 2.0,
            units: GspsUnits::Pixel,
        }]);
        let existing_sr = SrOverlay {
            graphics: vec![SrOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 3.0,
                    y: 4.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: None,
                rendering_intent: SrRenderingIntent::PresentationRequired,
                cad_operating_point: Some(1.0),
            }],
        };
        let mut app = DicomViewerApp {
            history_preload_receiver: Some(rx),
            history_preload_queue: VecDeque::from([HistoryPreloadJob::StructuredReport(
                test_source("queued-report.dcm"),
            )]),
            pending_gsps_overlays: HashMap::from([("1.2.3".to_string(), existing_gsps)]),
            authoritative_gsps_overlay_keys: HashSet::from(["1.2.3".to_string()]),
            pending_sr_overlays: HashMap::from([("4.5.6".to_string(), existing_sr)]),
            authoritative_sr_overlay_keys: HashSet::from(["4.5.6".to_string()]),
            overlay_visible: true,
            ..Default::default()
        };

        let result = app.apply_prepared_load_paths(
            PreparedLoadPaths {
                other_files_found: 1,
                ..Default::default()
            },
            &ctx,
        );

        assert!(result.is_err());
        assert!(app.history_preload_receiver.is_some());
        assert_eq!(app.history_preload_queue.len(), 1);
        assert!(app.overlay_visible);
        assert!(app.pending_gsps_overlays.contains_key("1.2.3"));
        assert!(app.authoritative_gsps_overlay_keys.contains("1.2.3"));
        assert!(app.pending_sr_overlays.contains_key("4.5.6"));
        assert!(app.authoritative_sr_overlay_keys.contains("4.5.6"));
    }

    #[test]
    fn apply_prepared_load_paths_preserves_history_preload_for_streamed_single_image() {
        let (_tx, rx) = mpsc::channel::<Result<HistoryPreloadResult, String>>();
        let ctx = egui::Context::default();
        let existing_gsps = GspsOverlay::from_graphics(vec![GspsGraphic::Point {
            x: 1.0,
            y: 2.0,
            units: GspsUnits::Pixel,
        }]);
        let existing_sr = SrOverlay {
            graphics: vec![SrOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 3.0,
                    y: 4.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: None,
                rendering_intent: SrRenderingIntent::PresentationRequired,
                cad_operating_point: Some(1.0),
            }],
        };
        let prepared_gsps = GspsOverlay::from_graphics(vec![GspsGraphic::Polyline {
            points: vec![(0.0, 0.0), (1.0, 1.0)],
            units: GspsUnits::Display,
            closed: false,
        }]);
        let prepared_sr = SrOverlay {
            graphics: vec![SrOverlayGraphic {
                graphic: GspsGraphic::Point {
                    x: 5.0,
                    y: 6.0,
                    units: GspsUnits::Pixel,
                },
                referenced_frames: Some(vec![1]),
                rendering_intent: SrRenderingIntent::PresentationRequired,
                cad_operating_point: Some(2.0),
            }],
        };
        let mut app = DicomViewerApp {
            dicomweb_active_group_expected: Some(1),
            dicomweb_active_group_paths: vec![test_meta("active-image.dcm")],
            history_preload_receiver: Some(rx),
            history_preload_queue: VecDeque::from([HistoryPreloadJob::StructuredReport(
                test_source("queued-report.dcm"),
            )]),
            pending_gsps_overlays: HashMap::from([("1.2.3".to_string(), existing_gsps)]),
            authoritative_gsps_overlay_keys: HashSet::from(["1.2.3".to_string()]),
            pending_sr_overlays: HashMap::from([("4.5.6".to_string(), existing_sr)]),
            authoritative_sr_overlay_keys: HashSet::from(["4.5.6".to_string()]),
            overlay_visible: true,
            ..Default::default()
        };

        let result = app.apply_prepared_load_paths(
            PreparedLoadPaths {
                image_paths: vec![test_source("active-image.dcm")],
                gsps_overlays: HashMap::from([("9.9.9".to_string(), prepared_gsps)]),
                sr_overlays: HashMap::from([("8.8.8".to_string(), prepared_sr)]),
                ..Default::default()
            },
            &ctx,
        );

        assert!(result.is_ok());
        assert!(app.history_preload_receiver.is_some());
        assert_eq!(app.history_preload_queue.len(), 1);
        assert!(app.overlay_visible);
        assert_eq!(app.pending_gsps_overlays.len(), 2);
        assert!(app.pending_gsps_overlays.contains_key("1.2.3"));
        assert!(app.pending_gsps_overlays.contains_key("9.9.9"));
        assert_eq!(app.pending_sr_overlays.len(), 2);
        assert!(app.pending_sr_overlays.contains_key("4.5.6"));
        assert!(app.pending_sr_overlays.contains_key("8.8.8"));
        assert_eq!(
            app.authoritative_gsps_overlay_keys,
            HashSet::from(["1.2.3".to_string()])
        );
        assert_eq!(
            app.authoritative_sr_overlay_keys,
            HashSet::from(["4.5.6".to_string()])
        );
    }

    #[test]
    fn apply_prepared_load_paths_single_stages_history_entries_after_overlay_reset() {
        let (_tx, rx) = mpsc::channel::<Result<HistoryPreloadResult, String>>();
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            history_preload_receiver: Some(rx),
            history_preload_queue: VecDeque::from([HistoryPreloadJob::StructuredReport(
                test_source("stale-report.dcm"),
            )]),
            ..Default::default()
        };

        let result = app.apply_prepared_load_paths(
            PreparedLoadPaths {
                image_paths: vec![test_source("active-image.dcm")],
                structured_report_paths: vec![test_source("fresh-report.dcm")],
                parametric_map_paths: vec![test_source("fresh-heatmap.dcm")],
                ..Default::default()
            },
            &ctx,
        );

        assert!(result.is_ok());
        assert!(app.history_preload_receiver.is_some());
        assert_eq!(app.history_preload_queue.len(), 1);
    }

    #[test]
    fn apply_prepared_load_paths_group_stages_history_entries_after_overlay_reset() {
        let (_tx, rx) = mpsc::channel::<Result<HistoryPreloadResult, String>>();
        let ctx = egui::Context::default();
        let mut app = DicomViewerApp {
            history_preload_receiver: Some(rx),
            history_preload_queue: VecDeque::from([HistoryPreloadJob::StructuredReport(
                test_source("stale-report.dcm"),
            )]),
            ..Default::default()
        };

        let result = app.apply_prepared_load_paths(
            PreparedLoadPaths {
                image_paths: vec![test_source("group-a.dcm"), test_source("group-b.dcm")],
                structured_report_paths: vec![test_source("fresh-report.dcm")],
                parametric_map_paths: vec![test_source("fresh-heatmap.dcm")],
                ..Default::default()
            },
            &ctx,
        );

        assert!(result.is_ok());
        assert!(app.history_preload_receiver.is_some());
        assert_eq!(app.history_preload_queue.len(), 1);
    }

    #[test]
    fn poll_dicomweb_single_preserves_error_on_sync_rejection() {
        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        tx.send(Ok(DicomWebDownloadResult::Single(
            (0..5)
                .map(|index| DicomSource::from(PathBuf::from(format!("invalid-{index}.dcm"))))
                .collect::<Vec<_>>(),
        )))
        .expect("single result should send");

        let mut app = DicomViewerApp {
            dicomweb_receiver: Some(rx),
            dicomweb_active_group_expected: Some(3),
            dicomweb_active_group_paths: vec![test_meta("active-a.dcm")],
            dicomweb_active_pending_paths: VecDeque::from(vec![test_source("pending.dcm")]),
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
            vec![test_meta("active-a.dcm")]
        );
        assert_eq!(
            app.dicomweb_active_pending_paths,
            VecDeque::from(vec![test_source("pending.dcm")])
        );
        assert!(app.history_pushed_for_active_group);
    }
}
