use std::collections::{HashSet, VecDeque};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use eframe::egui::{
    self, ColorImage, ResizeDirection, Sense, TextureHandle, TextureOptions, ViewportCommand,
};

use crate::dicom::{load_dicom, DicomImage, METADATA_FIELD_NAMES};
use crate::dicomweb::{
    download_dicomweb_group_request, download_dicomweb_request, DicomWebDownloadResult,
    DicomWebGroupStreamUpdate,
};
use crate::launch::{DicomWebGroupedLaunchRequest, DicomWebLaunchRequest, LaunchRequest};
use crate::mammo::{
    mammo_image_align, mammo_label, mammo_sort_key, order_mammo_indices, preferred_mammo_slot,
};
use crate::renderer::{render_rgb, render_window_level};

const APP_TITLE: &str = "Perspecta Viewer";
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const HISTORY_MAX_ENTRIES: usize = 24;
const HISTORY_THUMB_MAX_DIM: usize = 96;
const HISTORY_LIST_THUMB_MAX_DIM: f32 = 56.0;
const DEFAULT_CINE_FPS: f32 = 24.0;

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

struct PendingMammoLoad {
    path: PathBuf,
    image: DicomImage,
}

enum HistoryPreloadResult {
    Single {
        path: PathBuf,
        image: DicomImage,
    },
    Group {
        viewports: Vec<(PathBuf, DicomImage)>,
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
enum HistoryKind {
    Single(Box<HistorySingleData>),
    Group(HistoryGroupData),
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

pub struct DicomViewerApp {
    image: Option<DicomImage>,
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
    mammo_load_receiver: Option<Receiver<Result<PendingMammoLoad, String>>>,
    mammo_load_sender: Option<Sender<Result<PendingMammoLoad, String>>>,
    history_pushed_for_active_group: bool,
    history_preload_receiver: Option<Receiver<Result<HistoryPreloadResult, String>>>,
    window_center: f32,
    window_width: f32,
    status_line: String,
    current_frame: usize,
    cine_mode: bool,
    cine_fps: f32,
    last_cine_advance: Option<Instant>,
    single_view_zoom: f32,
    single_view_pan: egui::Vec2,
    single_view_frame_scroll_accum: f32,
}

impl Default for DicomViewerApp {
    fn default() -> Self {
        Self::new(None, None)
    }
}

impl DicomViewerApp {
    pub fn new(initial_request: Option<LaunchRequest>, initial_status: Option<String>) -> Self {
        let settings_path = metadata_settings_file_path();
        let visible_metadata_fields = settings_path
            .as_deref()
            .and_then(load_visible_metadata_fields)
            .unwrap_or_else(default_visible_metadata_fields);

        Self {
            image: None,
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
            mammo_load_receiver: None,
            mammo_load_sender: None,
            history_pushed_for_active_group: false,
            history_preload_receiver: None,
            window_center: 0.0,
            window_width: 1.0,
            status_line: initial_status.unwrap_or_default(),
            current_frame: 0,
            cine_mode: false,
            cine_fps: DEFAULT_CINE_FPS,
            last_cine_advance: None,
            single_view_zoom: 1.0,
            single_view_pan: egui::Vec2::ZERO,
            single_view_frame_scroll_accum: 0.0,
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
            || self.mammo_load_receiver.is_some()
            || self.history_preload_receiver.is_some()
            || self.pending_history_open_index.is_some()
            || self.pending_local_open_paths.is_some()
    }

    fn has_mammo_group(&self) -> bool {
        !self.mammo_group.is_empty()
            || self.mammo_load_receiver.is_some()
            || (self.dicomweb_active_group_expected == Some(4)
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
        self.mammo_group.len() == 4 && self.loaded_mammo_count() == 4
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

    fn set_mammo_group_frame(&mut self, frame_index: usize) {
        if self.loaded_mammo_count() == 0 {
            return;
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
            }
        }
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
            eprintln!(
                "Could not create settings directory {}: {err}",
                parent.display()
            );
            return;
        }

        let fields = ordered_visible_metadata_fields(&self.visible_metadata_fields);
        let contents = render_settings_toml(&fields);
        if let Err(err) = fs::write(path, contents) {
            eprintln!("Could not write settings file {}: {err}", path.display());
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
        self.status_line = "Loading selected DICOM(s)...".to_string();
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
        self.load_selected_paths(paths, ctx);
    }

    fn clear_single_viewer(&mut self) {
        self.image = None;
        self.current_single_path = None;
        self.texture = None;
        self.current_frame = 0;
        self.cine_mode = false;
        self.last_cine_advance = None;
        self.mammo_selected_index = 0;
        self.reset_single_view_transform();
        self.single_view_frame_scroll_accum = 0.0;
    }

    fn reset_single_view_transform(&mut self) {
        self.single_view_zoom = 1.0;
        self.single_view_pan = egui::Vec2::ZERO;
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
        if group.len() != 4 {
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
        paths: &[PathBuf],
        tx: &mpsc::Sender<Result<HistoryPreloadResult, String>>,
    ) {
        let result = match paths.len() {
            1 => {
                let path = paths[0].clone();
                load_dicom(&path)
                    .map(|image| HistoryPreloadResult::Single { path, image })
                    .map_err(|err| format!("{err:#}"))
            }
            4 => {
                let mut viewports = Vec::with_capacity(4);
                for path in paths {
                    let image = match load_dicom(path).map_err(|err| format!("{err:#}")) {
                        Ok(image) => image,
                        Err(err) => {
                            let _ = tx.send(Err(err));
                            return;
                        }
                    };
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
        groups: &[Vec<PathBuf>],
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
                Self::preload_group_into_history(&group, &tx);
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
                self.image = Some(single.image);
                self.current_single_path = Some(single.path);
                self.texture = None;
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
                self.rebuild_texture(ctx);
                self.status_line = "Loaded study from memory cache.".to_string();
                ctx.request_repaint();
            }
            HistoryKind::Group(group) => {
                self.mammo_load_receiver = None;
                self.mammo_load_sender = None;
                self.clear_single_viewer();
                let ordered_indices =
                    order_mammo_indices(&group.viewports, |viewport| &viewport.image);
                let selected_index = ordered_indices
                    .iter()
                    .position(|index| *index == group.selected_index);
                let mut viewports = group.viewports.into_iter().map(Some).collect::<Vec<_>>();
                let mut ordered = Vec::with_capacity(viewports.len());
                for index in ordered_indices {
                    if let Some(viewport) = viewports[index].take() {
                        ordered.push(Some(MammoViewport {
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
                        }));
                    }
                }
                self.mammo_group = ordered;
                if self.loaded_mammo_count() == 0 {
                    self.status_line = "History entry had no cached mammo images.".to_string();
                    return;
                }
                self.mammo_selected_index = selected_index
                    .unwrap_or(group.selected_index)
                    .min(self.mammo_group.len().saturating_sub(1));
                self.status_line.clear();
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
            self.status_line = "Launch request had no groups to open.".to_string();
            return;
        }

        for (index, group) in groups.iter().enumerate() {
            if group.len() != 1 && group.len() != 4 {
                self.status_line = format!(
                    "Launch group {} has {} paths; each group must contain 1 or 4 DICOM files.",
                    index,
                    group.len()
                );
                return;
            }
        }

        let active_group = open_group.min(groups.len().saturating_sub(1));
        self.load_selected_paths(groups[active_group].clone(), ctx);
        self.preload_non_active_groups_into_history(&groups, active_group);
    }

    fn start_dicomweb_download(&mut self, request: DicomWebLaunchRequest) {
        if self.dicomweb_receiver.is_some() {
            self.status_line = "DICOMweb download already in progress.".to_string();
            return;
        }

        self.sync_current_state_to_history();
        self.history_preload_receiver = None;
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.history_pushed_for_active_group = false;
        self.dicomweb_active_path_receiver = None;
        self.dicomweb_active_group_expected = None;
        self.dicomweb_active_group_paths.clear();
        self.dicomweb_active_pending_paths.clear();
        self.status_line = "Loading study from DICOMweb...".to_string();
        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        thread::spawn(move || {
            let result = download_dicomweb_request(&request).map_err(|err| format!("{err:#}"));
            let _ = tx.send(result);
        });
        self.dicomweb_receiver = Some(rx);
    }

    fn start_dicomweb_group_download(&mut self, request: DicomWebGroupedLaunchRequest) {
        if self.dicomweb_receiver.is_some() {
            self.status_line = "DICOMweb download already in progress.".to_string();
            return;
        }

        self.sync_current_state_to_history();
        self.history_preload_receiver = None;
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.history_pushed_for_active_group = false;
        self.status_line = "Loading grouped study from DICOMweb...".to_string();
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
        pending: PendingMammoLoad,
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
                "Discarded streamed image {}: no available mammo slot (loaded={}, capacity={})",
                pending.path.display(),
                self.loaded_mammo_count(),
                self.mammo_group.len()
            ));
        };

        let default_center = pending.image.window_center;
        let default_width = pending.image.window_width;
        let Some(color_image) =
            Self::render_image_frame(&pending.image, 0, default_center, default_width)
        else {
            return Err(format!(
                "Could not prepare preview for {} (no decodable frame).",
                pending.path.display()
            ));
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

    fn poll_dicomweb_active_paths(&mut self, ctx: &egui::Context) {
        let mut keep_receiver = false;
        if let Some(receiver) = self.dicomweb_active_path_receiver.take() {
            keep_receiver = true;
            loop {
                match receiver.try_recv() {
                    Ok(DicomWebGroupStreamUpdate::ActiveGroupInstanceCount(count)) => {
                        self.dicomweb_active_group_expected = Some(count);
                        if count == 4 {
                            self.mammo_load_receiver = None;
                            self.mammo_load_sender = None;
                            self.history_pushed_for_active_group = false;
                            self.clear_single_viewer();
                            self.mammo_group = (0..4).map(|_| None).collect();
                            self.mammo_selected_index = 0;
                            self.cine_mode = false;
                            self.last_cine_advance = None;
                            self.status_line =
                                "Loading grouped study from DICOMweb (streaming active group)..."
                                    .to_string();
                            let (tx, rx) = mpsc::channel::<Result<PendingMammoLoad, String>>();
                            self.mammo_load_sender = Some(tx);
                            self.mammo_load_receiver = Some(rx);
                        }
                    }
                    Ok(DicomWebGroupStreamUpdate::ActivePath(path)) => {
                        self.dicomweb_active_pending_paths.push_back(path);
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        if self.dicomweb_active_group_expected == Some(4) {
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
            self.dicomweb_active_group_paths.push(path.clone());
            match expected {
                1 => {
                    self.load_selected_paths(vec![path], ctx);
                }
                4 => {
                    if let Some(sender) = self.mammo_load_sender.as_ref().cloned() {
                        thread::spawn(move || {
                            let result = match load_dicom(&path) {
                                Ok(image) => Ok(PendingMammoLoad { path, image }),
                                Err(err) => Err(format!(
                                    "Error opening streamed DICOM {}: {err:#}",
                                    path.display()
                                )),
                            };
                            let _ = sender.send(result);
                        });
                    } else {
                        self.status_line =
                            "Streaming mammo load channel not available.".to_string();
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
                                eprintln!(
                                    "History preload skipped group viewport {} (instance {:?}).",
                                    path.display(),
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
                        if loaded.len() == 4 {
                            loaded.sort_by(|a, b| {
                                mammo_sort_key(&a.image, &a.path)
                                    .cmp(&mammo_sort_key(&b.image, &b.path))
                            });
                            self.push_group_history_entry(&loaded, 0, ctx);
                            self.move_current_history_to_front();
                        }
                        break;
                    }
                    Err(err) => {
                        eprintln!("History preload skipped: {err}");
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
                        self.load_selected_paths(paths, ctx);
                        self.dicomweb_active_group_expected = None;
                        self.dicomweb_active_group_paths.clear();
                        self.dicomweb_active_pending_paths.clear();
                        self.dicomweb_active_path_receiver = None;
                        self.mammo_load_sender = None;
                        self.history_pushed_for_active_group = false;
                        if self.status_line.is_empty() {
                            self.status_line = "Loaded study from DICOMweb.".to_string();
                        }
                    }
                    DicomWebDownloadResult::Grouped { groups, open_group } => {
                        let active_group_len =
                            groups.get(open_group).map(|group| group.len()).unwrap_or(0);
                        let streamed_count = self.dicomweb_active_group_paths.len();
                        let streaming_started = streamed_count > 0
                            || !self.dicomweb_active_pending_paths.is_empty()
                            || ((active_group_len == 1 || active_group_len == 4)
                                && self.dicomweb_active_group_expected == Some(active_group_len));
                        let streamed_active_complete = streamed_count >= active_group_len
                            && (active_group_len == 1 || active_group_len == 4)
                            && self.dicomweb_active_pending_paths.is_empty();

                        if !streamed_active_complete && !streaming_started {
                            self.load_local_groups(groups, open_group, ctx);
                        } else {
                            self.preload_non_active_groups_into_history(&groups, open_group);
                            if active_group_len == 4
                                && self.mammo_group_complete()
                                && !self.history_pushed_for_active_group
                            {
                                let mut loaded = self
                                    .mammo_group
                                    .iter()
                                    .filter_map(Option::as_ref)
                                    .cloned()
                                    .collect::<Vec<_>>();
                                loaded.sort_by(|a, b| {
                                    mammo_sort_key(&a.image, &a.path)
                                        .cmp(&mammo_sort_key(&b.image, &b.path))
                                });
                                self.push_group_history_entry(
                                    &loaded,
                                    self.mammo_selected_index,
                                    ctx,
                                );
                                self.history_pushed_for_active_group = true;
                            }
                            self.move_current_history_to_front();
                        }

                        if streamed_active_complete || !streaming_started {
                            self.dicomweb_active_group_expected = None;
                            self.dicomweb_active_group_paths.clear();
                            self.dicomweb_active_pending_paths.clear();
                            self.dicomweb_active_path_receiver = None;
                            self.mammo_load_sender = None;
                            self.history_pushed_for_active_group = false;
                        }
                        if self.status_line.is_empty() {
                            self.status_line = "Loaded grouped study from DICOMweb.".to_string();
                        }
                    }
                },
                Err(err) => {
                    self.status_line = format!("DICOMweb error: {err}");
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
                self.status_line = "DICOMweb download worker disconnected.".to_string();
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
                        self.status_line = err;
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
                    if self.mammo_group_complete()
                        && (self.dicomweb_active_group_expected == Some(4)
                            || self.dicomweb_active_path_receiver.is_some())
                        && !self.history_pushed_for_active_group
                    {
                        let mut loaded = self
                            .mammo_group
                            .iter()
                            .filter_map(Option::as_ref)
                            .cloned()
                            .collect::<Vec<_>>();
                        loaded.sort_by(|a, b| {
                            mammo_sort_key(&a.image, &a.path)
                                .cmp(&mammo_sort_key(&b.image, &b.path))
                        });
                        self.push_group_history_entry(&loaded, self.mammo_selected_index, ctx);
                        self.move_current_history_to_front();
                        self.history_pushed_for_active_group = true;
                    }
                    ctx.request_repaint();
                }
                Err(err) => {
                    self.status_line = err;
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
            if !self.history_pushed_for_active_group {
                let mut loaded = self
                    .mammo_group
                    .iter()
                    .filter_map(Option::as_ref)
                    .cloned()
                    .collect::<Vec<_>>();
                loaded.sort_by(|a, b| {
                    mammo_sort_key(&a.image, &a.path).cmp(&mammo_sort_key(&b.image, &b.path))
                });
                self.push_group_history_entry(&loaded, self.mammo_selected_index, ctx);
            }
            self.status_line.clear();
        } else {
            self.status_line =
                "Mammo group load incomplete: worker exited before all images were received."
                    .to_string();
        }
        ctx.request_repaint();
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

    fn load_selected_paths(&mut self, paths: Vec<PathBuf>, ctx: &egui::Context) {
        if !paths.is_empty() {
            self.sync_current_state_to_history();
        }
        self.history_preload_receiver = None;

        match paths.len() {
            0 => {}
            1 => {
                self.mammo_load_receiver = None;
                self.mammo_load_sender = None;
                self.history_pushed_for_active_group = false;
                if let Some(path) = paths.into_iter().next() {
                    self.load_path(path, ctx);
                }
            }
            4 => self.load_mammo_group_paths(paths, ctx),
            other => {
                self.status_line = format!(
                    "Select 1 DICOM for 1x1 view or 4 DICOMs for mammo 2x2 (got {}).",
                    other
                );
            }
        }
    }

    fn load_path(&mut self, path: PathBuf, ctx: &egui::Context) {
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.history_pushed_for_active_group = false;
        match load_dicom(&path) {
            Ok(image) => {
                self.window_center = image.window_center;
                self.window_width = image.window_width;
                self.current_frame = 0;
                self.cine_mode = false;
                self.last_cine_advance = None;
                self.cine_fps = image
                    .recommended_cine_fps
                    .unwrap_or(DEFAULT_CINE_FPS)
                    .clamp(1.0, 120.0);

                self.image = Some(image);
                self.current_single_path = Some(path.clone());
                self.mammo_group.clear();
                self.mammo_selected_index = 0;
                self.reset_single_view_transform();
                self.single_view_frame_scroll_accum = 0.0;
                self.rebuild_texture(ctx);
                let history_image = self.image.clone();
                let history_texture = self.texture.clone();
                if let (Some(active_image), Some(texture)) =
                    (history_image.as_ref(), history_texture.as_ref())
                {
                    self.push_single_history_entry(
                        HistorySingleData {
                            path: path.clone(),
                            image: active_image.clone(),
                            texture: texture.clone(),
                            window_center: self.window_center,
                            window_width: self.window_width,
                            current_frame: self.current_frame,
                            cine_fps: self.cine_fps,
                        },
                        ctx,
                    );
                }
                self.status_line.clear();
            }
            Err(err) => {
                self.status_line = format!("Error opening {}: {err:#}", path.display());
            }
        }
    }

    fn load_mammo_group_paths(&mut self, paths: Vec<PathBuf>, ctx: &egui::Context) {
        if paths.len() != 4 {
            self.status_line = format!(
                "Mammo 2x2 group requires exactly 4 DICOM files (got {}).",
                paths.len()
            );
            return;
        }

        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.history_pushed_for_active_group = false;
        self.clear_single_viewer();
        self.mammo_group = (0..4).map(|_| None).collect();
        self.mammo_selected_index = 0;
        self.cine_mode = false;
        self.last_cine_advance = None;
        self.status_line = "Loading mammo 2x2 group...".to_string();

        let (tx, rx) = mpsc::channel::<Result<PendingMammoLoad, String>>();
        thread::spawn(move || {
            for path in paths {
                match load_dicom(&path) {
                    Ok(image) => {
                        let _ = tx.send(Ok(PendingMammoLoad { path, image }));
                    }
                    Err(err) => {
                        let _ = tx.send(Err(format!("Error opening {}: {err:#}", path.display())));
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
                self.status_line = "Cine mode requires a multi-frame DICOM.".to_string();
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
            self.status_line = "Mammo cine mode requires all 4 views to be loaded.".to_string();
            return;
        }

        let frame_count = self.mammo_group_common_frame_count();
        if frame_count <= 1 {
            self.cine_mode = false;
            self.status_line =
                "Mammo cine mode requires all 4 views to be multi-frame.".to_string();
            return;
        }

        let enabling = !self.cine_mode;
        self.cine_mode = enabling;
        self.last_cine_advance = Some(Instant::now());
        if enabling {
            let start_frame = self
                .selected_mammo_frame_index()
                .min(frame_count.saturating_sub(1));
            self.set_mammo_group_frame(start_frame);
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
                self.set_mammo_group_frame(next_frame);
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
                frame_pixels,
                image.invert,
                window_center,
                window_width,
            ))
        } else {
            let frame_pixels = image.frame_rgb_pixels(frame_index)?;
            Some(render_rgb(
                image.width,
                image.height,
                frame_pixels,
                image.samples_per_pixel,
            ))
        }
    }

    fn rebuild_texture(&mut self, ctx: &egui::Context) {
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
            self.texture = None;
            return;
        };

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
            self.set_mammo_group_frame(frame_index);
            self.last_cine_advance = Some(Instant::now());
        } else if let Some(viewport) = self.selected_mammo_viewport_mut() {
            viewport.window_center = state.window_center;
            viewport.window_width = state.window_width.max(1.0);
            if state.frame_count == 0 {
                viewport.current_frame = 0;
            } else {
                viewport.current_frame = state.current_frame.min(state.frame_count - 1);
            }
            self.rebuild_selected_mammo_texture();
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

    fn rebuild_selected_mammo_texture(&mut self) {
        let Some(viewport) = self.selected_mammo_viewport_mut() else {
            return;
        };
        let frame_count = viewport.image.frame_count();
        if frame_count == 0 {
            return;
        }

        viewport.current_frame = viewport.current_frame.min(frame_count.saturating_sub(1));
        let Some(color_image) = Self::render_image_frame(
            &viewport.image,
            viewport.current_frame,
            viewport.window_center,
            viewport.window_width,
        ) else {
            return;
        };
        viewport.texture.set(color_image, TextureOptions::LINEAR);
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
        if raw_steps > 0 {
            -raw_steps
        } else {
            raw_steps.unsigned_abs() as i32
        }
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

        ui.scope(|ui| {
            ui.spacing_mut().item_spacing = egui::vec2(MAMMO_GRID_GAP, MAMMO_GRID_GAP);

            let available = ui.available_size();
            let cell_width = ((available.x - MAMMO_GRID_GAP).max(2.0)) / 2.0;
            let cell_height = ((available.y - MAMMO_GRID_GAP).max(2.0)) / 2.0;
            let cell_size = egui::vec2(cell_width, cell_height);
            let mut clicked_index = None;
            let mut pending_frame_target: Option<(usize, usize)> = None;

            for row in 0..2 {
                ui.horizontal(|ui| {
                    for col in 0..2 {
                        let index = row * 2 + col;
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
                                        egui::Color32::from_rgb(90, 140, 220)
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
                                                let (modifiers, raw_scroll, smooth_scroll) = ui
                                                    .input(|input| {
                                                        (
                                                            input.modifiers,
                                                            input.raw_scroll_delta,
                                                            input.smooth_scroll_delta,
                                                        )
                                                    });
                                                let frame_scroll_mode =
                                                    Self::is_frame_scroll_input(modifiers);
                                                let scroll = Self::dominant_scroll_axis(
                                                    raw_scroll,
                                                    smooth_scroll,
                                                );

                                                if frame_scroll_mode {
                                                    let frame_count = viewport.image.frame_count();
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
                                                    let zoom_delta =
                                                        ui.input(|input| input.zoom_delta());
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
                                            ui.painter().with_clip_rect(viewport_rect).image(
                                                viewport.texture.id(),
                                                image_rect,
                                                egui::Rect::from_min_max(
                                                    egui::Pos2::ZERO,
                                                    egui::pos2(1.0, 1.0),
                                                ),
                                                egui::Color32::WHITE,
                                            );
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
                self.set_mammo_group_frame(frame_target);
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
                            egui::Color32::from_rgb(90, 140, 220)
                        } else {
                            egui::Color32::from_gray(35)
                        };
                        let fill_color = if is_current {
                            egui::Color32::from_gray(18)
                        } else {
                            egui::Color32::TRANSPARENT
                        };

                        egui::Frame::none()
                            .fill(fill_color)
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
        if self.is_loading() {
            ctx.set_cursor_icon(egui::CursorIcon::Progress);
        } else {
            ctx.set_cursor_icon(egui::CursorIcon::Default);
        }
        self.process_pending_history_open(ctx);
        self.process_pending_local_open(ctx);

        if let Some(request) = self.pending_launch_request.take() {
            self.handle_launch_request(request, ctx);
        }

        self.poll_dicomweb_active_paths(ctx);
        self.poll_dicomweb_download(ctx);
        self.poll_history_preload(ctx);
        self.poll_mammo_group_load(ctx);
        self.advance_cine_if_needed(ctx);

        let mut history_cycle_direction = None;
        let mut close_requested = false;
        let mut c_pressed = false;
        ctx.input_mut(|input| {
            if input.consume_key(egui::Modifiers::COMMAND, egui::Key::W) {
                close_requested = true;
            } else if input.consume_key(egui::Modifiers::SHIFT, egui::Key::Tab) {
                history_cycle_direction = Some(-1);
            } else if input.consume_key(egui::Modifiers::NONE, egui::Key::Tab) {
                history_cycle_direction = Some(1);
            }
            c_pressed = input.consume_key(egui::Modifiers::NONE, egui::Key::C);
        });
        if close_requested {
            ctx.send_viewport_cmd(ViewportCommand::Close);
            return;
        }
        if let Some(direction) = history_cycle_direction {
            self.cycle_history_entry(direction);
        }
        let history_transition_pending = self.pending_history_open_index.is_some();
        if c_pressed && !history_transition_pending {
            self.toggle_cine_mode();
        }

        let mut open_dicoms_clicked = false;

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
        let mut request_rebuild = false;

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
                                    - ui.spacing().item_spacing.x)
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
                                                ),
                                            )
                                            .on_hover_text("Reset Center")
                                            .clicked()
                                        {
                                            state.window_center = state.default_center;
                                            request_rebuild = true;
                                        }

                                        request_rebuild |= ui
                                            .add_sized(
                                                [slider_with_refresh_width, row_height],
                                                egui::Slider::new(
                                                    &mut state.window_center,
                                                    center_range,
                                                )
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
                                                ),
                                            )
                                            .on_hover_text("Reset Width")
                                            .clicked()
                                        {
                                            state.window_width = state.default_width;
                                            request_rebuild = true;
                                        }

                                        request_rebuild |= ui
                                            .add_sized(
                                                [slider_with_refresh_width, row_height],
                                                egui::Slider::new(
                                                    &mut state.window_width,
                                                    width_range,
                                                )
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
                                    - ui.spacing().item_spacing.x)
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
                                                ),
                                            )
                                            .on_hover_text("Reset Frame")
                                            .clicked()
                                        {
                                            frame_index = 0;
                                            state.current_frame = 0;
                                            self.last_cine_advance = Some(Instant::now());
                                            request_rebuild = true;
                                        }

                                        if ui
                                            .add_sized(
                                                [slider_with_refresh_width, row_height],
                                                egui::Slider::new(&mut frame_index, 0..=max_frame)
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

                                let button_width = 128.0;
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

                                        if ui
                                            .add(
                                                egui::Slider::new(&mut self.cine_fps, 1.0..=120.0)
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
                                        if ui
                                            .add(
                                                egui::Button::new(if self.cine_mode {
                                                    "Stop Cine (C)"
                                                } else {
                                                    "Start Cine (C)"
                                                })
                                                .min_size(egui::vec2(
                                                    button_width,
                                                    ui.spacing().interact_size.y,
                                                )),
                                            )
                                            .clicked()
                                        {
                                            toggle_cine_clicked = true;
                                        }
                                    },
                                );
                            }
                        });
                    });
                });
        }

        if toggle_cine_clicked {
            self.toggle_cine_mode();
        }

        // Avoid applying stale W/L UI state while cycling history quickly with Tab.
        if request_rebuild {
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
                    ui.painter().image(
                        texture.id(),
                        image_rect,
                        egui::Rect::from_min_max(egui::Pos2::ZERO, egui::pos2(1.0, 1.0)),
                        egui::Color32::WHITE,
                    );
                }
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

        if let Some(active_image) = self.active_image() {
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
                            for (key, value) in &active_image.metadata {
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

        if let Some(index) = open_history_index {
            self.queue_history_open(index);
        }

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

    let columns = if images.len() >= 4 { 2 } else { images.len() };
    let rows = images.len().div_ceil(columns);
    let cell_width = (max_dim / columns).max(1);
    let cell_height = (max_dim / rows).max(1);
    let target_width = cell_width * columns;
    let target_height = cell_height * rows;
    let mut pixels = vec![egui::Color32::BLACK; target_width * target_height];

    let align_mammo = images.len() == 4;

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
    fn streaming_mammo_group_counts_as_group() {
        let mut app = DicomViewerApp::default();
        assert!(!app.has_mammo_group());

        let (_tx, rx) = mpsc::channel::<Result<PendingMammoLoad, String>>();
        app.mammo_load_receiver = Some(rx);
        assert!(app.has_mammo_group());

        app.mammo_load_receiver = None;
        let (_tx, rx) = mpsc::channel::<DicomWebGroupStreamUpdate>();
        app.dicomweb_active_group_expected = Some(4);
        app.dicomweb_active_path_receiver = Some(rx);
        assert!(app.has_mammo_group());

        app.dicomweb_active_path_receiver = None;
        app.dicomweb_active_group_expected = Some(4);
        app.dicomweb_active_pending_paths
            .push_back(PathBuf::from("streamed.dcm"));
        assert!(app.has_mammo_group());
    }
}
