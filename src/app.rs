use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use eframe::egui::{
    self, ColorImage, ResizeDirection, Sense, TextureHandle, TextureOptions, ViewportCommand,
};

use crate::dicom::{load_dicom, DicomImage, METADATA_FIELD_NAMES};
use crate::dicomweb::{
    download_dicomweb_group_request, download_dicomweb_request, DicomWebDownloadResult,
};
use crate::launch::{DicomWebGroupedLaunchRequest, DicomWebLaunchRequest, LaunchRequest};
use crate::renderer::{render_rgb, render_window_level};

const APP_TITLE: &str = "Perspecta Viewer";
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const HISTORY_MAX_ENTRIES: usize = 24;
const HISTORY_THUMB_MAX_DIM: usize = 96;
const HISTORY_LIST_THUMB_MAX_DIM: f32 = 56.0;
const DEFAULT_CINE_FPS: f32 = 24.0;

struct MammoViewport {
    path: PathBuf,
    image: DicomImage,
    texture: TextureHandle,
    label: String,
    window_center: f32,
    window_width: f32,
    current_frame: usize,
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
    mammo_group: Vec<MammoViewport>,
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
    window_center: f32,
    window_width: f32,
    status_line: String,
    current_frame: usize,
    cine_mode: bool,
    cine_fps: f32,
    last_cine_advance: Option<Instant>,
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
            window_center: 0.0,
            window_width: 1.0,
            status_line: initial_status.unwrap_or_default(),
            current_frame: 0,
            cine_mode: false,
            cine_fps: DEFAULT_CINE_FPS,
            last_cine_advance: None,
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
            || self.pending_history_open_index.is_some()
            || self.pending_local_open_paths.is_some()
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
        self.mammo_group
            .iter()
            .map(|viewport| viewport.image.frame_count())
            .min()
            .unwrap_or(0)
    }

    fn set_mammo_group_frame(&mut self, frame_index: usize) {
        if self.mammo_group.is_empty() {
            return;
        }

        let (mut rendered_frames, safe_frames) = {
            let inputs = self
                .mammo_group
                .iter()
                .map(|viewport| {
                    let frame_count = viewport.image.frame_count();
                    let safe_frame = if frame_count == 0 {
                        0
                    } else {
                        frame_index.min(frame_count.saturating_sub(1))
                    };
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

            (rendered, safe_frames)
        };

        for (index, viewport) in self.mammo_group.iter_mut().enumerate() {
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
        let mut rendered_views = Vec::new();
        for viewport in group {
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
        if group.is_empty() {
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

        if self.mammo_group.is_empty() {
            return None;
        }

        let paths = self
            .mammo_group
            .iter()
            .map(|viewport| viewport.path.clone())
            .collect::<Vec<_>>();
        Some(history_id_from_paths(&paths))
    }

    fn sync_current_state_to_history(&mut self) {
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
                if self.mammo_group.is_empty() {
                    return;
                }
                group.selected_index = self
                    .mammo_selected_index
                    .min(self.mammo_group.len().saturating_sub(1));
                for cached_viewport in &mut group.viewports {
                    if let Some(active_viewport) = self
                        .mammo_group
                        .iter()
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
                self.clear_single_viewer();
                self.mammo_group = group
                    .viewports
                    .into_iter()
                    .map(|viewport| MammoViewport {
                        path: viewport.path,
                        image: viewport.image,
                        texture: viewport.texture,
                        label: viewport.label,
                        window_center: viewport.window_center,
                        window_width: viewport.window_width,
                        current_frame: viewport.current_frame,
                    })
                    .collect();
                if self.mammo_group.is_empty() {
                    self.status_line = "History entry had no cached mammo images.".to_string();
                    return;
                }
                self.mammo_selected_index = group
                    .selected_index
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
        let mut preload_order = (0..groups.len())
            .filter(|index| *index != active_group)
            .collect::<Vec<_>>();
        preload_order.reverse();

        for index in preload_order {
            self.load_selected_paths(groups[index].clone(), ctx);
        }
        self.load_selected_paths(groups[active_group].clone(), ctx);
    }

    fn start_dicomweb_download(&mut self, request: DicomWebLaunchRequest) {
        if self.dicomweb_receiver.is_some() {
            self.status_line = "DICOMweb download already in progress.".to_string();
            return;
        }

        self.sync_current_state_to_history();
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
        self.status_line = "Loading grouped study from DICOMweb...".to_string();
        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        thread::spawn(move || {
            let result =
                download_dicomweb_group_request(&request).map_err(|err| format!("{err:#}"));
            let _ = tx.send(result);
        });
        self.dicomweb_receiver = Some(rx);
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
                        if self.status_line.is_empty() {
                            self.status_line = "Loaded study from DICOMweb.".to_string();
                        }
                    }
                    DicomWebDownloadResult::Grouped { groups, open_group } => {
                        self.load_local_groups(groups, open_group, ctx);
                        if self.status_line.is_empty() {
                            self.status_line = "Loaded grouped study from DICOMweb.".to_string();
                        }
                    }
                },
                Err(err) => {
                    self.status_line = format!("DICOMweb error: {err}");
                }
            },
            Err(TryRecvError::Empty) => {
                self.dicomweb_receiver = Some(receiver);
                ctx.request_repaint_after(Duration::from_millis(16));
            }
            Err(TryRecvError::Disconnected) => {
                self.status_line = "DICOMweb download worker disconnected.".to_string();
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

    fn load_selected_paths(&mut self, paths: Vec<PathBuf>, ctx: &egui::Context) {
        if !paths.is_empty() {
            self.sync_current_state_to_history();
        }

        match paths.len() {
            0 => {}
            1 => {
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

        let mut loaded = Vec::with_capacity(4);
        for path in paths {
            match load_dicom(&path) {
                Ok(image) => {
                    let default_center = image.window_center;
                    let default_width = image.window_width;
                    let color_image =
                        match Self::render_image_frame(&image, 0, default_center, default_width) {
                            Some(color_image) => color_image,
                            None => {
                                self.status_line = format!(
                                    "Could not prepare preview for {} (no decodable frame).",
                                    path.display()
                                );
                                return;
                            }
                        };

                    let texture_name = format!("mammo-group:{}", path.display());
                    let texture =
                        ctx.load_texture(texture_name, color_image, TextureOptions::LINEAR);
                    let label = mammo_label(&image, &path);
                    loaded.push(MammoViewport {
                        path,
                        image,
                        texture,
                        label,
                        window_center: default_center,
                        window_width: default_width,
                        current_frame: 0,
                    });
                }
                Err(err) => {
                    self.status_line = format!("Error opening {}: {err:#}", path.display());
                    return;
                }
            }
        }

        loaded.sort_by(|a, b| {
            mammo_sort_key(&a.image, &a.path).cmp(&mammo_sort_key(&b.image, &b.path))
        });
        self.push_group_history_entry(&loaded, 0, ctx);

        self.clear_single_viewer();
        self.mammo_group = loaded;
        self.mammo_selected_index = 0;
        self.status_line.clear();
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

        if self.mammo_group.is_empty() {
            self.cine_mode = false;
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
        if self.mammo_group.is_empty() {
            return None;
        }
        let selected = self
            .mammo_selected_index
            .min(self.mammo_group.len().saturating_sub(1));
        self.mammo_group.get(selected)
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
        if self.mammo_group.is_empty() {
            return None;
        }
        let selected = self
            .mammo_selected_index
            .min(self.mammo_group.len().saturating_sub(1));
        self.mammo_group.get_mut(selected)
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

            for row in 0..2 {
                ui.horizontal(|ui| {
                    for col in 0..2 {
                        let index = row * 2 + col;
                        ui.allocate_ui_with_layout(
                            cell_size,
                            egui::Layout::top_down(egui::Align::Center),
                            |ui| {
                                if let Some(viewport) = self.mammo_group.get(index) {
                                    let stroke_color = if index == self.mammo_selected_index {
                                        egui::Color32::from_rgb(90, 140, 220)
                                    } else {
                                        egui::Color32::BLACK
                                    };
                                    let frame = egui::Frame::none()
                                        .stroke(egui::Stroke::new(1.0, stroke_color))
                                        .inner_margin(egui::Margin::same(MAMMO_VIEW_INNER_MARGIN));
                                    frame.show(ui, |ui| {
                                        let remaining = ui.available_size();
                                        let texture_size = viewport.texture.size_vec2();
                                        let scale = (remaining.x / texture_size.x)
                                            .min(remaining.y / texture_size.y)
                                            .max(0.01);
                                        let draw_size = texture_size * scale;
                                        let image_align = mammo_image_align(
                                            &viewport.image,
                                            viewport.label.as_str(),
                                        );

                                        ui.allocate_ui_with_layout(
                                            remaining,
                                            egui::Layout::top_down(image_align),
                                            |ui| {
                                                let top_padding =
                                                    ((remaining.y - draw_size.y) * 0.5).max(0.0);
                                                if top_padding > 0.0 {
                                                    ui.add_space(top_padding);
                                                }
                                                let response = ui.add(
                                                    egui::Image::new((
                                                        viewport.texture.id(),
                                                        draw_size,
                                                    ))
                                                    .sense(Sense::click()),
                                                );
                                                if response.clicked() {
                                                    clicked_index = Some(index);
                                                }
                                            },
                                        );
                                    });
                                }
                            },
                        );
                    }
                });
            }

            if let Some(index) = clicked_index {
                self.mammo_selected_index = index;
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

        if ctx.input(|input| input.key_pressed(egui::Key::C)) {
            self.toggle_cine_mode();
        }

        if let Some(request) = self.pending_launch_request.take() {
            self.handle_launch_request(request, ctx);
        }

        self.poll_dicomweb_download(ctx);
        self.advance_cine_if_needed(ctx);

        let mut history_cycle_direction = None;
        let mut close_requested = false;
        ctx.input_mut(|input| {
            if input.consume_key(egui::Modifiers::COMMAND, egui::Key::W) {
                close_requested = true;
            } else if input.consume_key(egui::Modifiers::SHIFT, egui::Key::Tab) {
                history_cycle_direction = Some(-1);
            } else if input.consume_key(egui::Modifiers::NONE, egui::Key::Tab) {
                history_cycle_direction = Some(1);
            }
        });
        if close_requested {
            ctx.send_viewport_cmd(ViewportCommand::Close);
            return;
        }
        if let Some(direction) = history_cycle_direction {
            self.cycle_history_entry(direction);
        }
        let history_transition_pending =
            history_cycle_direction.is_some() || self.pending_history_open_index.is_some();

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

        let has_mammo_group = !self.mammo_group.is_empty();

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
                                            .add_enabled(
                                                !history_transition_pending,
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
                                            .add_enabled(
                                                !history_transition_pending,
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
                                            .add_enabled(
                                                !history_transition_pending,
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
        if request_rebuild && !history_transition_pending {
            if let Some(state) = active_state.as_ref() {
                self.apply_active_viewport_state(state, ctx);
            }
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            if has_mammo_group {
                self.show_mammo_grid(ui);
            } else if let Some(texture) = self.texture.as_ref() {
                let available = ui.available_size();
                let image_size = texture.size_vec2();
                let scale = (available.x / image_size.x)
                    .min(available.y / image_size.y)
                    .max(0.1);
                let draw_size = image_size * scale;

                ui.vertical_centered(|ui| {
                    ui.image((texture.id(), draw_size));
                });
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

fn normalize_token(value: Option<&str>) -> String {
    value
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase()
        .replace(' ', "")
}

fn classify_laterality(value: Option<&str>) -> Option<&'static str> {
    let token = normalize_token(value);
    if token.starts_with('R') || token.contains("RIGHT") {
        Some("R")
    } else if token.starts_with('L') || token.contains("LEFT") {
        Some("L")
    } else {
        None
    }
}

fn classify_view(value: Option<&str>) -> Option<&'static str> {
    let token = normalize_token(value);
    if token.contains("MLO") {
        Some("MLO")
    } else if token.contains("CC") {
        Some("CC")
    } else {
        None
    }
}

fn mammo_image_align(image: &DicomImage, label: &str) -> egui::Align {
    if let Some(laterality) = classify_laterality(image.image_laterality.as_deref()) {
        return if laterality == "R" {
            egui::Align::Max
        } else {
            egui::Align::Min
        };
    }

    let label_token = normalize_token(Some(label));
    if label_token.starts_with('R') {
        egui::Align::Max
    } else if label_token.starts_with('L') {
        egui::Align::Min
    } else {
        egui::Align::Center
    }
}

fn mammo_sort_key(image: &DicomImage, path: &Path) -> (u8, u8, i32, String) {
    let view_rank = match classify_view(image.view_position.as_deref()) {
        Some("CC") => 0,
        Some("MLO") => 1,
        _ => 2,
    };
    let laterality_rank = match classify_laterality(image.image_laterality.as_deref()) {
        Some("R") => 0,
        Some("L") => 1,
        _ => 2,
    };

    let instance_number = image.instance_number.unwrap_or(i32::MAX);
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_string();

    (view_rank, laterality_rank, instance_number, file_name)
}

fn mammo_label(image: &DicomImage, path: &Path) -> String {
    let laterality = classify_laterality(image.image_laterality.as_deref());
    let view = classify_view(image.view_position.as_deref());
    let code = match (laterality, view) {
        (Some(laterality), Some(view)) => format!("{laterality}{view}"),
        (Some(laterality), None) => laterality.to_string(),
        (None, Some(view)) => view.to_string(),
        _ => String::new(),
    };

    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("DICOM");

    if code.is_empty() {
        file_name.to_string()
    } else {
        format!("{code} ({file_name})")
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

    for (index, image) in images.iter().enumerate() {
        let source_width = image.size[0].max(1);
        let source_height = image.size[1].max(1);

        let scale = (cell_width as f32 / source_width as f32)
            .min(cell_height as f32 / source_height as f32);
        let draw_width = ((source_width as f32 * scale).round() as usize).clamp(1, cell_width);
        let draw_height = ((source_height as f32 * scale).round() as usize).clamp(1, cell_height);

        let col = index % columns;
        let row = index / columns;
        let base_x = col * cell_width + (cell_width - draw_width) / 2;
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
}
