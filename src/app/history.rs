use super::*;

pub(super) enum HistoryPreloadResult {
    Single {
        path: DicomSource,
        image: Box<DicomImage>,
    },
    Group {
        viewports: Vec<(DicomSource, DicomImage)>,
    },
    Report {
        path: DicomSource,
        report: Box<StructuredReportDocument>,
    },
}

pub(super) enum HistoryPreloadJob {
    Group(PreparedLoadPaths),
    ParametricMap(DicomSource),
    StructuredReport(DicomSource),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum HistoryPreloadJobKey {
    Group(String),
    ParametricMap(String),
    StructuredReport(String),
}

impl HistoryPreloadJob {
    fn preload_key(&self) -> HistoryPreloadJobKey {
        match self {
            Self::Group(prepared) => {
                HistoryPreloadJobKey::Group(history_preload_group_id(prepared))
            }
            Self::ParametricMap(path) => {
                HistoryPreloadJobKey::ParametricMap(history_preload_source_key(path))
            }
            Self::StructuredReport(path) => {
                HistoryPreloadJobKey::StructuredReport(history_preload_source_key(path))
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct HistorySingleData {
    pub(super) path: DicomSourceMeta,
    pub(super) image: DicomImage,
    pub(super) texture: TextureHandle,
    pub(super) window_center: f32,
    pub(super) window_width: f32,
    pub(super) current_frame: usize,
    pub(super) cine_fps: f32,
}

#[derive(Clone)]
pub(super) struct HistoryGroupViewportData {
    pub(super) path: DicomSourceMeta,
    pub(super) image: DicomImage,
    pub(super) texture: TextureHandle,
    pub(super) label: String,
    pub(super) window_center: f32,
    pub(super) window_width: f32,
    pub(super) current_frame: usize,
}

#[derive(Clone)]
pub(super) struct HistoryGroupData {
    pub(super) viewports: Vec<HistoryGroupViewportData>,
    pub(super) selected_index: usize,
}

#[derive(Clone)]
pub(super) struct HistoryReportData {
    pub(super) path: DicomSourceMeta,
    pub(super) report: StructuredReportDocument,
}

#[derive(Clone)]
pub(super) enum HistoryKind {
    Single(Box<HistorySingleData>),
    Group(HistoryGroupData),
    Report(Box<HistoryReportData>),
}

pub(super) struct HistoryThumb {
    pub(super) texture: TextureHandle,
}

pub(super) struct HistoryEntry {
    pub(super) id: String,
    pub(super) kind: HistoryKind,
    pub(super) thumbs: Vec<HistoryThumb>,
}

impl DicomViewerApp {
    fn next_history_texture_name(&mut self, prefix: &str) -> String {
        self.history_nonce = self.history_nonce.saturating_add(1);
        format!("history-{prefix}-{}", self.history_nonce)
    }

    pub(super) fn source_texture_name(prefix: &str, source: &DicomSourceMeta) -> String {
        format!("{prefix}:{}", source.identity_key())
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
        let rendered =
            Self::render_image_frame(image, safe_frame, window_center, window_width, false)?;
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
                false,
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

    pub(super) fn push_single_history_entry(
        &mut self,
        single: HistorySingleData,
        ctx: &egui::Context,
    ) {
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

    pub(super) fn push_group_history_entry(
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

    pub(super) fn push_report_history_entry(
        &mut self,
        path: DicomSourceMeta,
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

    pub(super) fn current_history_id(&self) -> Option<String> {
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

    pub(super) fn has_open_study(&self) -> bool {
        self.image.is_some()
            || self.report.is_some()
            || self.current_single_path.is_some()
            || self.has_mammo_group()
            || self.single_load_receiver.is_some()
            || self.dicomweb_receiver.is_some()
            || self.dicomweb_active_path_receiver.is_some()
            || !self.dicomweb_active_pending_paths.is_empty()
    }

    pub(super) fn clear_active_study(&mut self) {
        self.pending_launch_request = None;
        self.pending_local_open_paths = None;
        self.pending_local_open_armed = false;
        self.pending_history_open_id = None;
        self.pending_history_open_armed = false;
        self.dicomweb_receiver = None;
        self.dicomweb_active_path_receiver = None;
        self.dicomweb_active_group_expected = None;
        self.dicomweb_active_group_paths.clear();
        self.dicomweb_completed_background_groups.clear();
        self.dicomweb_active_pending_paths.clear();
        self.single_load_receiver = None;
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.clear_history_preload();
        self.history_pushed_for_active_group = false;
        self.pending_gsps_overlays.clear();
        self.authoritative_gsps_overlay_keys.clear();
        self.pending_sr_overlays.clear();
        self.authoritative_sr_overlay_keys.clear();
        self.pending_pm_overlays.clear();
        self.authoritative_pm_overlay_keys.clear();
        self.clear_single_viewer();
        self.mammo_group.clear();
        self.mammo_selected_index = 0;
        self.clear_load_error();
    }

    pub(super) fn close_current_group(&mut self, ctx: &egui::Context) {
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

    pub(super) fn handle_close_group_shortcut(&mut self, ctx: &egui::Context) -> bool {
        if self.has_open_study() {
            self.close_current_group(ctx);
            false
        } else {
            true
        }
    }

    pub(super) fn move_current_history_to_front(&mut self) {
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

    pub(super) fn clear_history_preload(&mut self) {
        self.history_preload_receiver = None;
        self.history_preload_queue.clear();
        self.history_preload_active_key = None;
    }

    fn preload_report_into_history(
        path: DicomSource,
        tx: &mpsc::Sender<Result<HistoryPreloadResult, String>>,
    ) {
        let result = load_structured_report(&path)
            .map(|report| HistoryPreloadResult::Report {
                path: path.clone(),
                report: Box::new(report),
            })
            .map_err(|err| format!("{err:#}"));
        let _ = tx.send(result);
    }

    fn preload_parametric_map_into_history(
        path: DicomSource,
        tx: &mpsc::Sender<Result<HistoryPreloadResult, String>>,
    ) {
        let result = load_parametric_map(&path)
            .map(|image| HistoryPreloadResult::Single {
                path: path.clone(),
                image: Box::new(image),
            })
            .map_err(|err| format!("{err:#}"));
        let _ = tx.send(result);
    }

    fn preload_group_into_history(
        prepared: PreparedLoadPaths,
        tx: &mpsc::Sender<Result<HistoryPreloadResult, String>>,
    ) {
        let load_paths = prepared.image_paths;
        let report_paths = prepared.structured_report_paths;
        let parametric_map_paths = prepared.parametric_map_paths;
        let gsps_overlays = prepared.gsps_overlays;
        let sr_overlays = prepared.sr_overlays;
        let pm_overlays = prepared.pm_overlays;

        if load_paths.is_empty() {
            if report_paths.is_empty() && parametric_map_paths.is_empty() {
                let _ = tx.send(Err("Unsupported preload group size".to_string()));
                return;
            }
            for path in report_paths {
                Self::preload_report_into_history(path, tx);
            }
            for path in parametric_map_paths {
                Self::preload_parametric_map_into_history(path, tx);
            }
            return;
        }

        for path in report_paths {
            Self::preload_report_into_history(path, tx);
        }
        for path in parametric_map_paths {
            Self::preload_parametric_map_into_history(path, tx);
        }

        let result = match load_paths.len() {
            1 => {
                let path = load_paths[0].clone();
                load_dicom(&path)
                    .map(|mut image| {
                        Self::attach_matching_gsps_overlay(&mut image, &gsps_overlays);
                        Self::attach_matching_sr_overlay(&mut image, &sr_overlays);
                        Self::attach_matching_pm_overlay(&mut image, &pm_overlays);
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
                    Self::attach_matching_sr_overlay(&mut image, &sr_overlays);
                    Self::attach_matching_pm_overlay(&mut image, &pm_overlays);
                    viewports.push((path.clone(), image));
                }
                Ok(HistoryPreloadResult::Group { viewports })
            }
            _ => Err("Unsupported preload group size".to_string()),
        };
        let _ = tx.send(result);
    }

    pub(super) fn start_next_history_preload(&mut self, ctx: &egui::Context) {
        if self.history_preload_receiver.is_some() {
            return;
        }

        let Some(job) = self.history_preload_queue.pop_front() else {
            return;
        };
        let job_key = job.preload_key();

        let (tx, rx) = mpsc::channel::<Result<HistoryPreloadResult, String>>();
        thread::spawn(move || match job {
            HistoryPreloadJob::Group(prepared) => Self::preload_group_into_history(prepared, &tx),
            HistoryPreloadJob::ParametricMap(path) => {
                Self::preload_parametric_map_into_history(path, &tx);
            }
            HistoryPreloadJob::StructuredReport(path) => {
                Self::preload_report_into_history(path, &tx);
            }
        });
        self.history_preload_receiver = Some(rx);
        self.history_preload_active_key = Some(job_key);
        ctx.request_repaint_after(Duration::from_millis(16));
    }

    pub(super) fn enqueue_history_preload_job(
        &mut self,
        job: HistoryPreloadJob,
        ctx: &egui::Context,
    ) {
        let job_key = job.preload_key();
        if self.history_preload_active_key.as_ref() == Some(&job_key)
            || self
                .history_preload_queue
                .iter()
                .any(|queued| queued.preload_key() == job_key)
        {
            return;
        }

        self.history_preload_queue.push_back(job);
        self.start_next_history_preload(ctx);
    }

    pub(super) fn preload_non_active_groups_into_history(
        &mut self,
        groups: &[PreparedLoadPaths],
        open_group: usize,
        completed_background_groups: Option<&HashSet<usize>>,
        ctx: &egui::Context,
    ) {
        let queued_groups = groups
            .iter()
            .enumerate()
            .rev()
            .filter(|(index, _)| {
                *index != open_group
                    && completed_background_groups
                        .map(|completed_groups| !completed_groups.contains(index))
                        .unwrap_or(true)
            })
            .map(|(_, group)| group.clone())
            .collect::<Vec<_>>();

        for group in queued_groups {
            self.enqueue_history_preload_job(HistoryPreloadJob::Group(group), ctx);
        }
    }

    pub(super) fn sync_current_state_to_history(&mut self) {
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
                if let Some(image) = self.image.as_ref() {
                    single.image.gsps_overlay = image.gsps_overlay.clone();
                    single.image.sr_overlay = image.sr_overlay.clone();
                    single.image.pm_overlay = image.pm_overlay.clone();
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
                        cached_viewport.image.gsps_overlay =
                            active_viewport.image.gsps_overlay.clone();
                        cached_viewport.image.sr_overlay = active_viewport.image.sr_overlay.clone();
                        cached_viewport.image.pm_overlay = active_viewport.image.pm_overlay.clone();
                        cached_viewport.texture = active_viewport.texture.clone();
                        cached_viewport.window_center = active_viewport.window_center;
                        cached_viewport.window_width = active_viewport.window_width;
                        cached_viewport.current_frame = active_viewport.current_frame;
                    }
                    Self::attach_matching_gsps_overlay(
                        &mut cached_viewport.image,
                        &self.pending_gsps_overlays,
                    );
                    Self::attach_matching_sr_overlay(
                        &mut cached_viewport.image,
                        &self.pending_sr_overlays,
                    );
                    Self::attach_matching_pm_overlay(
                        &mut cached_viewport.image,
                        &self.pending_pm_overlays,
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

    pub(super) fn open_history_entry(&mut self, index: usize, ctx: &egui::Context) {
        self.sync_current_state_to_history();
        self.single_load_receiver = None;
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.reset_live_measurement();

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
                self.overlay_visible = false;
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
                self.attach_pending_overlays_to_current_study();
                self.sync_current_state_to_history();
                self.clear_load_error();
                self.rebuild_texture(ctx);
                log::info!("Loaded study from memory cache.");
                ctx.request_repaint();
            }
            HistoryKind::Group(group) => {
                self.clear_single_viewer();
                self.overlay_visible = false;
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
                self.attach_pending_overlays_to_current_study();
                self.refresh_active_textures(ctx);
                self.sync_current_state_to_history();
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

    pub(super) fn cycle_history_entry(&mut self, direction: i32) {
        let len = self.history_entries.len();
        if len <= 1 {
            return;
        }

        let current_index = self
            .pending_history_open_id
            .as_deref()
            .and_then(|id| self.history_entries.iter().position(|entry| entry.id == id))
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

    pub(super) fn poll_history_preload(&mut self, ctx: &egui::Context) {
        self.start_next_history_preload(ctx);

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
                        let Some(color_image) =
                            Self::render_image_frame(&image, 0, center, width, false)
                        else {
                            break;
                        };
                        let path_meta = DicomSourceMeta::from(&path);
                        let texture_name =
                            Self::source_texture_name("history-preload-single", &path_meta);
                        let texture =
                            ctx.load_texture(texture_name, color_image, TextureOptions::LINEAR);
                        self.push_single_history_entry(
                            HistorySingleData {
                                path: path_meta,
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
                        let mut render_failed = false;
                        for (path, image) in viewports {
                            let center = image.window_center;
                            let width = image.window_width;
                            let Some(color_image) =
                                Self::render_image_frame(&image, 0, center, width, false)
                            else {
                                log::warn!(
                                    "History preload skipped group viewport (instance {:?}).",
                                    image.instance_number
                                );
                                render_failed = true;
                                break;
                            };
                            let path_meta = DicomSourceMeta::from(&path);
                            let texture_name =
                                Self::source_texture_name("history-preload-group", &path_meta);
                            let texture =
                                ctx.load_texture(texture_name, color_image, TextureOptions::LINEAR);
                            let label = mammo_label(&image, &path_meta);
                            loaded.push(MammoViewport {
                                path: path_meta,
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
                        if !render_failed && Self::is_supported_multi_view_group_size(loaded.len())
                        {
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
                        self.push_report_history_entry((&path).into(), *report, ctx);
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
            return;
        }

        self.history_preload_active_key = None;
        self.start_next_history_preload(ctx);
    }

    pub(super) fn show_history_list(
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
}

fn history_preload_source_key(path: &DicomSource) -> String {
    DicomSourceMeta::from(path).identity_key().to_string()
}

fn history_preload_group_id(prepared: &PreparedLoadPaths) -> String {
    let mut paths = Vec::with_capacity(
        prepared.image_paths.len()
            + prepared.structured_report_paths.len()
            + prepared.parametric_map_paths.len(),
    );
    paths.extend(prepared.image_paths.iter().cloned());
    paths.extend(prepared.structured_report_paths.iter().cloned());
    paths.extend(prepared.parametric_map_paths.iter().cloned());
    history_id_from_paths(&paths)
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

pub(super) fn history_id_from_paths<T>(paths: &[T]) -> String
where
    T: Clone + Into<DicomSourceMeta>,
{
    let mut normalized = paths
        .iter()
        .cloned()
        .map(Into::into)
        .map(|path: DicomSourceMeta| path.identity_key().to_string())
        .collect::<Vec<_>>();
    normalized.sort();

    let mut history_id = format!("{}:", normalized.len());
    for identity in normalized {
        history_id.push_str(&format!("{}:{}", identity.len(), identity));
    }
    history_id
}
