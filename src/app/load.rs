use super::*;

pub(super) struct PendingLoad {
    pub(super) path: DicomSource,
    pub(super) image: DicomImage,
}

struct PreparedImagePath {
    path: DicomSource,
    sop_instance_uid: Option<String>,
}

struct PreparedParametricMapPath {
    path: DicomSource,
    overlays: HashMap<String, ParametricMapOverlay>,
}

struct PendingOverlayState {
    clear_history_preload: bool,
    pending_gsps_overlays: HashMap<String, GspsOverlay>,
    authoritative_gsps_overlay_keys: HashSet<String>,
    pending_sr_overlays: HashMap<String, SrOverlay>,
    authoritative_sr_overlay_keys: HashSet<String>,
    pending_pm_overlays: HashMap<String, ParametricMapOverlay>,
    authoritative_pm_overlay_keys: HashSet<String>,
    overlay_visible: bool,
    attach_to_current_study: bool,
}

pub(super) enum PendingSingleLoad {
    Image(Box<PendingLoad>),
    StructuredReport {
        path: DicomSource,
        report: Box<StructuredReportDocument>,
    },
}

#[derive(Default, Clone)]
pub(super) struct PreparedLoadPaths {
    pub(super) image_paths: Vec<DicomSource>,
    pub(super) structured_report_paths: Vec<DicomSource>,
    pub(super) parametric_map_paths: Vec<DicomSource>,
    pub(super) gsps_overlays: HashMap<String, GspsOverlay>,
    pub(super) sr_overlays: HashMap<String, SrOverlay>,
    pub(super) pm_overlays: HashMap<String, ParametricMapOverlay>,
    pub(super) gsps_files_found: usize,
    pub(super) other_files_found: usize,
}

impl DicomViewerApp {
    fn commit_pending_overlay_state(&mut self, pending_overlay_state: PendingOverlayState) {
        if pending_overlay_state.clear_history_preload {
            self.clear_history_preload();
        }

        self.pending_gsps_overlays = pending_overlay_state.pending_gsps_overlays;
        self.authoritative_gsps_overlay_keys =
            pending_overlay_state.authoritative_gsps_overlay_keys;
        self.pending_sr_overlays = pending_overlay_state.pending_sr_overlays;
        self.authoritative_sr_overlay_keys = pending_overlay_state.authoritative_sr_overlay_keys;
        self.pending_pm_overlays = pending_overlay_state.pending_pm_overlays;
        self.authoritative_pm_overlay_keys = pending_overlay_state.authoritative_pm_overlay_keys;
        self.overlay_visible = pending_overlay_state.overlay_visible;

        if pending_overlay_state.attach_to_current_study {
            self.attach_pending_overlays_to_current_study();
            self.sync_current_state_to_history();
        }
    }

    pub(super) fn prepare_load_paths<T>(paths: Vec<T>) -> PreparedLoadPaths
    where
        T: Into<DicomSource>,
    {
        let mut prepared = PreparedLoadPaths::default();
        let mut prepared_images = Vec::<PreparedImagePath>::new();
        let mut prepared_parametric_maps = Vec::<PreparedParametricMapPath>::new();

        for path in paths {
            let path = path.into();
            match classify_dicom_path(&path) {
                Ok(DicomPathKind::Gsps) => {
                    prepared.gsps_files_found = prepared.gsps_files_found.saturating_add(1);
                    match load_gsps_overlays(&path) {
                        Ok(overlays) => {
                            Self::merge_gsps_overlays(&mut prepared.gsps_overlays, &overlays)
                        }
                        Err(err) => {
                            log::warn!("Could not parse GSPS input: {err:#}");
                        }
                    }
                }
                Ok(DicomPathKind::StructuredReport) => {
                    match load_mammography_cad_sr_overlays(&path) {
                        Ok(overlays) => {
                            Self::merge_sr_overlays(&mut prepared.sr_overlays, &overlays)
                        }
                        Err(err) => {
                            log::warn!("Could not parse Mammography CAD SR overlay input: {err:#}");
                        }
                    }
                    prepared.structured_report_paths.push(path);
                }
                Ok(DicomPathKind::ParametricMap) => match load_parametric_map_overlays(&path) {
                    Ok(overlays) => {
                        prepared_parametric_maps.push(PreparedParametricMapPath { path, overlays })
                    }
                    Err(err) => {
                        log::warn!("Could not parse Parametric Map overlay input: {err:#}");
                        prepared.parametric_map_paths.push(path);
                    }
                },
                Ok(DicomPathKind::Image) | Err(_) => {
                    let sop_instance_uid = match read_sop_instance_uid(&path) {
                        Ok(uid) => uid,
                        Err(err) => {
                            log::warn!("Could not inspect image SOP Instance UID: {err:#}");
                            None
                        }
                    };
                    prepared_images.push(PreparedImagePath {
                        path,
                        sop_instance_uid,
                    });
                }
                Ok(DicomPathKind::Other) => {
                    prepared.other_files_found = prepared.other_files_found.saturating_add(1);
                }
            }
        }

        let selected_image_uids = prepared_images
            .iter()
            .filter_map(|image| image.sop_instance_uid.clone())
            .collect::<HashSet<_>>();
        prepared.image_paths = prepared_images
            .into_iter()
            .map(|image| image.path)
            .collect::<Vec<_>>();

        for prepared_map in prepared_parametric_maps {
            let matched_overlays = prepared_map
                .overlays
                .into_iter()
                .filter(|(sop_uid, overlay)| {
                    selected_image_uids.contains(sop_uid) && !overlay.is_empty()
                })
                .collect::<HashMap<_, _>>();
            if matched_overlays.is_empty() {
                prepared.parametric_map_paths.push(prepared_map.path);
            } else {
                Self::merge_pm_overlays(&mut prepared.pm_overlays, &matched_overlays);
            }
        }

        prepared
    }

    pub(super) fn handle_launch_request(&mut self, request: LaunchRequest, ctx: &egui::Context) {
        match request {
            LaunchRequest::LocalPaths(paths) => self.queue_local_paths_open(paths),
            LaunchRequest::LocalGroups { groups, open_group } => {
                self.load_local_groups(groups, open_group, ctx)
            }
            LaunchRequest::DicomWebGroups(request) => self.start_dicomweb_group_download(request),
            LaunchRequest::DicomWeb(request) => self.start_dicomweb_download(request),
        }
    }

    pub(super) fn load_local_groups<T>(
        &mut self,
        groups: Vec<Vec<T>>,
        open_group: usize,
        ctx: &egui::Context,
    ) where
        T: Clone + Into<DicomSource>,
    {
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
        self.preload_non_active_groups_into_history(&preload_groups, active_group, None, ctx);
    }

    pub(super) fn start_dicomweb_download(&mut self, request: DicomWebLaunchRequest) {
        if self.dicomweb_receiver.is_some() {
            log::warn!("DICOMweb download already in progress.");
            return;
        }

        self.clear_load_error();
        self.sync_current_state_to_history();
        self.clear_history_preload();
        self.single_load_receiver = None;
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.history_pushed_for_active_group = false;
        self.pending_gsps_overlays.clear();
        self.authoritative_gsps_overlay_keys.clear();
        self.pending_sr_overlays.clear();
        self.authoritative_sr_overlay_keys.clear();
        self.pending_pm_overlays.clear();
        self.authoritative_pm_overlay_keys.clear();
        self.overlay_visible = false;
        self.dicomweb_active_path_receiver = None;
        self.dicomweb_active_group_expected = None;
        self.dicomweb_active_group_paths.clear();
        self.dicomweb_completed_background_groups.clear();
        self.dicomweb_active_pending_paths.clear();
        log::info!("Loading study from DICOMweb...");
        let (tx, rx) = mpsc::channel::<Result<DicomWebDownloadResult, String>>();
        thread::spawn(move || {
            let result = download_dicomweb_request(&request).map_err(|err| format!("{err:#}"));
            let _ = tx.send(result);
        });
        self.dicomweb_receiver = Some(rx);
    }

    pub(super) fn start_dicomweb_group_download(&mut self, request: DicomWebGroupedLaunchRequest) {
        if self.dicomweb_receiver.is_some() {
            log::warn!("DICOMweb download already in progress.");
            return;
        }

        self.clear_load_error();
        self.sync_current_state_to_history();
        self.clear_history_preload();
        self.single_load_receiver = None;
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.history_pushed_for_active_group = false;
        self.pending_gsps_overlays.clear();
        self.authoritative_gsps_overlay_keys.clear();
        self.pending_sr_overlays.clear();
        self.authoritative_sr_overlay_keys.clear();
        self.pending_pm_overlays.clear();
        self.authoritative_pm_overlay_keys.clear();
        self.overlay_visible = false;
        log::info!("Loading grouped study from DICOMweb...");
        self.dicomweb_active_group_expected = None;
        self.dicomweb_active_group_paths.clear();
        self.dicomweb_completed_background_groups.clear();
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

    pub(super) fn insert_loaded_mammo(
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
        Self::attach_matching_sr_overlay(&mut pending.image, &self.pending_sr_overlays);
        Self::attach_matching_pm_overlay(&mut pending.image, &self.pending_pm_overlays);

        let default_center = pending.image.window_center;
        let default_width = pending.image.window_width;
        let Some(color_image) =
            Self::render_image_frame(&pending.image, 0, default_center, default_width, false)
        else {
            return Err("Could not prepare preview for image (no decodable frame).".to_string());
        };

        let path_meta = DicomSourceMeta::from(&pending.path);
        let texture_name = Self::source_texture_name("mammo-group", &path_meta);
        let texture = ctx.load_texture(texture_name, color_image, TextureOptions::LINEAR);
        let label = mammo_label(&pending.image, &path_meta);
        self.mammo_group[slot_index] = Some(MammoViewport {
            path: path_meta,
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

    pub(super) fn reorder_complete_mammo_group(&mut self) {
        self.clear_live_measurement();
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

    pub(super) fn poll_dicomweb_active_paths(&mut self, ctx: &egui::Context) {
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
                    Ok(DicomWebGroupStreamUpdate::BackgroundGroupReady { group_index, paths }) => {
                        if self
                            .dicomweb_completed_background_groups
                            .insert(group_index)
                        {
                            let prepared = Self::prepare_load_paths(paths);
                            if Self::is_supported_prepared_group(&prepared) {
                                self.enqueue_history_preload_job(
                                    HistoryPreloadJob::Group(prepared),
                                    ctx,
                                );
                            } else {
                                log::warn!(
                                    "Ignoring streamed background group {} with unsupported content.",
                                    group_index
                                );
                            }
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
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
        for _ in 0..DICOMWEB_ACTIVE_PENDING_BATCH_SIZE {
            let Some(path) = self.dicomweb_active_pending_paths.pop_front() else {
                break;
            };
            match classify_dicom_path(&path) {
                Ok(DicomPathKind::Gsps) => match load_gsps_overlays(&path) {
                    Ok(overlays) => {
                        self.merge_pending_gsps_overlays(overlays);
                    }
                    Err(err) => {
                        log::warn!("Could not parse streamed GSPS input: {err:#}");
                    }
                },
                Ok(DicomPathKind::StructuredReport) => {
                    match load_mammography_cad_sr_overlays(&path) {
                        Ok(overlays) => {
                            self.merge_pending_sr_overlays(overlays);
                        }
                        Err(err) => {
                            log::warn!(
                                "Could not parse streamed Mammography CAD SR overlay input: {err:#}"
                            );
                        }
                    }
                    self.enqueue_history_preload_job(
                        HistoryPreloadJob::StructuredReport(path),
                        ctx,
                    );
                }
                Ok(DicomPathKind::ParametricMap) => match load_parametric_map_overlays(&path) {
                    Ok(overlays) => {
                        self.merge_pending_pm_overlays(overlays);
                    }
                    Err(err) => {
                        log::warn!(
                            "Could not parse streamed Parametric Map overlay input: {err:#}"
                        );
                    }
                },
                Ok(DicomPathKind::Image) | Err(_) => match expected {
                    1 => {
                        self.dicomweb_active_group_paths.push((&path).into());
                        let active_group_is_displayed = self.displayed_study_matches_paths(
                            self.dicomweb_active_group_paths.as_slice(),
                        );
                        let another_study_is_displayed = self.current_single_path.is_some()
                            || self.report.is_some()
                            || self.mammo_group_complete();
                        if another_study_is_displayed && !active_group_is_displayed {
                            log::info!(
                                "Skipping streamed single-view activation because another study is open."
                            );
                        } else {
                            let _ = self.load_selected_paths(vec![path], ctx);
                        }
                    }
                    count if Self::is_supported_multi_view_group_size(count) => {
                        self.dicomweb_active_group_paths.push((&path).into());
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
                            let active_group_is_displayed = self.displayed_study_matches_paths(
                                self.dicomweb_active_group_paths.as_slice(),
                            );
                            let another_study_is_displayed = self.current_single_path.is_some()
                                || self.report.is_some()
                                || self.mammo_group_complete();
                            if another_study_is_displayed && !active_group_is_displayed {
                                log::info!(
                                    "Skipping streamed multi-view decode because another study is open."
                                );
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
                                self.dicomweb_completed_background_groups.clear();
                                self.dicomweb_active_pending_paths.clear();
                                self.dicomweb_active_group_expected = None;
                                self.dicomweb_active_path_receiver = None;
                            }
                        }
                    }
                    _ => {}
                },
                Ok(DicomPathKind::Other) => {
                    log::warn!("Ignoring streamed non-image DICOM input.");
                }
            }
        }

        if keep_receiver || !self.dicomweb_active_pending_paths.is_empty() {
            ctx.request_repaint_after(Duration::from_millis(16));
        }
    }

    pub(super) fn poll_dicomweb_download(&mut self, ctx: &egui::Context) {
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
                        let grouped_gsps_overlays =
                            Self::collect_grouped_gsps_overlays(&prepared_groups);
                        let grouped_sr_overlays =
                            Self::collect_grouped_sr_overlays(&prepared_groups);
                        let grouped_pm_overlays =
                            Self::collect_grouped_pm_overlays(&prepared_groups);
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
                        let active_group_is_multi_view =
                            Self::is_supported_multi_view_group_size(active_group_len);
                        let streamed_active_complete = streamed_count >= active_group_len
                            && (active_group_len == 1 || active_group_is_multi_view)
                            && self.dicomweb_active_pending_paths.is_empty()
                            && (!active_group_is_multi_view || self.mammo_load_receiver.is_none());
                        let active_group_is_displayed =
                            self.displayed_study_matches_paths(active_group_paths.as_slice());
                        let grouped_ready;

                        if !streamed_active_complete && !streaming_started {
                            self.load_local_groups(groups, validated_open_group, ctx);
                            grouped_ready =
                                self.displayed_study_matches_paths(active_group_paths.as_slice());
                        } else {
                            let completed_background_groups =
                                self.dicomweb_completed_background_groups.clone();
                            self.preload_non_active_groups_into_history(
                                &prepared_groups,
                                validated_open_group,
                                Some(&completed_background_groups),
                                ctx,
                            );
                            if !self.history_pushed_for_active_group {
                                if Self::is_supported_multi_view_group_size(active_group_len)
                                    && self.mammo_group_complete()
                                    && active_group_is_displayed
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
                                } else if !active_group_is_displayed {
                                    if let Some(active_group) =
                                        prepared_groups.get(validated_open_group).cloned()
                                    {
                                        self.enqueue_history_preload_job(
                                            HistoryPreloadJob::Group(active_group),
                                            ctx,
                                        );
                                    }
                                }
                                self.history_pushed_for_active_group = true;
                            }
                            self.move_current_history_to_front();
                            grouped_ready = if active_group_is_multi_view {
                                active_group_is_displayed && self.mammo_group_complete()
                            } else {
                                active_group_is_displayed
                            };
                        }
                        self.set_authoritative_pending_gsps_overlays(grouped_gsps_overlays);
                        self.set_authoritative_pending_sr_overlays(grouped_sr_overlays);
                        self.set_authoritative_pending_pm_overlays(grouped_pm_overlays);

                        if streamed_active_complete || !streaming_started {
                            self.dicomweb_active_group_expected = None;
                            self.dicomweb_active_group_paths.clear();
                            self.dicomweb_completed_background_groups.clear();
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
                    self.dicomweb_completed_background_groups.clear();
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
                self.dicomweb_completed_background_groups.clear();
                self.dicomweb_active_pending_paths.clear();
                self.dicomweb_active_path_receiver = None;
                self.mammo_load_sender = None;
                self.history_pushed_for_active_group = false;
            }
        }
    }

    pub(super) fn poll_mammo_group_load(&mut self, ctx: &egui::Context) {
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
                            self.dicomweb_completed_background_groups.clear();
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
                        self.dicomweb_completed_background_groups.clear();
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

    pub(super) fn poll_single_load(&mut self, ctx: &egui::Context) {
        let Some(receiver) = self.single_load_receiver.take() else {
            return;
        };

        match receiver.try_recv() {
            Ok(result) => {
                match result {
                    Ok(PendingSingleLoad::Image(pending)) => {
                        let pending = *pending;
                        self.apply_loaded_single(pending.path, pending.image, ctx);
                        self.clear_load_error();
                    }
                    Ok(PendingSingleLoad::StructuredReport { path, report }) => {
                        self.apply_loaded_structured_report(path, *report, ctx);
                        self.clear_load_error();
                    }
                    Err(err) => {
                        self.set_load_error("Failed to load selected item.");
                        log::error!("Failed to load selected item: {err}");
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
                self.set_load_error("Selected item load did not complete.");
                log::error!(
                    "Selected item load incomplete: worker exited before sending a result."
                );
                ctx.request_repaint();
            }
        }
    }

    pub(super) fn apply_prepared_load_paths(
        &mut self,
        prepared: PreparedLoadPaths,
        ctx: &egui::Context,
    ) -> Result<(), ()> {
        let PreparedLoadPaths {
            image_paths: paths,
            structured_report_paths,
            parametric_map_paths,
            gsps_overlays,
            sr_overlays,
            pm_overlays,
            gsps_files_found,
            other_files_found,
        } = prepared;
        if !paths.is_empty()
            || !structured_report_paths.is_empty()
            || !parametric_map_paths.is_empty()
            || gsps_files_found > 0
        {
            self.sync_current_state_to_history();
        }
        let preserve_history_preload = paths.len() == 1
            && self.dicomweb_active_group_expected == Some(1)
            && !self.dicomweb_active_group_paths.is_empty();
        let pending_overlay_state = if preserve_history_preload {
            let mut new_pending_gsps_overlays = self.pending_gsps_overlays.clone();
            let mut new_pending_sr_overlays = self.pending_sr_overlays.clone();
            let mut new_pending_pm_overlays = self.pending_pm_overlays.clone();
            let inserted_gsps =
                Self::insert_missing_gsps_overlays(&mut new_pending_gsps_overlays, gsps_overlays);
            let inserted_sr =
                Self::insert_missing_sr_overlays(&mut new_pending_sr_overlays, sr_overlays);
            let inserted_pm =
                Self::insert_missing_pm_overlays(&mut new_pending_pm_overlays, pm_overlays);
            PendingOverlayState {
                clear_history_preload: false,
                pending_gsps_overlays: new_pending_gsps_overlays,
                authoritative_gsps_overlay_keys: self.authoritative_gsps_overlay_keys.clone(),
                pending_sr_overlays: new_pending_sr_overlays,
                authoritative_sr_overlay_keys: self.authoritative_sr_overlay_keys.clone(),
                pending_pm_overlays: new_pending_pm_overlays,
                authoritative_pm_overlay_keys: self.authoritative_pm_overlay_keys.clone(),
                overlay_visible: self.overlay_visible,
                attach_to_current_study: inserted_gsps || inserted_sr || inserted_pm,
            }
        } else {
            PendingOverlayState {
                clear_history_preload: true,
                pending_gsps_overlays: gsps_overlays,
                authoritative_gsps_overlay_keys: HashSet::new(),
                pending_sr_overlays: sr_overlays,
                authoritative_sr_overlay_keys: HashSet::new(),
                pending_pm_overlays: pm_overlays,
                authoritative_pm_overlay_keys: HashSet::new(),
                overlay_visible: false,
                attach_to_current_study: false,
            }
        };

        if paths.is_empty() {
            if let Some((pm_path, remaining_parametric_maps)) = parametric_map_paths
                .split_first()
                .map(|(first, rest)| (first.clone(), rest))
            {
                self.commit_pending_overlay_state(pending_overlay_state);
                self.stage_structured_report_history_entries(&structured_report_paths, ctx);
                self.stage_parametric_map_history_entries(remaining_parametric_maps, ctx);
                self.load_parametric_map_path(pm_path, ctx);
                return Ok(());
            }
            if let Some((report_path, remaining_reports)) = structured_report_paths
                .split_first()
                .map(|(first, rest)| (first.clone(), rest))
            {
                self.commit_pending_overlay_state(pending_overlay_state);
                self.stage_structured_report_history_entries(remaining_reports, ctx);
                self.load_structured_report_path(report_path, ctx);
                return Ok(());
            }
            if gsps_files_found > 0 {
                self.set_load_error("GSPS detected, but no displayable DICOM image was selected.");
                log::warn!("GSPS detected, but no displayable DICOM image was selected.");
                ctx.request_repaint();
                return Err(());
            }
            if other_files_found > 0 {
                self.set_load_error(
                    "Selected DICOM objects are not displayable images, parametric maps, or structured reports.",
                );
                log::warn!(
                    "Selected DICOM objects are not displayable images, parametric maps, or structured reports."
                );
                ctx.request_repaint();
                return Err(());
            }
            return Err(());
        }

        match paths.len() {
            0 => Err(()),
            1 => {
                self.commit_pending_overlay_state(pending_overlay_state);
                if !structured_report_paths.is_empty() {
                    self.stage_structured_report_history_entries(&structured_report_paths, ctx);
                    log::info!(
                        "Opening {} image DICOM(s) and staging {} structured report object(s) as separate history entries.",
                        paths.len(),
                        structured_report_paths.len()
                    );
                }
                if !parametric_map_paths.is_empty() {
                    self.stage_parametric_map_history_entries(&parametric_map_paths, ctx);
                }
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
                self.commit_pending_overlay_state(pending_overlay_state);
                if !structured_report_paths.is_empty() {
                    self.stage_structured_report_history_entries(&structured_report_paths, ctx);
                    log::info!(
                        "Opening {} image DICOM(s) and staging {} structured report object(s) as separate history entries.",
                        paths.len(),
                        structured_report_paths.len()
                    );
                }
                if !parametric_map_paths.is_empty() {
                    self.stage_parametric_map_history_entries(&parametric_map_paths, ctx);
                }
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

    pub(super) fn load_selected_paths<T>(
        &mut self,
        paths: Vec<T>,
        ctx: &egui::Context,
    ) -> Result<(), ()>
    where
        T: Into<DicomSource>,
    {
        self.clear_load_error();
        let prepared = Self::prepare_load_paths(paths);
        self.apply_prepared_load_paths(prepared, ctx)
    }

    pub(super) fn stage_structured_report_history_entries<T>(
        &mut self,
        report_paths: &[T],
        ctx: &egui::Context,
    ) where
        T: Clone + Into<DicomSource>,
    {
        for path in report_paths {
            self.enqueue_history_preload_job(
                HistoryPreloadJob::StructuredReport(path.clone().into()),
                ctx,
            );
        }
    }

    pub(super) fn stage_parametric_map_history_entries<T>(
        &mut self,
        pm_paths: &[T],
        ctx: &egui::Context,
    ) where
        T: Clone + Into<DicomSource>,
    {
        for path in pm_paths {
            self.enqueue_history_preload_job(
                HistoryPreloadJob::ParametricMap(path.clone().into()),
                ctx,
            );
        }
    }

    pub(super) fn load_path(&mut self, path: DicomSource, ctx: &egui::Context) {
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
                    Ok(PendingSingleLoad::Image(Box::new(PendingLoad {
                        path,
                        image,
                    })))
                }
                Err(err) => Err(format!("Error opening selected DICOM: {err:#}")),
            };
            let _ = tx.send(result);
        });
        self.single_load_receiver = Some(rx);
        ctx.request_repaint();
    }

    pub(super) fn load_parametric_map_path(&mut self, path: DicomSource, ctx: &egui::Context) {
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.single_load_receiver = None;
        self.history_pushed_for_active_group = false;
        self.clear_load_error();
        log::info!("Loading selected Parametric Map...");
        let (tx, rx) = mpsc::channel::<Result<PendingSingleLoad, String>>();
        thread::spawn(move || {
            let result = match load_parametric_map(&path) {
                Ok(image) => Ok(PendingSingleLoad::Image(Box::new(PendingLoad {
                    path,
                    image,
                }))),
                Err(err) => Err(format!("Error opening selected Parametric Map: {err:#}")),
            };
            let _ = tx.send(result);
        });
        self.single_load_receiver = Some(rx);
        ctx.request_repaint();
    }

    pub(super) fn load_structured_report_path(&mut self, path: DicomSource, ctx: &egui::Context) {
        self.mammo_load_receiver = None;
        self.mammo_load_sender = None;
        self.single_load_receiver = None;
        self.history_pushed_for_active_group = false;
        self.clear_load_error();
        log::info!("Loading selected Structured Report...");
        let (tx, rx) = mpsc::channel::<Result<PendingSingleLoad, String>>();
        thread::spawn(move || {
            let result = match load_structured_report(&path) {
                Ok(report) => Ok(PendingSingleLoad::StructuredReport {
                    path,
                    report: Box::new(report),
                }),
                Err(err) => Err(format!("Error opening selected Structured Report: {err:#}")),
            };
            let _ = tx.send(result);
        });
        self.single_load_receiver = Some(rx);
        ctx.request_repaint();
    }

    pub(super) fn apply_loaded_single(
        &mut self,
        path: DicomSource,
        image: DicomImage,
        ctx: &egui::Context,
    ) {
        let mut image = image;
        Self::attach_matching_gsps_overlay(&mut image, &self.pending_gsps_overlays);
        Self::attach_matching_sr_overlay(&mut image, &self.pending_sr_overlays);
        Self::attach_matching_pm_overlay(&mut image, &self.pending_pm_overlays);
        self.overlay_visible = false;
        self.clear_load_error();
        self.reset_live_measurement();

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
        let path_meta = DicomSourceMeta::from(&path);
        self.report = None;
        self.image = Some(image);
        self.current_single_path = Some(path_meta.clone());
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
                    path: path_meta,
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

    pub(super) fn apply_loaded_structured_report(
        &mut self,
        path: DicomSource,
        report: StructuredReportDocument,
        ctx: &egui::Context,
    ) {
        self.clear_single_viewer();
        self.mammo_group.clear();
        self.clear_load_error();
        let path_meta = DicomSourceMeta::from(&path);
        self.push_report_history_entry(path_meta.clone(), report.clone(), ctx);
        self.report = Some(report);
        self.current_single_path = Some(path_meta);
        ctx.request_repaint();
        log::info!("Loaded selected Structured Report.");
    }

    pub(super) fn load_mammo_group_paths(&mut self, paths: Vec<DicomSource>, ctx: &egui::Context) {
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
}
