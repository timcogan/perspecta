use super::*;

#[cfg(any(test, target_arch = "wasm32"))]
fn add_selected_source_retained_pixels(
    counter: &mut WebRetainedPixelCounter,
    source: &DicomSource,
) -> Result<(), String> {
    let pixels = match classify_dicom_path(source) {
        Ok(DicomPathKind::Image) => read_dicom_frame_pixel_count(source),
        Ok(DicomPathKind::ParametricMap) => read_dicom_logical_pixel_count(source),
        Ok(DicomPathKind::Gsps | DicomPathKind::StructuredReport | DicomPathKind::Other) => {
            return Ok(())
        }
        Err(_) => read_dicom_frame_pixel_count(source),
    };
    let pixels = pixels.map_err(|_| {
        "Could not inspect a selected DICOM object's pixel dimensions before browser decode."
            .to_string()
    })?;
    counter
        .add_pixels(pixels)
        .ok_or_else(|| WEB_PIXEL_LIMIT_MESSAGE.to_string())?;
    counter.ensure_limit()?;
    Ok(())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(super) fn selected_source_retained_pixels(sources: &[DicomSource]) -> Result<usize, String> {
    let mut counter = WebRetainedPixelCounter::default();
    for source in sources {
        add_selected_source_retained_pixels(&mut counter, source)?;
    }
    Ok(counter.total())
}

#[cfg(target_arch = "wasm32")]
pub(super) async fn selected_source_retained_pixels_cooperative(
    sources: Vec<DicomSource>,
) -> WebPreflightResult {
    let mut counter = WebRetainedPixelCounter::default();
    for source in &sources {
        add_selected_source_retained_pixels(&mut counter, source)?;
        crate::platform::yield_to_browser().await;
    }
    Ok((sources, counter.total()))
}

impl DicomViewerApp {
    #[cfg(any(test, target_arch = "wasm32"))]
    pub(super) fn begin_web_selection_pixel_reservation(
        &mut self,
        pixels: usize,
    ) -> Result<(), String> {
        if self.web_selection_pixel_reservation > 0 || self.is_loading() {
            return Err(
                "Perspecta is still opening the previous selection. Wait for it to finish, then try again."
                    .to_string(),
            );
        }
        if pixels > WEB_MAX_RETAINED_PIXELS {
            return Err(WEB_PIXEL_LIMIT_MESSAGE.to_string());
        }

        self.sync_current_state_to_history();
        self.apply_web_history_budget(pixels)?;
        self.web_selection_pixel_reservation = pixels;
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    pub(super) fn finish_web_selection_pixel_reservation_if_idle(&mut self) {
        if self.web_selection_pixel_reservation == 0 || self.is_loading() {
            return;
        }

        self.web_selection_pixel_reservation = 0;
        if let Err(message) = self.apply_web_history_budget(0) {
            self.set_load_error(message);
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub(super) fn rebalance_web_history_if_unreserved(&mut self) {
        if self.web_selection_pixel_reservation == 0 {
            let _ = self.apply_web_history_budget(0);
        }
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    fn apply_web_history_budget(&mut self, reserved_pixels: usize) -> Result<(), String> {
        let current_id = self.current_history_id();
        let current_index = current_id.as_ref().and_then(|id| {
            self.history_entries
                .iter()
                .position(|entry| entry.id == *id)
        });

        let mut counter = WebRetainedPixelCounter::default();
        counter
            .add_pm_overlays(&self.pending_pm_overlays)
            .ok_or_else(|| WEB_PIXEL_LIMIT_MESSAGE.to_string())?;

        if current_index.is_none() {
            if let Some(image) = self.image.as_ref() {
                counter
                    .add_image(image)
                    .ok_or_else(|| WEB_PIXEL_LIMIT_MESSAGE.to_string())?;
            }
            for viewport in self.loaded_mammo_viewports() {
                counter
                    .add_image(&viewport.image)
                    .ok_or_else(|| WEB_PIXEL_LIMIT_MESSAGE.to_string())?;
            }
        }
        counter
            .add_pixels(reserved_pixels)
            .ok_or_else(|| WEB_PIXEL_LIMIT_MESSAGE.to_string())?;
        counter.ensure_limit()?;

        let retained = super::history::web_history_retained_entry_flags_with_counter(
            &self.history_entries,
            current_index,
            WEB_MAX_RETAINED_PIXELS,
            counter,
        );
        if current_index.is_some_and(|index| !retained.get(index).copied().unwrap_or(false)) {
            return Err(format!(
                "The active study and this selection exceed the {WEB_MAX_RETAINED_PIXELS} retained-pixel browser limit. Close the active study and try again."
            ));
        }

        let mut index = 0usize;
        self.history_entries.retain(|_| {
            let keep = retained.get(index).copied().unwrap_or(false);
            index = index.saturating_add(1);
            keep
        });
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    fn single_entry(label: &str, ctx: &egui::Context) -> (DicomSourceMeta, HistoryEntry) {
        let source = DicomSource::from_memory(label, vec![0]);
        let path = source.to_meta();
        let image = DicomImage::test_stub_with_mono_frames(None, 1);
        let texture = ctx.load_texture(
            format!("web-budget-{label}"),
            ColorImage::new([1, 1], vec![egui::Color32::BLACK]),
            TextureOptions::LINEAR,
        );
        let entry = HistoryEntry {
            id: super::super::history::history_id_from_paths(std::slice::from_ref(&path)),
            kind: HistoryKind::Single(Box::new(HistorySingleData {
                path: path.clone(),
                image,
                texture,
                window_center: 0.0,
                window_width: 1.0,
                current_frame: 0,
                cine_fps: DEFAULT_CINE_FPS,
            })),
            thumbs: Vec::new(),
        };
        (path, entry)
    }

    #[test]
    fn selection_reservation_evicts_background_before_decode_and_preserves_current() {
        let ctx = egui::Context::default();
        let (_, background) = single_entry("background.dcm", &ctx);
        let (current_path, current) = single_entry("current.dcm", &ctx);
        let current_id = current.id.clone();
        let mut app = DicomViewerApp {
            current_single_path: Some(current_path),
            history_entries: vec![background, current],
            ..Default::default()
        };

        app.begin_web_selection_pixel_reservation(WEB_MAX_RETAINED_PIXELS - 1)
            .expect("one active pixel plus the reservation should fit exactly");

        assert_eq!(
            app.web_selection_pixel_reservation,
            WEB_MAX_RETAINED_PIXELS - 1
        );
        assert_eq!(app.history_entries.len(), 1);
        assert_eq!(app.history_entries[0].id, current_id);
    }

    #[test]
    fn selection_reservation_rejects_before_evicting_the_active_study() {
        let ctx = egui::Context::default();
        let (_, background) = single_entry("background.dcm", &ctx);
        let (current_path, current) = single_entry("current.dcm", &ctx);
        let mut app = DicomViewerApp {
            current_single_path: Some(current_path),
            history_entries: vec![background, current],
            ..Default::default()
        };

        let error = app
            .begin_web_selection_pixel_reservation(WEB_MAX_RETAINED_PIXELS)
            .expect_err("the active pixel and full-budget reservation cannot coexist");

        assert!(error.contains("Close the active study"));
        assert_eq!(app.web_selection_pixel_reservation, 0);
        assert_eq!(app.history_entries.len(), 2);
    }
}
