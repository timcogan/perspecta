use std::sync::Arc;

use super::*;
use crate::dicom::{
    load_full_metadata_from_source, FullMetadataField, FullMetadataItem, FullMetadataValue,
};

impl DicomViewerApp {
    pub(super) fn active_full_metadata(&self) -> Option<Arc<[FullMetadataField]>> {
        if self.image.is_some() || self.loaded_mammo_count() > 0 {
            self.active_image()?.loaded_full_metadata()
        } else {
            self.report
                .as_ref()
                .map(|report| Arc::clone(&report.full_metadata))
        }
    }

    pub(super) fn can_toggle_full_metadata_popup(&self) -> bool {
        self.pending_history_open_id.is_none() && self.has_active_full_metadata()
    }

    pub(super) fn toggle_full_metadata_popup(&mut self) {
        if !self.can_toggle_full_metadata_popup() {
            return;
        }
        self.full_metadata_popup_open = !self.full_metadata_popup_open;
    }

    pub(super) fn close_full_metadata_popup(&mut self) {
        self.full_metadata_popup_open = false;
    }

    pub(super) fn poll_full_metadata_load(&mut self, ctx: &egui::Context) {
        let Some(receiver) = self.full_metadata_receiver.as_ref() else {
            return;
        };

        let mut loaded_results = Vec::new();
        loop {
            match receiver.try_recv() {
                Ok(result) => loaded_results.push(result),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.full_metadata_receiver = None;
                    self.full_metadata_sender = None;
                    break;
                }
            }
        }

        let mut updated = false;
        for result in loaded_results {
            updated |= self.apply_loaded_full_metadata(result);
        }

        if updated {
            ctx.request_repaint();
        }
    }

    pub(super) fn show_metadata_ui(&mut self, ctx: &egui::Context) {
        let has_full_metadata = self.has_active_full_metadata();
        let toggle_enabled = has_full_metadata && self.can_toggle_full_metadata_popup();
        let open_requested = self
            .active_metadata()
            .map(|metadata| {
                Self::show_summary_metadata_overlay(
                    ctx,
                    metadata,
                    &self.visible_metadata_fields,
                    toggle_enabled,
                )
            })
            .unwrap_or(false);

        if open_requested && toggle_enabled {
            self.full_metadata_popup_open = true;
        }
        if !has_full_metadata {
            self.full_metadata_popup_open = false;
            return;
        }

        if !self.full_metadata_popup_open {
            return;
        }

        self.ensure_active_full_metadata_loading(ctx);

        if self.active_full_metadata_loading() {
            let mut popup_open = self.full_metadata_popup_open;
            Self::show_full_metadata_loading_popup(ctx, &mut popup_open);
            self.full_metadata_popup_open = popup_open;
            ctx.request_repaint_after(Duration::from_millis(16));
            return;
        }

        let Some(metadata) = self.active_full_metadata() else {
            self.full_metadata_popup_open = false;
            return;
        };

        let mut popup_open = self.full_metadata_popup_open;
        Self::show_full_metadata_popup(ctx, metadata.as_ref(), &mut popup_open);
        self.full_metadata_popup_open = popup_open;
    }

    fn has_active_full_metadata(&self) -> bool {
        if let Some(image) = self.active_image() {
            image.has_full_metadata()
        } else {
            self.report
                .as_ref()
                .is_some_and(|report| !report.full_metadata.is_empty())
        }
    }

    fn active_full_metadata_loading(&self) -> bool {
        self.active_image()
            .is_some_and(DicomImage::full_metadata_loading)
    }

    fn ensure_active_full_metadata_loading(&mut self, ctx: &egui::Context) {
        let Some(sender) = self.full_metadata_sender.clone() else {
            return;
        };
        let Some(image) = self.active_image_mut() else {
            return;
        };
        let Some(source) = image.begin_full_metadata_load() else {
            return;
        };

        let source_key = source.stable_id();
        thread::spawn(move || {
            let metadata = match load_full_metadata_from_source(&source) {
                Ok(metadata) => metadata,
                Err(err) => {
                    log::warn!("Could not load full metadata: {err:#}");
                    Arc::default()
                }
            };
            let _ = sender.send(FullMetadataLoadResult {
                source_key,
                metadata,
            });
        });
        ctx.request_repaint_after(Duration::from_millis(16));
    }

    fn apply_loaded_full_metadata(&mut self, result: FullMetadataLoadResult) -> bool {
        if let Some(image) = self.image.as_mut() {
            if image.finish_full_metadata_load(&result.source_key, Arc::clone(&result.metadata)) {
                return true;
            }
        }

        for viewport in self.mammo_group.iter_mut().filter_map(Option::as_mut) {
            if viewport
                .image
                .finish_full_metadata_load(&result.source_key, Arc::clone(&result.metadata))
            {
                return true;
            }
        }

        false
    }

    fn show_summary_metadata_overlay(
        ctx: &egui::Context,
        metadata: &[(String, String)],
        visible_metadata_fields: &HashSet<String>,
        toggle_enabled: bool,
    ) -> bool {
        let overlay_height = (ctx.screen_rect().height() * 0.62).max(180.0);
        let mut open_requested = false;
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
                            if !visible_metadata_fields.contains(key.as_str()) {
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

                        ui.add_space(ui.spacing().item_spacing.y);
                        if Self::metadata_overlay_action(ui, "View all fields (V)", toggle_enabled)
                            .clicked()
                        {
                            open_requested = true;
                        }
                    });
            });
        open_requested
    }

    fn metadata_overlay_action(ui: &mut egui::Ui, text: &str, enabled: bool) -> egui::Response {
        let font_id = egui::TextStyle::Body.resolve(ui.style());
        let galley = ui.painter().layout_no_wrap(
            text.to_owned(),
            font_id.clone(),
            ui.visuals().weak_text_color(),
        );
        let sense = if enabled {
            egui::Sense::click()
        } else {
            egui::Sense::hover()
        };
        let (rect, response) = ui.allocate_exact_size(galley.size(), sense);
        let response = if enabled {
            response.on_hover_cursor(egui::CursorIcon::PointingHand)
        } else {
            response
        };
        let color = if enabled && response.hovered() {
            egui::Color32::WHITE
        } else if enabled {
            ui.visuals().weak_text_color()
        } else {
            ui.visuals().weak_text_color().gamma_multiply(0.65)
        };
        ui.painter().text(
            rect.left_top(),
            egui::Align2::LEFT_TOP,
            text,
            font_id,
            color,
        );
        response
    }

    fn show_full_metadata_popup(
        ctx: &egui::Context,
        metadata: &[FullMetadataField],
        popup_open: &mut bool,
    ) {
        let popup_id = egui::Id::new("full-metadata-popup");
        let screen_rect = ctx.screen_rect();
        let default_size = egui::vec2(
            (screen_rect.width() * 0.74).clamp(520.0, 980.0),
            (screen_rect.height() * 0.76).clamp(360.0, 760.0),
        );

        let previous_visuals = ctx.style().visuals.clone();
        let mut popup_visuals = previous_visuals.clone();
        popup_visuals.widgets.open.weak_bg_fill = egui::Color32::BLACK;
        ctx.set_visuals(popup_visuals);

        egui::Window::new(
            egui::RichText::new("Metadata fields").color(previous_visuals.text_color()),
        )
        .id(popup_id)
        .order(egui::Order::Foreground)
        .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
        .collapsible(false)
        .default_size(default_size)
        .open(popup_open)
        .resizable(true)
        .show(ctx, |ui| {
            egui::ScrollArea::vertical()
                .id_salt("full-metadata-popup-scroll")
                .show(ui, |ui| {
                    if metadata.is_empty() {
                        ui.label("No metadata fields available.");
                        return;
                    }

                    let mut path = Vec::new();
                    Self::show_full_metadata_fields(ui, metadata, &mut path);
                });
        });
        ctx.move_to_top(egui::LayerId::new(egui::Order::Foreground, popup_id));

        ctx.set_visuals(previous_visuals);
    }

    fn show_full_metadata_loading_popup(ctx: &egui::Context, popup_open: &mut bool) {
        let popup_id = egui::Id::new("full-metadata-popup");
        let screen_rect = ctx.screen_rect();
        let default_size = egui::vec2(
            (screen_rect.width() * 0.74).clamp(520.0, 980.0),
            (screen_rect.height() * 0.76).clamp(360.0, 760.0),
        );

        let previous_visuals = ctx.style().visuals.clone();
        let mut popup_visuals = previous_visuals.clone();
        popup_visuals.widgets.open.weak_bg_fill = egui::Color32::BLACK;
        ctx.set_visuals(popup_visuals);

        egui::Window::new(
            egui::RichText::new("Metadata fields").color(previous_visuals.text_color()),
        )
        .id(popup_id)
        .order(egui::Order::Foreground)
        .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
        .collapsible(false)
        .default_size(default_size)
        .open(popup_open)
        .resizable(true)
        .show(ctx, |ui| {
            ui.label("Loading metadata fields...");
        });
        ctx.move_to_top(egui::LayerId::new(egui::Order::Foreground, popup_id));

        ctx.set_visuals(previous_visuals);
    }

    fn show_full_metadata_fields(
        ui: &mut egui::Ui,
        fields: &[FullMetadataField],
        path: &mut Vec<usize>,
    ) {
        for (field_index, field) in fields.iter().enumerate() {
            path.push(field_index);
            Self::show_full_metadata_field(ui, field, path);
            path.pop();
            ui.add_space(4.0);
        }
    }

    fn show_full_metadata_field(
        ui: &mut egui::Ui,
        field: &FullMetadataField,
        path: &mut Vec<usize>,
    ) {
        ui.push_id(path.clone(), |ui| match &field.value {
            FullMetadataValue::Scalar(value) => {
                ui.horizontal_wrapped(|ui| {
                    ui.monospace(Self::full_metadata_field_label(field));
                    ui.label(egui::RichText::new(format!("[{}]", field.vr)).weak());
                    if value.is_empty() {
                        ui.label(egui::RichText::new("<empty>").italics().weak());
                    } else {
                        ui.label(value);
                    }
                });
            }
            FullMetadataValue::Sequence(items) => {
                egui::CollapsingHeader::new(format!(
                    "{} [{}] ({} items)",
                    Self::full_metadata_field_label(field),
                    field.vr,
                    items.len()
                ))
                .show(ui, |ui| {
                    for (item_index, item) in items.iter().enumerate() {
                        path.push(item_index);
                        Self::show_full_metadata_item(ui, item_index, item, path);
                        path.pop();
                        ui.add_space(6.0);
                    }
                });
            }
        });
    }

    fn show_full_metadata_item(
        ui: &mut egui::Ui,
        item_index: usize,
        item: &FullMetadataItem,
        path: &mut Vec<usize>,
    ) {
        ui.push_id(path.clone(), |ui| {
            ui.group(|ui| {
                ui.label(egui::RichText::new(format!("Item {}", item_index + 1)).strong());
                ui.add_space(4.0);
                if item.fields.is_empty() {
                    ui.label("No fields.");
                } else {
                    Self::show_full_metadata_fields(ui, &item.fields, path);
                }
            });
        });
    }

    fn full_metadata_field_label(field: &FullMetadataField) -> String {
        if field.keyword.is_empty() {
            field.tag.clone()
        } else {
            format!("{} {}", field.keyword, field.tag)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dicom::StructuredReportDocument;

    fn sample_full_metadata() -> Vec<FullMetadataField> {
        vec![FullMetadataField {
            keyword: "PatientName".to_string(),
            tag: "(0010,0010)".to_string(),
            vr: "PN".to_string(),
            value: FullMetadataValue::Scalar("Doe^Jane".to_string()),
        }]
    }

    #[test]
    fn toggle_full_metadata_popup_requires_active_metadata() {
        let mut app = DicomViewerApp::default();

        app.toggle_full_metadata_popup();

        assert!(!app.full_metadata_popup_open);
    }

    #[test]
    fn toggle_full_metadata_popup_toggles_for_active_image() {
        let mut image = DicomImage::test_stub(None);
        image.full_metadata = sample_full_metadata().into();
        let mut app = DicomViewerApp {
            image: Some(image),
            ..Default::default()
        };

        app.toggle_full_metadata_popup();
        assert!(app.full_metadata_popup_open);

        app.toggle_full_metadata_popup();
        assert!(!app.full_metadata_popup_open);
    }

    #[test]
    fn active_full_metadata_reads_report_when_no_image_is_active() {
        let mut report = StructuredReportDocument::test_stub();
        report.full_metadata = sample_full_metadata().into();
        let app = DicomViewerApp {
            report: Some(report),
            ..Default::default()
        };

        let metadata = app
            .active_full_metadata()
            .expect("report should expose full metadata");

        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0].keyword, "PatientName");
    }

    #[test]
    fn close_full_metadata_popup_clears_popup_state() {
        let mut image = DicomImage::test_stub(None);
        image.full_metadata = sample_full_metadata().into();
        let mut app = DicomViewerApp {
            image: Some(image),
            full_metadata_popup_open: true,
            ..Default::default()
        };

        app.close_full_metadata_popup();

        assert!(!app.full_metadata_popup_open);
    }

    #[test]
    fn can_toggle_full_metadata_popup_requires_loaded_metadata() {
        let app = DicomViewerApp::default();

        assert!(!app.can_toggle_full_metadata_popup());
    }

    #[test]
    fn can_toggle_full_metadata_popup_respects_pending_history_transition() {
        let mut image = DicomImage::test_stub(None);
        image.full_metadata = sample_full_metadata().into();
        let app = DicomViewerApp {
            image: Some(image),
            pending_history_open_id: Some("history-entry".to_string()),
            ..Default::default()
        };

        assert!(!app.can_toggle_full_metadata_popup());
    }

    #[test]
    fn toggle_full_metadata_popup_ignores_pending_history_transition() {
        let mut image = DicomImage::test_stub(None);
        image.full_metadata = sample_full_metadata().into();
        let mut app = DicomViewerApp {
            image: Some(image),
            pending_history_open_id: Some("history-entry".to_string()),
            ..Default::default()
        };

        app.toggle_full_metadata_popup();

        assert!(!app.full_metadata_popup_open);
    }
}
