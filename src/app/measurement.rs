use eframe::egui::{self, Align2, FontId, PointerButton};

use crate::dicom::DicomImage;

use super::{DicomViewerApp, PERSPECTA_BRAND_BLUE};

const MEASUREMENT_COLOR: egui::Color32 = PERSPECTA_BRAND_BLUE;
const MEASUREMENT_STROKE_WIDTH: f32 = 2.0;
const MEASUREMENT_HANDLE_RADIUS: f32 = 4.0;
const MEASUREMENT_LABEL_OFFSET_X: f32 = 8.0;
const MEASUREMENT_LABEL_OFFSET_Y: f32 = 8.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MeasurementTarget {
    Single,
    Mammo { index: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MeasurementUnits {
    Millimeters,
    Pixels,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct MeasurementGeometry {
    width: usize,
    height: usize,
    pixel_spacing_mm: Option<(f32, f32)>,
}

impl MeasurementGeometry {
    pub(super) fn from_image(image: &DicomImage) -> Self {
        Self {
            width: image.width,
            height: image.height,
            pixel_spacing_mm: image
                .pixel_spacing_mm
                .map(|spacing| (spacing.row_mm, spacing.col_mm)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct LiveMeasurement {
    pub(super) target: MeasurementTarget,
    units: MeasurementUnits,
    anchor_image_pos: egui::Pos2,
    live_image_pos: egui::Pos2,
}

impl DicomViewerApp {
    pub(super) fn reset_live_measurement(&mut self) {
        self.live_measurement = None;
        self.block_primary_interactions_until_release = false;
    }

    pub(super) fn clear_live_measurement(&mut self) {
        self.live_measurement = None;
    }

    pub(super) fn has_live_measurement(&self) -> bool {
        self.live_measurement.is_some()
    }

    pub(super) fn sync_measurement_primary_interaction_block(&mut self, ctx: &egui::Context) {
        if self.block_primary_interactions_until_release
            && !ctx.input(|input| input.pointer.button_down(PointerButton::Primary))
        {
            self.block_primary_interactions_until_release = false;
        }
    }

    pub(super) fn maybe_clear_live_measurement_with_primary(
        &mut self,
        response: &egui::Response,
    ) -> bool {
        if self.block_primary_interactions_until_release {
            return true;
        }
        if self.live_measurement.is_none() || !response.contains_pointer() {
            return false;
        }

        let primary_pressed = response
            .ctx
            .input(|input| input.pointer.button_pressed(PointerButton::Primary));
        if !primary_pressed {
            return false;
        }

        self.clear_live_measurement();
        self.block_primary_interactions_until_release = true;
        true
    }

    pub(super) fn handle_escape_action(&mut self) -> bool {
        if self.has_live_measurement() {
            self.clear_live_measurement();
            return true;
        }
        if self.full_metadata_popup_open {
            self.close_full_metadata_popup();
            return true;
        }
        false
    }

    pub(super) fn set_single_current_frame(&mut self, frame_index: usize) {
        if self.current_frame != frame_index {
            self.clear_live_measurement();
            self.current_frame = frame_index;
        }
    }

    pub(super) fn begin_live_measurement(
        &mut self,
        target: MeasurementTarget,
        geometry: MeasurementGeometry,
        image_rect: egui::Rect,
        pointer_pos: egui::Pos2,
    ) {
        if !image_rect.contains(pointer_pos) {
            return;
        }

        let image_pos = screen_to_image_pos(pointer_pos, image_rect, geometry);
        self.live_measurement = Some(LiveMeasurement {
            target,
            units: measurement_units(geometry),
            anchor_image_pos: image_pos,
            live_image_pos: image_pos,
        });
    }

    pub(super) fn update_live_measurement_for_target(
        &mut self,
        target: MeasurementTarget,
        geometry: MeasurementGeometry,
        image_rect: egui::Rect,
        pointer_pos: Option<egui::Pos2>,
    ) {
        let Some(measurement) = self.live_measurement.as_mut() else {
            return;
        };
        if measurement.target != target {
            return;
        }
        let Some(pointer_pos) = pointer_pos else {
            return;
        };

        measurement.live_image_pos = screen_to_image_pos(pointer_pos, image_rect, geometry);
    }

    pub(super) fn draw_live_measurement(
        &self,
        painter: &egui::Painter,
        target: MeasurementTarget,
        geometry: MeasurementGeometry,
        image_rect: egui::Rect,
    ) {
        let Some(measurement) = self.live_measurement.as_ref() else {
            return;
        };
        if measurement.target != target {
            return;
        }

        let start = image_to_screen_pos(measurement.anchor_image_pos, image_rect, geometry);
        let end = image_to_screen_pos(measurement.live_image_pos, image_rect, geometry);
        let stroke = egui::Stroke::new(MEASUREMENT_STROKE_WIDTH, MEASUREMENT_COLOR);
        painter.line_segment([start, end], stroke);
        painter.circle_filled(start, MEASUREMENT_HANDLE_RADIUS, MEASUREMENT_COLOR);
        painter.circle_filled(end, MEASUREMENT_HANDLE_RADIUS, MEASUREMENT_COLOR);

        let label = measurement_label_text(*measurement, geometry);
        let (label_offset, label_anchor) = measurement_label_layout(start, end);
        let label_pos = end + label_offset;
        let font_id = FontId::monospace(12.0);
        painter.text(
            label_pos + egui::vec2(1.0, 1.0),
            label_anchor,
            &label,
            font_id.clone(),
            egui::Color32::BLACK,
        );
        painter.text(label_pos, label_anchor, label, font_id, MEASUREMENT_COLOR);
    }

    pub(super) fn update_measurement_cursor(
        &self,
        ctx: &egui::Context,
        target: MeasurementTarget,
        image_rect: egui::Rect,
        pointer_pos: Option<egui::Pos2>,
    ) {
        let Some(measurement) = self.live_measurement.as_ref() else {
            return;
        };
        if measurement.target != target {
            return;
        }
        let Some(pointer_pos) = pointer_pos else {
            return;
        };
        if image_rect.contains(pointer_pos) {
            ctx.set_cursor_icon(egui::CursorIcon::Crosshair);
        }
    }
}

fn measurement_units(geometry: MeasurementGeometry) -> MeasurementUnits {
    if geometry.pixel_spacing_mm.is_some() {
        MeasurementUnits::Millimeters
    } else {
        MeasurementUnits::Pixels
    }
}

fn screen_to_image_pos(
    pointer_pos: egui::Pos2,
    image_rect: egui::Rect,
    geometry: MeasurementGeometry,
) -> egui::Pos2 {
    let clamped_x = pointer_pos.x.clamp(image_rect.left(), image_rect.right());
    let clamped_y = pointer_pos.y.clamp(image_rect.top(), image_rect.bottom());
    let width = geometry.width.max(1) as f32;
    let height = geometry.height.max(1) as f32;
    let norm_x = ((clamped_x - image_rect.left()) / image_rect.width()).clamp(0.0, 1.0);
    let norm_y = ((clamped_y - image_rect.top()) / image_rect.height()).clamp(0.0, 1.0);
    egui::pos2(norm_x * width, norm_y * height)
}

fn image_to_screen_pos(
    image_pos: egui::Pos2,
    image_rect: egui::Rect,
    geometry: MeasurementGeometry,
) -> egui::Pos2 {
    let width = geometry.width.max(1) as f32;
    let height = geometry.height.max(1) as f32;
    let norm_x = (image_pos.x / width).clamp(0.0, 1.0);
    let norm_y = (image_pos.y / height).clamp(0.0, 1.0);
    egui::pos2(
        image_rect.left() + norm_x * image_rect.width(),
        image_rect.top() + norm_y * image_rect.height(),
    )
}

fn measurement_distance(measurement: LiveMeasurement, geometry: MeasurementGeometry) -> f32 {
    let dx = measurement.live_image_pos.x - measurement.anchor_image_pos.x;
    let dy = measurement.live_image_pos.y - measurement.anchor_image_pos.y;
    match (measurement.units, geometry.pixel_spacing_mm) {
        (MeasurementUnits::Millimeters, Some((row_mm, col_mm))) => {
            ((dx * col_mm).powi(2) + (dy * row_mm).powi(2)).sqrt()
        }
        _ => (dx.powi(2) + dy.powi(2)).sqrt(),
    }
}

fn measurement_label_text(measurement: LiveMeasurement, geometry: MeasurementGeometry) -> String {
    let distance = measurement_distance(measurement, geometry);
    match measurement.units {
        MeasurementUnits::Millimeters => format!("{distance:.2} mm"),
        MeasurementUnits::Pixels => format!("{distance:.1} px"),
    }
}

fn measurement_label_layout(start: egui::Pos2, end: egui::Pos2) -> (egui::Vec2, Align2) {
    let delta = end - start;
    let place_right = delta.x >= 0.0;
    let place_below = delta.y >= 0.0;
    let offset = egui::vec2(
        if place_right {
            MEASUREMENT_LABEL_OFFSET_X
        } else {
            -MEASUREMENT_LABEL_OFFSET_X
        },
        if place_below {
            MEASUREMENT_LABEL_OFFSET_Y
        } else {
            -MEASUREMENT_LABEL_OFFSET_Y
        },
    );
    let anchor = match (place_right, place_below) {
        (true, true) => Align2::LEFT_TOP,
        (true, false) => Align2::LEFT_BOTTOM,
        (false, true) => Align2::RIGHT_TOP,
        (false, false) => Align2::RIGHT_BOTTOM,
    };
    (offset, anchor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn screen_to_image_pos_clamps_to_image_rect() {
        let geometry = MeasurementGeometry {
            width: 100,
            height: 50,
            pixel_spacing_mm: None,
        };
        let point = screen_to_image_pos(
            egui::pos2(120.0, -10.0),
            egui::Rect::from_min_max(egui::Pos2::ZERO, egui::pos2(100.0, 50.0)),
            geometry,
        );

        assert_eq!(point, egui::pos2(100.0, 0.0));
    }

    #[test]
    fn measurement_distance_uses_anisotropic_pixel_spacing() {
        let geometry = MeasurementGeometry {
            width: 100,
            height: 100,
            pixel_spacing_mm: Some((0.5, 0.25)),
        };
        let measurement = LiveMeasurement {
            target: MeasurementTarget::Single,
            units: MeasurementUnits::Millimeters,
            anchor_image_pos: egui::pos2(10.0, 20.0),
            live_image_pos: egui::pos2(13.0, 24.0),
        };

        let distance = measurement_distance(measurement, geometry);

        assert!(
            (distance - 2.1360009).abs() < 0.0001,
            "unexpected distance {distance}"
        );
    }

    #[test]
    fn measurement_label_text_falls_back_to_pixels_without_spacing() {
        let geometry = MeasurementGeometry {
            width: 100,
            height: 100,
            pixel_spacing_mm: None,
        };
        let measurement = LiveMeasurement {
            target: MeasurementTarget::Single,
            units: MeasurementUnits::Pixels,
            anchor_image_pos: egui::pos2(1.0, 2.0),
            live_image_pos: egui::pos2(4.0, 6.0),
        };

        assert_eq!(measurement_label_text(measurement, geometry), "5.0 px");
    }

    #[test]
    fn handle_escape_action_clears_measurement_before_metadata_popup() {
        let mut app = DicomViewerApp {
            full_metadata_popup_open: true,
            live_measurement: Some(LiveMeasurement {
                target: MeasurementTarget::Single,
                units: MeasurementUnits::Pixels,
                anchor_image_pos: egui::pos2(0.0, 0.0),
                live_image_pos: egui::pos2(10.0, 10.0),
            }),
            ..Default::default()
        };

        assert!(app.handle_escape_action());
        assert!(app.live_measurement.is_none());
        assert!(app.full_metadata_popup_open);

        assert!(app.handle_escape_action());
        assert!(!app.full_metadata_popup_open);
    }

    #[test]
    fn measurement_label_layout_places_text_away_from_line_on_rightward_segment() {
        let (offset, anchor) =
            measurement_label_layout(egui::pos2(10.0, 10.0), egui::pos2(50.0, 40.0));

        assert_eq!(
            offset,
            egui::vec2(MEASUREMENT_LABEL_OFFSET_X, MEASUREMENT_LABEL_OFFSET_Y)
        );
        assert_eq!(anchor, Align2::LEFT_TOP);
    }

    #[test]
    fn measurement_label_layout_places_text_away_from_line_on_leftward_segment() {
        let (offset, anchor) =
            measurement_label_layout(egui::pos2(50.0, 40.0), egui::pos2(10.0, 10.0));

        assert_eq!(
            offset,
            egui::vec2(-MEASUREMENT_LABEL_OFFSET_X, -MEASUREMENT_LABEL_OFFSET_Y)
        );
        assert_eq!(anchor, Align2::RIGHT_BOTTOM);
    }
}
