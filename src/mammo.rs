use std::collections::VecDeque;
use std::path::Path;

use eframe::egui;

use crate::dicom::DicomImage;

pub fn normalize_token(value: Option<&str>) -> String {
    value
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase()
        .replace(' ', "")
}

pub fn classify_laterality(value: Option<&str>) -> Option<&'static str> {
    let token = normalize_token(value);
    if token.starts_with('R') || token.contains("RIGHT") {
        Some("R")
    } else if token.starts_with('L') || token.contains("LEFT") {
        Some("L")
    } else {
        None
    }
}

pub fn classify_view(value: Option<&str>) -> Option<&'static str> {
    let token = normalize_token(value);
    if token == "MLO" {
        Some("MLO")
    } else if token == "CC" {
        Some("CC")
    } else {
        None
    }
}

fn mammo_slot_index(image: &DicomImage) -> Option<usize> {
    match (
        classify_view(image.view_position.as_deref()),
        classify_laterality(image.image_laterality.as_deref()),
    ) {
        (Some("CC"), Some("R")) => Some(0),
        (Some("CC"), Some("L")) => Some(1),
        (Some("MLO"), Some("R")) => Some(2),
        (Some("MLO"), Some("L")) => Some(3),
        _ => None,
    }
}

pub fn preferred_mammo_slot(
    image: &DicomImage,
    len: usize,
    mut is_free: impl FnMut(usize) -> bool,
) -> Option<usize> {
    let laterality = classify_laterality(image.image_laterality.as_deref());
    mammo_slot_index(image)
        .filter(|index| *index < len && is_free(*index))
        .or_else(|| {
            preferred_slots_for_laterality(laterality)
                .into_iter()
                .find(|index| *index < len && is_free(*index))
        })
}

pub fn order_mammo_indices<T>(
    items: &[T],
    mut image_of: impl FnMut(&T) -> &DicomImage,
) -> Vec<usize> {
    let mut ordered = vec![None; items.len()];
    let mut fallback = VecDeque::new();

    for (index, item) in items.iter().enumerate() {
        let image = image_of(item);
        let slot = preferred_mammo_slot(image, ordered.len(), |slot_index| {
            ordered.get(slot_index).and_then(Option::as_ref).is_none()
        });

        if let Some(slot) = slot {
            if ordered[slot].is_none() {
                ordered[slot] = Some(index);
            } else {
                fallback.push_back(index);
            }
        } else {
            fallback.push_back(index);
        }
    }

    for slot in ordered.iter_mut() {
        if slot.is_none() {
            *slot = fallback.pop_front();
        }
    }

    ordered.into_iter().flatten().collect()
}

pub fn preferred_slots_for_laterality(laterality: Option<&str>) -> [usize; 4] {
    match laterality {
        Some("R") => [0, 2, 1, 3],
        Some("L") => [1, 3, 0, 2],
        _ => [0, 1, 2, 3],
    }
}

pub fn mammo_image_align(index: usize) -> egui::Align {
    match index {
        // Quadrants 1 and 3 (left column): right-justify image in viewport.
        0 | 2 => egui::Align::Max,
        // Quadrants 2 and 4 (right column): left-justify image in viewport.
        1 | 3 => egui::Align::Min,
        _ => egui::Align::Center,
    }
}

pub fn mammo_sort_key(image: &DicomImage, path: &Path) -> (u8, u8, i32, String) {
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

pub fn mammo_label(image: &DicomImage, path: &Path) -> String {
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
